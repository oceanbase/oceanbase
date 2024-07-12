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

#include "storage/memtable/ob_memtable_context.h"
#include "lib/ob_errno.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_data.h"
#include "ob_memtable.h"
#include "storage/ob_sequence.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_multi_data_source.h"
#include "share/ob_tenant_mgr.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "share/ob_force_print_log.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_array_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_table_lock_callback.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction;
using namespace transaction::tablelock;

namespace memtable
{
ObMemtableCtx::ObMemtableCtx()
    : ObIMemtableCtx(),
      rwlock_(),
      lock_(),
      end_code_(OB_SUCCESS),
      tx_status_(ObTxStatus::NORMAL),
      ref_(0),
      query_allocator_(),
      ctx_cb_allocator_(),
      ctx_(NULL),
      truncate_cnt_(0),
      lock_for_read_retry_count_(0),
      lock_for_read_elapse_(0),
      trans_mem_total_size_(0),
      unsubmitted_cnt_(0),
      callback_mem_used_(0),
      callback_alloc_count_(0),
      callback_free_count_(0),
      is_read_only_(false),
      is_master_(true),
      has_row_updated_(false),
      mem_ctx_obj_pool_(ctx_cb_allocator_),
      lock_mem_ctx_(*this),
      trans_mgr_(*this, ctx_cb_allocator_, mem_ctx_obj_pool_),
      is_inited_(false)
{
}

ObMemtableCtx::~ObMemtableCtx()
{
  // do not invoke virtual function in deconstruct
  ObMemtableCtx::reset();
}

int ObMemtableCtx::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) { // use is_inited_ to prevent memtable ctx from being inited repeatedly
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(query_allocator_.init(tenant_id))) {
      TRANS_LOG(ERROR, "query_allocator init error", K(ret));
    } else if (OB_FAIL(ctx_cb_allocator_.init(tenant_id))) {
      TRANS_LOG(ERROR, "ctx_allocator_ init error", K(ret));
    } else if (OB_FAIL(reset_log_generator_())) {
      TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

// for mintest
int ObMemtableCtx::enable_lock_table(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_mem_ctx_.init(handle))) {
    TRANS_LOG(WARN, "lock mem ctx init failed", K(ret));
  }
  return ret;
}

int ObMemtableCtx::enable_lock_table(ObLSTxCtxMgr *ls_tx_ctx_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_mem_ctx_.init(ls_tx_ctx_mgr))) {
    TRANS_LOG(WARN, "lock mem ctx init failed", K(ret));
  }
  return ret;
}

void ObMemtableCtx::reset()
{
  if (IS_INIT) {
    if ((ATOMIC_LOAD(&callback_mem_used_) > 8 * 1024 * 1024) && REACH_TIME_INTERVAL(200000)) {
      TRANS_LOG(INFO, "memtable callback memory used > 8MB", K(callback_mem_used_), K(*this));
    }
    if (OB_UNLIKELY(callback_alloc_count_ != callback_free_count_)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "callback alloc and free count not match", K(*this));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
    }
    if (OB_UNLIKELY(unsubmitted_cnt_ != 0)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "txn unsubmitted cnt not zero", K(*this), K(unsubmitted_cnt_));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
    }
    if (OB_TRANS_KILLED != end_code_) {
      // _NOTE_: skip when txn was forcedly killed
      // if txn killed forcedly, callbacks of log unsynced will not been processed
      // after log sync succeed
      const int64_t fill = log_gen_.get_redo_filled_count();
      const int64_t sync_succ = log_gen_.get_redo_sync_succ_count();
      const int64_t sync_fail = log_gen_.get_redo_sync_fail_count();
      if (OB_UNLIKELY(fill != sync_succ + sync_fail)) {
        TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "redo filled_count != sync_succ + sync_fail", KPC(this),
                    K(fill), K(sync_succ), K(sync_fail));
#ifdef ENABLE_DEBUG_LOG
        ob_abort();
#endif
      }
    }
    is_inited_ = false;
    callback_free_count_ = 0;
    callback_alloc_count_ = 0;
    callback_mem_used_ = 0;
    has_row_updated_ = false;
    trans_mem_total_size_ = 0;
    lock_for_read_retry_count_ = 0;
    lock_for_read_elapse_ = 0;
    truncate_cnt_ = 0;
    unsubmitted_cnt_ = 0;
    mem_ctx_obj_pool_.reset();
    lock_mem_ctx_.reset();
    retry_info_.reset();
    trans_mgr_.reset();
    log_gen_.reset();
    query_allocator_.reset(/*only_check*/ true);
    ctx_cb_allocator_.reset(/*only_check*/ true);
    ref_ = 0;
    is_master_ = true;
    is_read_only_ = false;
    end_code_ = OB_SUCCESS;
    tx_status_ = ObTxStatus::NORMAL;
    // blocked_trans_ids_.reset();
    //FIXME: ObIMemtableCtx don't have resetfunction,
    //thus ObIMvccCtx::reset is called, so resource_link_is not reset
    ObIMemtableCtx::reset();
  }
}

int64_t ObMemtableCtx::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "{");
  pos += ObIMvccCtx::to_string(buf + pos, buf_len);
  common::databuff_printf(buf, buf_len, pos,
                          " end_code=%d tx_status=%ld is_readonly=%s "
                          "ref=%ld trans_id=%s ls_id=%ld "
                          "row_callback[alloc:%ld, free:%ld, unsubmit:%ld] "
                          "redo[fill:%ld,sync_succ:%ld, sync_fail:%ld] "
                          "main_list_len=%ld pending_log_size=%ld ",
                          end_code_, tx_status_, STR_BOOL(is_read_only_), ref_,
                          NULL == ctx_ ? "" : S(ctx_->get_trans_id()),
                          NULL == ctx_ ? -1 : ctx_->get_ls_id().id(),
                          callback_alloc_count_, callback_free_count_, unsubmitted_cnt_,
                          log_gen_.get_redo_filled_count(),
                          log_gen_.get_redo_sync_succ_count(),
                          log_gen_.get_redo_sync_fail_count(),
                          trans_mgr_.get_main_list_length(),
                          trans_mgr_.get_pending_log_size());
  trans_mgr_.print_statistics(buf, buf_len, pos);
  common::databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

void ObMemtableCtx::set_read_only()
{
  ATOMIC_STORE(&is_read_only_, true);
}

void ObMemtableCtx::set_trans_ctx(ObPartTransCtx *ctx)
{
  ATOMIC_STORE(&ctx_, ctx);
}

void ObMemtableCtx::inc_ref()
{
  (void)ATOMIC_AAF(&ref_, 1);
}

void ObMemtableCtx::dec_ref()
{
  (void)ATOMIC_AAF(&ref_, -1);
}

void ObMemtableCtx::wait_pending_write()
{
  ATOMIC_STORE(&is_master_, false);
  WRLockGuard wrguard(rwlock_);
}

void ObMemtableCtx::wait_write_end()
{
  WRLockGuard wrguard(rwlock_);
}

SCN ObMemtableCtx::get_tx_end_scn() const
{
  return ctx_->get_tx_end_log_ts();
}

int ObMemtableCtx::write_auth(const bool exclusive)
{
  int ret = OB_SUCCESS;
  bool lock_succ = exclusive ?
     rwlock_.try_wrlock() :
    rwlock_.try_rdlock();

  // start to sanity check, always do this even try to acquire lock failed
  do {
    if (ATOMIC_LOAD(&is_read_only_)) {
      ret = OB_ERR_READ_ONLY_TRANSACTION;
      TRANS_LOG(ERROR, "WriteAuth: readonly trans not support update operation",
                "trans_id", ctx_->get_trans_id(), "ls_id", ctx_->get_ls_id(), K(ret));
    } else if (!ATOMIC_LOAD(&is_master_)) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "WriteAuth: trans is already not master",
                "trans_id", ctx_->get_trans_id(), "ls_id", ctx_->get_ls_id(), K(ret));
    } else if (OB_SUCCESS != ATOMIC_LOAD(&end_code_)) {
      ret = ATOMIC_LOAD(&end_code_);
      TRANS_LOG(WARN, "WriteAuth: trans is already end", K(ret),
                "trans_id", ctx_->get_trans_id(), "ls_id", ctx_->get_ls_id(), K_(end_code));
    } else if (is_tx_rollbacked()) {
      // The txn has been killed during normal processing. So we return
      // OB_TRANS_KILLED to prompt this abnormal state.
      ret = OB_TRANS_KILLED;
      TRANS_LOG(WARN, "WriteAuth: trans is already end", K(ret),
                "trans_id", ctx_->get_trans_id(), "ls_id", ctx_->get_ls_id(), K_(end_code));
    } else if (lock_succ) {
      // all check passed after lock succ
      break;
    } else {
      // check passed, block on wait lock
      ret = exclusive ? rwlock_.wrlock() : rwlock_.rdlock();
      if (OB_SUCC(ret)) {
        lock_succ = true;
      }
      // lock hold, do checks again after lock succ
    }
  } while(OB_SUCCESS == ret);

  if (OB_FAIL(ret)) {
    if (lock_succ) {
      rwlock_.unlock();
    }
  }
  TRANS_LOG(DEBUG, "mem_ctx.write_auth", K(ret), KPC(this));
  return ret;
}

int ObMemtableCtx::write_done()
{
  return rwlock_.unlock();
}

int ObMemtableCtx::write_lock_yield()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_done())) {
    TRANS_LOG(WARN, "write_done fail", K(ret));
  } else if (OB_FAIL(write_auth(false))) {
    TRANS_LOG(WARN, "write_auth fail", K(ret));
  }
  return ret;
}

void ObMemtableCtx::on_wlock_retry(const ObMemtableKey& key, const transaction::ObTransID &conflict_tx_id)
{
  #define USING_LOG_PREFIX TRANS
  if (retry_info_.need_print()) {
    FLOG_INFO("mvcc_write conflict", K(key), "tx_id", get_tx_id(), K(conflict_tx_id), KPC(this));
  }
  #undef USING_LOG_PREFIX
  retry_info_.on_conflict();
}

void ObMemtableCtx::on_key_duplication_retry(const ObMemtableKey& key)
{
  if (retry_info_.need_print()) {
    TRANS_LOG_RET(WARN, OB_SUCCESS, "primary key duplication conflict", K(key), KPC(this));
  }
}

void ObMemtableCtx::on_tsc_retry(const ObMemtableKey& key,
                                 const SCN snapshot_version,
                                 const SCN max_trans_version,
                                 const transaction::ObTransID &conflict_tx_id)
{
  if (retry_info_.need_print()) {
    TRANS_LOG_RET(WARN, OB_SUCCESS, "transaction_set_consistency conflict", K(key), K(snapshot_version), K(max_trans_version), K(conflict_tx_id), KPC(this));
  }
  retry_info_.on_conflict();
}

void *ObMemtableCtx::old_row_alloc(const int64_t size)
{
  void* ret = NULL;
  if (OB_ISNULL(ret = ctx_cb_allocator_.alloc(size))) {
    TRANS_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "old row alloc error, no memory", K(size), K(*this));
  } else {
    ATOMIC_FAA(&callback_mem_used_, size);
    TRANS_LOG(DEBUG, "old row alloc succ", K(*this), KP(ret), K(lbt()));
  }
  return ret;
}

void ObMemtableCtx::old_row_free(void *row)
{
  if (OB_ISNULL(row)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "row is null, unexpected error", KP(row), K(*this));
  } else {
    TRANS_LOG(DEBUG, "row release succ", KP(row), K(*this), K(lbt()));
    ctx_cb_allocator_.free(row);
    row = NULL;
  }
}

void *ObMemtableCtx::alloc_mvcc_row_callback()
{
  void* ret = NULL;
  if (OB_ISNULL(ret = trans_mgr_.alloc_mvcc_row_callback())) {
    TRANS_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "callback alloc error, no memory", K(*this));
  } else {
    ATOMIC_FAA(&callback_mem_used_, sizeof(ObMvccRowCallback));
    ATOMIC_INC(&callback_alloc_count_);
    TRANS_LOG(DEBUG, "callback alloc succ", K(*this), KP(ret), K(lbt()));
  }
  return ret;
}

void ObMemtableCtx::free_mvcc_row_callback(ObITransCallback *cb)
{
  if (OB_ISNULL(cb)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cb is null, unexpected error", KP(cb), K(*this));
  } else if (cb->is_table_lock_callback()) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "try to free table lock callback", KPC(cb));
  } else if (MutatorType::MUTATOR_ROW_EXT_INFO == cb->get_mutator_type()) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "try to free ext info callback as mvcc row callback", KP(cb), K(*this));
  } else {
    ATOMIC_INC(&callback_free_count_);
    TRANS_LOG(DEBUG, "callback release succ", KP(cb), K(*this), K(lbt()));
    trans_mgr_.free_mvcc_row_callback(cb);
    cb = NULL;
  }
}

storage::ObExtInfoCallback *ObMemtableCtx::alloc_ext_info_callback()
{
  int ret = OB_SUCCESS;
  void *cb_buffer = nullptr;
  storage::ObExtInfoCallback *cb = nullptr;
  if (nullptr == (cb_buffer = mem_ctx_obj_pool_.alloc<storage::ObExtInfoCallback>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ObExtInfoCallback fail", K(ret));
  } else if (nullptr == (cb = new(cb_buffer) storage::ObExtInfoCallback())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "construct ObExtInfoCallback object fail", K(ret), "cb_buffer", cb_buffer);
  } else {
    trans_mgr_.add_callback_ext_info_log_count(1);
  }
  return cb;
}

void ObMemtableCtx::free_ext_info_callback(ObITransCallback *cb)
{
  if (OB_ISNULL(cb)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cb is null, unexpected error", KP(cb), K(*this));
  } else if (MutatorType::MUTATOR_ROW_EXT_INFO != cb->get_mutator_type()) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cb is not ext info callback", "type", cb->get_mutator_type(), K(*this));
  } else {
    ObExtInfoCallback *ext_cb = static_cast<ObExtInfoCallback *>(cb);
    ext_cb->~ObExtInfoCallback();
    mem_ctx_obj_pool_.free<storage::ObExtInfoCallback>(cb);
    TRANS_LOG(DEBUG, "callback release succ", KP(cb), K(*this), K(lbt()));
    trans_mgr_.add_callback_ext_info_log_count(-1);
    cb = NULL;
  }
}

ObIAllocator &ObMemtableCtx::get_query_allocator()
{
  return query_allocator_;
}

int ObMemtableCtx::trans_begin()
{
  int ret = OB_SUCCESS;

  trans_mgr_.trans_start();
  return ret;
}

int ObMemtableCtx::replay_begin(const bool parallel_replay, const SCN scn)
{
  // UNUSED(scn);
  trans_mgr_.replay_begin(parallel_replay, scn);
  return OB_SUCCESS;
}

// callback_list_idx:
// -1 means all callback-list should do commit/rollback
int ObMemtableCtx::replay_end(const bool is_replay_succ,
                              const int16_t callback_list_idx,
                              const SCN scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (!is_replay_succ) {
    ret = trans_mgr_.replay_fail(callback_list_idx, scn);
  } else {
    ret = trans_mgr_.replay_succ(callback_list_idx, scn);
  }

  return ret;
}

int ObMemtableCtx::rollback_redo_callbacks(const int16_t callback_list_idx, const SCN scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  ret = trans_mgr_.replay_fail(callback_list_idx, scn);

  return ret;
}

int ObMemtableCtx::trans_end(
    const bool commit,
    const SCN trans_version,
    const SCN final_scn)
{
  int ret = OB_SUCCESS;

  if (commit && get_trans_version().is_max()) {
    TRANS_LOG(ERROR, "unexpected prepare version", K(*this));
    // no retcode
  }

  ret = do_trans_end(commit,
                     trans_version,
                     final_scn,
                     commit ? OB_TRANS_COMMITED : OB_TRANS_ROLLBACKED);

  return ret;
}

int ObMemtableCtx::trans_clear()
{
  return OB_SUCCESS;
}

int ObMemtableCtx::elr_trans_preparing()
{
  RDLockGuard guard(rwlock_);
  if (NULL != ATOMIC_LOAD(&ctx_) && OB_SUCCESS == end_code_) {
    set_commit_version(static_cast<ObPartTransCtx *>(ctx_)->get_commit_version());
    trans_mgr_.elr_trans_preparing();
  }
  return OB_SUCCESS;
}

int ObMemtableCtx::do_trans_end(
    const bool commit,
    const SCN trans_version,
    const SCN final_scn,
    const int end_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  WRLockGuard wrguard(rwlock_);
  if (OB_SUCCESS == ATOMIC_LOAD(&end_code_)) {
    ATOMIC_STORE(&end_code_, end_code);
    set_commit_version(trans_version);
    if (OB_FAIL(trans_mgr_.trans_end(commit))) {
      TRANS_LOG(WARN, "trans end error", K(ret), K(*this));
    }
    // after a transaction finishes, callback memory should be released
    // and check memory leakage
    if (OB_UNLIKELY(ATOMIC_LOAD(&callback_alloc_count_) != ATOMIC_LOAD(&callback_free_count_))) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "callback alloc and free count not match", KPC(this));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
    }
    // release durable table lock
    if (OB_FAIL(ret)) {
      UNUSED(final_scn);
      //commit or abort log ts for clear table lock
    } else if (OB_FAIL(clear_table_lock_(commit, trans_version, final_scn))) {
      TRANS_LOG(ERROR, "clear table lock failed.", K(ret), K(*this));
    }
  }
  return ret;
}

int ObMemtableCtx::trans_kill()
{
  int ret = OB_SUCCESS;
  bool commit = false;
  ret = do_trans_end(commit, SCN::max_scn(), SCN::max_scn(), OB_TRANS_KILLED);
  return ret;
}

bool ObMemtableCtx::is_slow_query() const
{
  return false;
  //return get_us() - get_trans_start_time() >= SLOW_QUERY_THRESHOULD;
}

int ObMemtableCtx::trans_publish()
{
  return OB_SUCCESS;
}

int ObMemtableCtx::trans_replay_begin()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_master_, false);
  return ret;
}

int ObMemtableCtx::trans_replay_end(const bool commit,
                                    const SCN trans_version,
                                    const SCN final_scn,
                                    const uint64_t log_cluster_version,
                                    const uint64_t checksum)
{
  int ret = OB_SUCCESS;
  int cs_ret = OB_SUCCESS;

  // We must calculate the checksum and generate the checksum_scn even when
  // the checksum verification is unnecessary. This because the trans table
  // merge may be triggered after clear state in which the callback has already
  if (commit
      && 0 != checksum // if leader's checksum is skipped, follow skip check
      && log_cluster_version >= CLUSTER_VERSION_3100
      && !ObServerConfig::get_instance().ignore_replay_checksum_error) {
    ObSEArray<uint64_t, 1> replay_checksum;
    if (OB_FAIL(calc_checksum_all(replay_checksum))) {
      TRANS_LOG(WARN, "calc checksum fail", K(ret));
    } else {
      uint64_t checksum_collapsed = 0;
      uint8_t _sig[replay_checksum.count()];
      ObArrayHelper<uint8_t> checksum_signature(replay_checksum.count(), _sig);
      convert_checksum_for_commit_log(replay_checksum, checksum_collapsed, checksum_signature);
      if (checksum != checksum_collapsed) {
        cs_ret = OB_CHECKSUM_ERROR;
        TRANS_LOG(ERROR, "MT_CTX: replay checksum error", K(cs_ret),
                  "checksum_in_commit_log", checksum,
                  "checksum_replayed", checksum_collapsed,
                  "checksum_before_collapse", replay_checksum,
                  K(checksum_signature), KPC(this));
#ifdef ENABLE_DEBUG_LOG
        ob_usleep(5000);
        ob_abort();
#endif
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_trans_end(commit,
                           trans_version,
                           final_scn,
                           commit ? OB_TRANS_COMMITED : OB_TRANS_ROLLBACKED))) {
    TRANS_LOG(ERROR, "trans_end fail", K(ret), K(*this));
  } else {
    ret = cs_ret;
  }
  return ret;
}

//leader takeover actions
int ObMemtableCtx::replay_to_commit(const bool is_resume)
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_master_, true);
  ObByteLockGuard guard(lock_);
  trans_mgr_.set_for_replay(false);
  if (!is_resume) {
    trans_mgr_.clear_pending_log_size();
  }
  if (OB_FAIL(reuse_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "replay to commit failed", K(ret), K(this));
  }
  return ret;
}

//leader revoke action
int ObMemtableCtx::commit_to_replay()
{
  ATOMIC_STORE(&is_master_, false);
  WRLockGuard wrguard(rwlock_);
  trans_mgr_.set_for_replay(true);
  return OB_SUCCESS;
}

int ObMemtableCtx::fill_redo_log(ObTxFillRedoCtx &ctx)
{
  int ret = OB_SUCCESS;
  ret = log_gen_.fill_redo_log(ctx);
  return ret;
}

int ObMemtableCtx::get_log_guard(const transaction::ObTxSEQ &write_seq,
                                 ObCallbackListLogGuard &log_guard,
                                 int& cb_list_idx)
{
  return trans_mgr_.get_log_guard(write_seq, log_guard, cb_list_idx);
}

int ObMemtableCtx::log_submitted(const ObRedoLogSubmitHelper &helper)
{
  inc_pending_log_size(-1 * helper.data_size_);
  inc_flushed_log_size(helper.data_size_);
  return log_gen_.log_submitted(helper.callbacks_, helper.log_scn_);
}

int ObMemtableCtx::sync_log_succ(const SCN scn, const ObCallbackScopeArray &callbacks)
{
  lock_mem_ctx_.sync_log_succ(scn);
  log_gen_.sync_log_succ(callbacks, scn);
  return OB_SUCCESS;
}

void ObMemtableCtx::sync_log_fail(const ObCallbackScopeArray &callbacks,
                                  const share::SCN &max_applied_scn)
{
  int ret = OB_SUCCESS;
  if (callbacks.count() > 0) {
    set_partial_rollbacked();
  }
  if (OB_SUCCESS == ATOMIC_LOAD(&end_code_)) {
    if (OB_FAIL(reuse_log_generator_())) {
      TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
    } else {
      log_gen_.sync_log_fail(callbacks, max_applied_scn);
    }
  } else {
    if (callbacks.count() > 0) {
      TRANS_LOG(INFO, "skip do callbacks because of trans_end", K(end_code_), KPC(ctx_));
      int fail_cnt = 0;
      ARRAY_FOREACH(callbacks, i) {
        fail_cnt += callbacks.at(i).cnt_;
      }
      log_gen_.inc_sync_log_fail_cnt(fail_cnt);
    }
  }
  return;
}

int ObMemtableCtx::calc_checksum_all(ObIArray<uint64_t> &checksum)
{
  ObByteLockGuard guard(lock_);
  return trans_mgr_.calc_checksum_all(checksum);
}

void ObMemtableCtx::print_callbacks()
{
  trans_mgr_.print_callbacks();
}
int ObMemtableCtx::get_callback_list_stat(ObIArray<ObTxCallbackListStat> &stats)
{
  return trans_mgr_.get_callback_list_stat(stats);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int ObMemtableCtx::get_conflict_trans_ids(common::ObIArray<ObTransIDAndAddr> &array)
{
  int ret = OB_SUCCESS;
  common::ObArray<transaction::ObTransID> conflict_ids;
  {
    ObByteLockGuard guard(lock_);
    ret = conflict_ids.assign(conflict_trans_ids_);
  }
  if (OB_FAIL(ret)) {
    DETECT_LOG(ERROR, "fail to copy conflict_trans_ids_", KR(ret), KPC(this));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_BAD_NULL_ERROR;
    DETECT_LOG(ERROR, "trans part ctx on ObMemtableCtx is NULL", KR(ret), KPC(this));
  } else {
    for (int64_t idx = 0; idx < conflict_ids.count() && OB_SUCC(ret); ++idx) {
      ObAddr scheduler_addr;
      if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_trans_scheduler_info_on_participant(conflict_ids.at(idx),
                                                                                          ctx_->get_ls_id(),
                                                                                          scheduler_addr))) {
        if (ret == OB_TRANS_CTX_NOT_EXIST) {
          DETECT_LOG(INFO, "tx ctx not exist anymore, give up blocking on this tx_id",
                           KR(ret), KPC(this), K(conflict_ids), K(array), K(idx));
          ret = OB_SUCCESS;
        } else {
          DETECT_LOG(WARN, "fail to get conflict trans scheduler addr",
                           KR(ret), KPC(this), K(conflict_ids), K(array), K(idx));
        }
      } else if (OB_FAIL(array.push_back({conflict_ids[idx], scheduler_addr}))) {
        DETECT_LOG(ERROR, "copy block trans id failed",
                          KR(ret), KPC(this), K(conflict_ids), K(array), K(idx));
      }
    }
  }
  return ret;
}

void ObMemtableCtx::reset_conflict_trans_ids()
{
  ObByteLockGuard guard(lock_);
  conflict_trans_ids_.reset();
}

int ObMemtableCtx::add_conflict_trans_id(const ObTransID conflict_trans_id)
{
  auto if_contains = [this](const ObTransID trans_id) -> bool
  {
    for (int64_t idx = 0; idx < this->conflict_trans_ids_.count(); ++idx) {
      if (this->conflict_trans_ids_[idx] == trans_id) {
        return true;
      }
    }
    return false;
  };
  ObByteLockGuard guard(lock_);
  int ret = OB_SUCCESS;

  if (conflict_trans_ids_.count() >= MAX_RESERVED_CONFLICT_TX_NUM) {
    ret = OB_SIZE_OVERFLOW;
    DETECT_LOG(WARN, "too many conflict trans_id", K(*this), K(conflict_trans_id), K(conflict_trans_ids_));
  } else if (if_contains(conflict_trans_id)) {
    // do nothing
  } else if (OB_FAIL(conflict_trans_ids_.push_back(conflict_trans_id))) {
    DETECT_LOG(WARN, "push trans id to blocked trans ids failed", K(*this), K(conflict_trans_id), K(conflict_trans_ids_));
  }

  return ret;
}

void ObMemtableCtx::inc_lock_for_read_retry_count()
{
  lock_for_read_retry_count_++;
}

void ObMemtableCtx::add_trans_mem_total_size(const int64_t size)
{
  if (size < 0) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected size", K(size), K(*this));
  } else {
    ATOMIC_FAA(&trans_mem_total_size_, size);
  }
}

int ObMemtableCtx::get_memtable_key_arr(transaction::ObMemtableKeyArray &memtable_key_arr)
{
  int ret = OB_SUCCESS;
  // the memtable pointer read from
  ObMemtable *cur_memtable = NULL;
  // read row lock data
  if (OB_FAIL(trans_mgr_.get_memtable_key_arr(memtable_key_arr))) {
    TRANS_LOG(WARN, "get_memtable_key_arr fail", K(ret), K(memtable_key_arr));
  }

  // locking on a non-active memstore would generate OB_STATE_NOT_MATCH
  // OB_EAGAIN would be returned when no data is written, or after trans clear
  if (OB_STATE_NOT_MATCH == ret || OB_EAGAIN == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

uint64_t ObMemtableCtx::get_tenant_id() const
{
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  if (NULL != ATOMIC_LOAD(&ctx_)) {
    tenant_id = ctx_->get_tenant_id();
  }
  return tenant_id;
}

int ObMemtableCtx::rollback(const transaction::ObTxSEQ to_seq_no,
                            const transaction::ObTxSEQ from_seq_no,
                            const share::SCN replay_scn)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = common::ObClockGenerator::getClock();
  ObByteLockGuard guard(lock_);
  int64_t remove_cnt = 0;
  if (!to_seq_no.is_valid() || !from_seq_no.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(from_seq_no), K(to_seq_no));
  } else if (OB_ISNULL(ATOMIC_LOAD(&ctx_))) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "ctx is NULL", K(ret));
  } else if (OB_FAIL(reuse_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else if (OB_FAIL(trans_mgr_.rollback_to(to_seq_no, from_seq_no, replay_scn, remove_cnt))) {
    TRANS_LOG(WARN, "rollback to failed", K(ret), K(*this));
  // rollback the table lock that with no tablelock callback
  } else if (OB_FAIL(rollback_table_lock_(to_seq_no, from_seq_no))) {
    TRANS_LOG(WARN, "rollback table lock failed", K(ret), K(*this), K(to_seq_no));
  } else {
    const int64_t elapsed = common::ObClockGenerator::getClock() - start_ts;
    TRANS_LOG(INFO, "memtable handle rollback to successfuly", K(from_seq_no), K(to_seq_no), K(remove_cnt), K(elapsed), KPC(this));
  }
  return ret;
}

int ObMemtableCtx::remove_callbacks_for_fast_commit(const int16_t callback_list_idx, const share::SCN stop_scn)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("remove callbacks for fast commit", 10 * 1000);
  ObByteLockGuard guard(lock_);
  if (OB_FAIL(trans_mgr_.remove_callbacks_for_fast_commit(callback_list_idx, stop_scn))) {
    TRANS_LOG(WARN, "fail to remove callback for fast commit", K(ret), KPC(this));
  }
  return ret;
}
int ObMemtableCtx::remove_callbacks_for_fast_commit(const ObCallbackScopeArray &cb_scope_array)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("remove callbacks for fast commit", 10 * 1000);
  if (OB_FAIL(trans_mgr_.remove_callbacks_for_fast_commit(cb_scope_array))) {
    TRANS_LOG(WARN, "fail to remove callback for fast commit", K(ret));
  }
  return ret;
}

int ObMemtableCtx::remove_callback_for_uncommited_txn(const memtable::ObMemtableSet *memtable_set)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("remove callbacks for uncommitted txn", 10 * 1000);
  ObByteLockGuard guard(lock_);

  if (OB_ISNULL(memtable_set)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is NULL", K(memtable_set));
  } else if (OB_FAIL(reuse_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else if (OB_FAIL(trans_mgr_.remove_callback_for_uncommited_txn(memtable_set))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), K(memtable_set));
  }

  return ret;
}

int ObMemtableCtx::clean_unlog_callbacks()
{
  int ret = OB_SUCCESS;
  {
    int64_t removed_cnt = 0;
    struct BeforeRemoveCallback {
      memtable::ObMemtableCtx *mt_;
      BeforeRemoveCallback(memtable::ObMemtableCtx *mt) : mt_(mt) {}
      void operator()() {
        mt_->set_partial_rollbacked();
      }
    };
    ObFunction<void()> before_remove(BeforeRemoveCallback(this));
    ObByteLockGuard guard(lock_);
    if (OB_FAIL(trans_mgr_.clean_unlog_callbacks(removed_cnt, before_remove))) {
      TRANS_LOG(WARN, "clean unlog callbacks failed", KR(ret));
    } else {
      trans_mgr_.clear_pending_log_size();
    }
  }
  return ret;
}

int ObMemtableCtx::reset_log_generator_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(log_gen_.set(&trans_mgr_, this))) {
    TRANS_LOG(ERROR, "reset log_gen fail", K(ret), K(this));
  }

  return ret;
}

int ObMemtableCtx::reuse_log_generator_()
{
  int ret = OB_SUCCESS;

  log_gen_.reuse();

  return ret;
}

int ObMemtableCtx::calc_checksum_before_scn(const SCN scn,
                                            ObIArray<uint64_t> &checksum,
                                            ObIArray<SCN> &checksum_scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (OB_FAIL(trans_mgr_.calc_checksum_before_scn(scn, checksum, checksum_scn))) {
    TRANS_LOG(ERROR, "calc checksum before log ts should not report error", K(ret), K(scn));
  }

  return ret;
}

int ObMemtableCtx::update_checksum(const ObIArray<uint64_t> &checksum,
                                   const ObIArray<SCN> &checksum_scn)
{
  ObByteLockGuard guard(lock_);

  return trans_mgr_.update_checksum(checksum, checksum_scn);
}

bool ObMemtableCtx::pending_log_size_too_large(const ObTxSEQ &write_seq_no)
{
  bool ret = true;

  if (0 == GCONF._private_buffer_size) {
    ret = false;
  } else {
    ret = trans_mgr_.pending_log_size_too_large(write_seq_no, GCONF._private_buffer_size);
  }

  return ret;
}

int ObMemtableCtx::get_table_lock_store_info(ObTableLockInfo &table_lock_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_mem_ctx_.get_table_lock_store_info(table_lock_info))) {
    TRANS_LOG(WARN, "get_table_lock_store_info failed", K(ret));
  }
  return ret;
}

int ObMemtableCtx::get_table_lock_for_transfer(ObTableLockInfo &table_lock_info, const ObIArray<ObTabletID> &tablet_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_mem_ctx_.get_table_lock_for_transfer(table_lock_info, tablet_list))) {
    TRANS_LOG(WARN, "get tablet lock for transfer failed", K(ret));
  }
  return ret;
}

int ObMemtableCtx::recover_from_table_lock_durable_info(const ObTableLockInfo &table_lock_info,
                                                        const bool transfer_merge)
{
  int ret = OB_SUCCESS;
  const int64_t op_cnt = table_lock_info.table_lock_ops_.count();
  ObLockMemtable* lock_memtable = nullptr;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  const int64_t curr_timestamp = ObTimeUtility::current_time();
  for (int64_t i = 0; i < op_cnt && OB_SUCC(ret); ++i) {
    tablelock::ObTableLockOp lock_op = table_lock_info.table_lock_ops_.at(i);
    if (!lock_op.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "the lock_op is not valid", K(lock_op));
      // NOTE: we only need recover the lock op at lock list and lock map.
      // the buffer at multi source data is recovered by multi source data.
      // the tx ctx table may be copied from other ls replica we need fix the lockop's create timestamp.
    } else if (FALSE_IT(lock_op.create_timestamp_ = OB_MIN(curr_timestamp,
                                                           lock_op.create_timestamp_))) {
    } else if (OB_FAIL(lock_mem_ctx_.add_lock_record(lock_op, lock_op_node))) {
      TRANS_LOG(ERROR, "add_lock_record failed", K(ret), K(lock_op));
    } else if (OB_FAIL(lock_mem_ctx_.get_lock_memtable(lock_memtable))) {
      TRANS_LOG(ERROR, "get_lock_memtable failed", K(ret));
    } else if (OB_NOT_NULL(lock_memtable)
              && OB_FAIL(lock_memtable->recover_obj_lock(lock_op))) {
      TRANS_LOG(ERROR, "recover_obj_lock failed", K(ret), K(*lock_memtable));
    } else if (!transfer_merge) {
      lock_mem_ctx_.sync_log_succ(table_lock_info.max_durable_scn_);
    }
  }

  return ret;
}

int ObMemtableCtx::check_lock_need_replay(const SCN &scn,
                                          const tablelock::ObTableLockOp &lock_op,
                                          bool &need_replay)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_mem_ctx_.check_lock_need_replay(scn,
                                                   lock_op,
                                                   need_replay))) {
    TRANS_LOG(WARN, "check lock need replay failed. ", K(ret), K(lock_op));
  }
  return ret;
}

int ObMemtableCtx::check_lock_exist(const ObLockID &lock_id,
                                    const ObTableLockOwnerID &owner_id,
                                    const ObTableLockMode mode,
                                    const ObTableLockOpType op_type,
                                    bool &is_exist,
                                    uint64_t lock_mode_cnt_in_same_trans[]) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_FAIL(lock_mem_ctx_.check_lock_exist(lock_id,
                                             owner_id,
                                             mode,
                                             op_type,
                                             is_exist,
                                             lock_mode_cnt_in_same_trans))) {
    TRANS_LOG(WARN, "check lock exist failed. ", K(ret), K(lock_id),
              K(owner_id), K(mode), K(*this));
  }
  return ret;
}

int ObMemtableCtx::check_modify_schema_elapsed(
    const ObTabletID &tablet_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObLockID lock_id;

  if (OB_FAIL(get_lock_id(tablet_id, lock_id))) {
    TRANS_LOG(WARN, "get lock id failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(lock_mem_ctx_.check_modify_schema_elapsed(lock_id,
                                                               schema_version))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "check schema version elapsed failed", K(ret), K(lock_id),
                K(schema_version), K(*this));
    }
  }
  return ret;
}

int ObMemtableCtx::check_modify_time_elapsed(
    const ObTabletID &tablet_id,
    const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  ObLockID lock_id;

  if (OB_FAIL(get_lock_id(tablet_id, lock_id))) {
    TRANS_LOG(WARN, "get lock id failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(lock_mem_ctx_.check_modify_time_elapsed(lock_id,
                                                             timestamp))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "check timestamp elapsed failed", K(ret), K(lock_id),
                K(timestamp), K(*this));
    }
  }
  return ret;
}

int ObMemtableCtx::iterate_tx_obj_lock_op(ObLockOpIterator &iter) const
{
  return lock_mem_ctx_.iterate_tx_obj_lock_op(iter);
}

int ObMemtableCtx::add_lock_record(const tablelock::ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  ObLockMemtable *memtable = nullptr;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(lock_op));
  } else if (OB_FAIL(lock_mem_ctx_.add_lock_record(lock_op, lock_op_node))) {
    TRANS_LOG(WARN, "create lock record at memtable failed. ", K(ret), K(lock_op), K(*this));
  } else if (OB_FAIL(register_multi_source_data_if_need_(lock_op))) {
    TRANS_LOG(WARN, "register to multi source data failed", K(ret), K(lock_op));
  } else if (OB_UNLIKELY(!lock_op.need_register_callback())) {
    // do nothing
    TABLELOCK_LOG(DEBUG, "no need callback ObMemtableCtx::add_lock_record", K(lock_op));
  } else if (OB_FAIL(lock_mem_ctx_.get_lock_memtable(memtable))) {
    TRANS_LOG(WARN, "get lock memtable failed.", K(ret));
  } else if (OB_FAIL(register_table_lock_cb(memtable,
                                            lock_op_node))) {
    TRANS_LOG(WARN, "register table lock callback failed.", K(ret), K(lock_op));
  }
  if (OB_FAIL(ret) && lock_op_node != NULL) {
    lock_mem_ctx_.remove_lock_record(lock_op_node);
  }
  return ret;
}

int ObMemtableCtx::replay_add_lock_record(
    const tablelock::ObTableLockOp &lock_op,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  ObLockMemtable *memtable = nullptr;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(lock_op));
  } else if (OB_FAIL(lock_mem_ctx_.add_lock_record(lock_op, lock_op_node))) {
    TRANS_LOG(WARN, "create lock record at memtable failed. ", K(ret), K(lock_op),
              K(*this));
  } else if (OB_UNLIKELY(!lock_op.need_register_callback())) {
    // do nothing
    TABLELOCK_LOG(DEBUG, "no need callback ObMemtableCtx::add_lock_record", K(lock_op));
  } else if (OB_FAIL(lock_mem_ctx_.get_lock_memtable(memtable))) {
    TRANS_LOG(WARN, "get lock memtable failed.", K(ret));
  } else if (OB_FAIL(register_table_lock_replay_cb(memtable,
                                                   lock_op_node,
                                                   scn))) {
    TRANS_LOG(WARN, "register table lock callback failed.", K(ret), K(lock_op));
  } else {
    // make sure the replayed tablelock will be minor merged.
    // and update the max durable log ts.
    lock_mem_ctx_.sync_log_succ(scn);
  }
  if (OB_FAIL(ret) && lock_op_node != NULL) {
    lock_mem_ctx_.remove_lock_record(lock_op_node);
  }
  return ret;
}

void ObMemtableCtx::remove_lock_record(ObMemCtxLockOpLinkNode *lock_op)
{
  lock_mem_ctx_.remove_lock_record(lock_op);
}

int ObMemtableCtx::clear_table_lock_(const bool is_commit,
                                     const SCN &commit_version,
                                     const SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObLockMemtable *memtable = nullptr;
  if (is_read_only_) {
    // read only trx no need deal with table lock.
  } else if (OB_FAIL(lock_mem_ctx_.clear_table_lock(is_commit,
                                                    commit_version,
                                                    commit_scn))) {
    TRANS_LOG(WARN, "clear table lock failed", KP(this));
  }
  return ret;
}

int ObMemtableCtx::rollback_table_lock_(transaction::ObTxSEQ to_seq_no,
                                        transaction::ObTxSEQ from_seq_no)
{
  int ret = OB_SUCCESS;
  if (is_read_only_) {
    // read only trx no need deal with table lock.
  } else if (OB_FAIL(lock_mem_ctx_.rollback_table_lock(to_seq_no, from_seq_no))) {
    TRANS_LOG(WARN, "clear table lock failed", KP(this));
  }

  return ret;
}

int ObMemtableCtx::register_multi_source_data_if_need_(
    const tablelock::ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  ObRegisterMdsFlag mds_flag;
  mds_flag.reset();
  if (!lock_op.need_multi_source_data()) {
    // do nothing
  } else if (OB_ISNULL(ATOMIC_LOAD(&ctx_))) {
    // if a trans need lock table, it must not a readonly trans.
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ctx should not null", K(ret));
  } else {
    char buf[tablelock::ObTableLockOp::MAX_SERIALIZE_SIZE];
    int64_t serialize_size = lock_op.get_serialize_size();
    int64_t pos = 0;
    ObTxDataSourceType type = ObTxDataSourceType::TABLE_LOCK;
    ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx *>(ctx_);

    if (serialize_size > tablelock::ObTableLockOp::MAX_SERIALIZE_SIZE) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "lock op serialize size if over flow", K(ret), K(serialize_size));
    } else if (OB_FAIL(lock_op.serialize(buf, serialize_size, pos))) {
      TRANS_LOG(WARN, "serialize lock op failed", K(ret), K(serialize_size), K(pos));
      // TODO: yanyuan.cxf need seqno to do rollback.
      // THE MULTI SOURCE BUFFER CAN REGISTER AGAIN IF THE RET CODE IS NOT OB_SUCCESS
    } else if (OB_FAIL(part_ctx->register_multi_data_source(type,
                                                            buf,
                                                            serialize_size,
                                                            true /* try lock */,
                                                            lock_op.lock_seq_no_,
                                                            mds_flag))) {
      TRANS_LOG(WARN, "register to multi source data failed", K(ret));
    } else {
      // do nothing
    }
    TABLELOCK_LOG(DEBUG, "register table lock to multi source data", K(ret), K(lock_op));
  }
  return ret;
}

int ObMemtableCtx::replay_lock(const tablelock::ObTableLockOp &lock_op,
                               const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObLockMemtable *memtable = nullptr;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(lock_op));
  } else if (OB_FAIL(lock_mem_ctx_.get_lock_memtable(memtable))) {
    TRANS_LOG(WARN, "get lock memtable failed.", K(ret));
  } else if (OB_FAIL(memtable->replay_lock(this, lock_op, scn))) {
    TRANS_LOG(WARN, "replay lock failed.", K(ret), K(lock_op));
  } else {
    // do nothing
  }
  return ret;
}

void ObMemtableCtx::reset_pdml_stat()
{
  trans_mgr_.reset_pdml_stat();
}

transaction::ObTransID ObMemtableCtx::get_tx_id() const
{
  return ctx_->get_trans_id();
}

void ObMemtableCtx::inc_unsubmitted_cnt()
{
  ATOMIC_INC(&unsubmitted_cnt_);
}

void ObMemtableCtx::dec_unsubmitted_cnt()
{
  ATOMIC_DEC(&unsubmitted_cnt_);
}

int ObMemtableCtx::check_tx_mem_size_overflow(bool &is_overflow)
{
  int ret = OB_SUCCESS;
  const int64_t threshold = 30L * 1024L * 1024L * 1024L; // 30G

  if (trans_mem_total_size_ >= threshold) {
    is_overflow = true;
  } else {
    is_overflow = false;
  }

  return ret;
}

void ObMemtableCtx::print_first_mvcc_callback()
{
  log_gen_.print_first_mvcc_callback();
}

void ObMemtableCtx::convert_checksum_for_commit_log(const ObIArray<uint64_t> &arr,
                                                    uint64_t &checksum,
                                                    ObIArray<uint8_t> &sig)
{
  // NOTE:
  // should skip empty callback-list, because it will not be replayed out on follower
  if (arr.count() == 0) {
    checksum = 0;
    sig.reset();
  } else if (arr.count() == 1) {
    checksum = arr.at(0);
    sig.reset();
  } else {
    int valid_cnt = 0;
    for (int i = 0; i < arr.count(); i++) {
      if (arr.at(i) && arr.at(i) != 1) { // skip empty list whose checksum is '1'
        ++valid_cnt;
        if (valid_cnt == 1) {
          checksum = arr.at(i);
        } else {
          checksum = ob_crc64(checksum, (void*)&arr.at(i), sizeof(uint64_t));
        }
      }
      sig.push_back((uint8_t)(arr.at(i) & 0xFF));
    }
    if (valid_cnt == 0) {
      checksum = arr.at(0);
      sig.reset();
    } else if (valid_cnt == 1) {
      sig.reset();
    }
  }
}

void ObMemtableCtx::check_all_redo_flushed()
{
  trans_mgr_.check_all_redo_flushed();
}

}
}

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
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_multi_data_source.h"
#include "share/ob_tenant_mgr.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "share/ob_force_print_log.h"
#include "lib/utility/ob_tracepoint.h"
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
    : ObIMemtableCtx(ctx_cb_allocator_),
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
      unsynced_cnt_(0),
      unsubmitted_cnt_(0),
      callback_mem_used_(0),
      callback_alloc_count_(0),
      callback_free_count_(0),
      is_read_only_(false),
      is_master_(true),
      has_row_updated_(false),
      lock_mem_ctx_(ctx_cb_allocator_),
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

int ObMemtableCtx::enable_lock_table(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_mem_ctx_.init(handle))) {
    TRANS_LOG(WARN, "lock mem ctx init failed", K(ret));
  }
  return ret;
}

void ObMemtableCtx::reset()
{
  if (IS_INIT) {
    if ((ATOMIC_LOAD(&callback_mem_used_) > 8 * 1024 * 1024) && REACH_TIME_INTERVAL(200000)) {
      TRANS_LOG(INFO, "memtable callback used", K(*this));
    }
    if (OB_UNLIKELY(callback_alloc_count_ != callback_free_count_)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "callback alloc and free count not match", K(*this));
    }
    if (OB_UNLIKELY(unsynced_cnt_ != 0)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "txn unsynced cnt not zero", K(*this),
                K(unsynced_cnt_), K(unsubmitted_cnt_));
      ob_abort();
    }
    if (OB_UNLIKELY(unsubmitted_cnt_ != 0)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "txn unsubmitted cnt not zero", K(*this),
                K(unsynced_cnt_), K(unsubmitted_cnt_));
      ob_abort();
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
    unsynced_cnt_ = 0;
    unsubmitted_cnt_ = 0;
    lock_mem_ctx_.reset();
    retry_info_.reset();
    trans_mgr_.reset();
    log_gen_.reset();
    ref_ = 0;
    is_master_ = true;
    is_read_only_ = false;
    end_code_ = OB_SUCCESS;
    tx_status_ = ObTxStatus::NORMAL;
    // blocked_trans_ids_.reset();
    tx_table_guard_.reset();
    //FIXME: ObIMemtableCtx don't have resetfunction,
    //thus ObIMvccCtx::reset is called, so resource_link_is not reset
    ObIMemtableCtx::reset();
  }
}

void ObMemtableCtx::reset_trans_table_guard()
{
  tx_table_guard_.reset();
}

int64_t ObMemtableCtx::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "{");
  pos += ObIMvccCtx::to_string(buf + pos, buf_len);
  common::databuff_printf(buf, buf_len, pos,
                          " end_code=%d tx_status=%ld is_readonly=%s "
                          "ref=%ld trans_id=%s ls_id=%ld "
                          "callback_alloc_count=%ld callback_free_count=%ld "
                          "checksum=%lu tmp_checksum=%lu checksum_scn=%s "
                          "redo_filled_count=%ld redo_sync_succ_count=%ld "
                          "redo_sync_fail_count=%ld main_list_length=%ld "
                          "unsynced_cnt=%ld unsubmitted_cnt_=%ld "
                          "cb_statistics:[main=%ld, slave=%ld, merge=%ld, "
                          "tx_end=%ld, rollback_to=%ld, "
                          "fast_commit=%ld, remove_memtable=%ld]",
                          end_code_, tx_status_, STR_BOOL(is_read_only_), ref_,
                          NULL == ctx_ ? "" : S(ctx_->get_trans_id()),
                          NULL == ctx_ ? -1 : ctx_->get_ls_id().id(),
                          callback_alloc_count_, callback_free_count_,
                          trans_mgr_.get_checksum(),
                          trans_mgr_.get_tmp_checksum(),
                          to_cstring(trans_mgr_.get_checksum_scn()),
                          log_gen_.get_redo_filled_count(),
                          log_gen_.get_redo_sync_succ_count(),
                          log_gen_.get_redo_sync_fail_count(),
                          trans_mgr_.get_main_list_length(),
                          unsynced_cnt_, unsubmitted_cnt_,
                          trans_mgr_.get_callback_main_list_append_count(),
                          trans_mgr_.get_callback_slave_list_append_count(),
                          trans_mgr_.get_callback_slave_list_merge_count(),
                          trans_mgr_.get_callback_remove_for_trans_end_count(),
                          trans_mgr_.get_callback_remove_for_rollback_to_count(),
                          trans_mgr_.get_callback_remove_for_fast_commit_count(),
                          trans_mgr_.get_callback_remove_for_remove_memtable_count());
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
  TRANS_LOG(TRACE, "mem_ctx.write_auth", K(ret), KPC(this));
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

void *ObMemtableCtx::callback_alloc(const int64_t size)
{
  void* ret = NULL;
  if (OB_ISNULL(ret = trans_mgr_.callback_alloc(size))) {
    TRANS_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "callback alloc error, no memory", K(size), K(*this));
  } else {
    ATOMIC_FAA(&callback_mem_used_, size);
    ATOMIC_INC(&callback_alloc_count_);
    TRANS_LOG(DEBUG, "callback alloc succ", K(*this), KP(ret), K(lbt()));
  }
  return ret;
}

void ObMemtableCtx::callback_free(ObITransCallback *cb)
{
  if (OB_ISNULL(cb)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cb is null, unexpected error", KP(cb), K(*this));
  } else if (cb->is_table_lock_callback()) {
    free_table_lock_callback(cb);
  } else {
    ATOMIC_INC(&callback_free_count_);
    TRANS_LOG(DEBUG, "callback release succ", KP(cb), K(*this), K(lbt()));
    trans_mgr_.callback_free(cb);
    cb = NULL;
  }
}

ObOBJLockCallback *ObMemtableCtx::alloc_table_lock_callback(ObIMvccCtx &ctx,
                                                            ObLockMemtable *memtable)
{
  int ret = OB_SUCCESS;
  void *cb_buffer = NULL;
  ObOBJLockCallback *cb = NULL;
  if (NULL == (cb_buffer = lock_mem_ctx_.alloc_lock_op_callback())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ObOBJLockCallback cb_buffer fail", K(ret));
  }
  if (NULL != cb_buffer) {
    if (NULL == (cb = new(cb_buffer) ObOBJLockCallback(ctx, memtable))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "construct ObOBJLockCallback object fail", K(ret), "cb_buffer", cb_buffer);
    }
  }
  return cb;
}

void ObMemtableCtx::free_table_lock_callback(ObITransCallback *cb)
{
  if (OB_ISNULL(cb)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cb is null, unexpected error", KP(cb), K(*this));
  } else {
    TRANS_LOG(DEBUG, "callback release succ", KP(cb), K(*this), K(lbt()));
    lock_mem_ctx_.free_lock_op_callback(cb);
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

int ObMemtableCtx::replay_begin(const SCN scn)
{
  ObByteLockGuard guard(lock_);

  set_redo_scn(scn);

  return OB_SUCCESS;
}

int ObMemtableCtx::replay_end(const bool is_replay_succ,
                              const SCN scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (!is_replay_succ) {
    ret = trans_mgr_.replay_fail(scn);
  } else {
    ret = trans_mgr_.replay_succ(scn);
  }

  return ret;
}

int ObMemtableCtx::rollback_redo_callbacks(const SCN scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  ret = trans_mgr_.replay_fail(scn);

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
      TRANS_LOG(ERROR, "callback alloc and free count not match", K(*this));
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
      && 0 != checksum
      && log_cluster_version >= CLUSTER_VERSION_3100
      && !ObServerConfig::get_instance().ignore_replay_checksum_error) {
    const uint64_t checksum4 = calc_checksum_all();
    if (checksum != checksum4) {
      cs_ret = OB_CHECKSUM_ERROR;
      TRANS_LOG(ERROR, "MT_CTX: replay checksum error", K(ret), K(*this),
                K(commit), K(checksum), K(checksum4));
    }
  }

  if (OB_FAIL(do_trans_end(commit,
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
  trans_mgr_.merge_multi_callback_lists();
  return OB_SUCCESS;
}

int ObMemtableCtx::fill_redo_log(char *buf,
                                 const int64_t buf_len,
                                 int64_t &buf_pos,
                                 ObRedoLogSubmitHelper &helper,
                                 const bool log_for_lock_node)
{
  int ret = OB_SUCCESS;

  if (NULL == buf
      || 0 >= buf_len
      || buf_len <= buf_pos) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(log_gen_.fill_redo_log(buf,
                                       buf_len,
                                       buf_pos,
                                       helper,
                                       log_for_lock_node))) {
      // When redo log data is greater than or equal to 1.875M, or participant has
      // no redo log data at all, this branch would be reached. Don't print log here
    }
    if (OB_FAIL(ret)) {
      if (buf_pos > buf_len) {
        TRANS_LOG(ERROR, "unexpected buf pos", KP(buf), K(buf_len), K(buf_pos));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  if (OB_SUCCESS != ret && OB_EAGAIN != ret && OB_ENTRY_NOT_EXIST != ret) {
    TRANS_LOG(WARN, "fill_redo_log fail", "ret", ret, "trans_id",
              NULL == ctx_ ? "" : S(ctx_->get_trans_id()),
              "buf", buf,
              "buf_len", buf_len,
              "buf_pos", buf_pos,
              "pending_log_size", trans_mgr_.get_pending_log_size(),
              K(*this));
  }

  return ret;
}


int ObMemtableCtx::log_submitted(const ObRedoLogSubmitHelper &helper)
{
  inc_pending_log_size(-1 * helper.data_size_);
  inc_flushed_log_size(helper.data_size_);
  return log_gen_.log_submitted(helper.callbacks_);
}

int ObMemtableCtx::sync_log_succ(const SCN scn, const ObCallbackScope &callbacks)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ATOMIC_LOAD(&end_code_)) {
    if (OB_FAIL(log_gen_.sync_log_succ(scn, callbacks))) {
      TRANS_LOG(WARN, "sync log failed", K(ret));
    }
  } else {
    if (!callbacks.is_empty()) {
      TRANS_LOG(INFO, "No memtable callbacks because of trans_end", K(end_code_), K(scn),
                KPC(ctx_));
    }
  }

  return ret;
}

void ObMemtableCtx::sync_log_fail(const ObCallbackScope &callbacks)
{
  int ret = OB_SUCCESS;
  if (!callbacks.is_empty()) {
    set_partial_rollbacked();
  }
  if (OB_SUCCESS == ATOMIC_LOAD(&end_code_)) {
    if (OB_FAIL(reuse_log_generator_())) {
      TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
    } else {
      log_gen_.sync_log_fail(callbacks);
    }
  } else {
    if (!callbacks.is_empty()) {
      TRANS_LOG(INFO, "No memtable callbacks because of trans_end", K(end_code_), KPC(ctx_));
    }
  }
  return;
}

uint64_t ObMemtableCtx::calc_checksum_all()
{
  ObByteLockGuard guard(lock_);
  if (0 == trans_mgr_.get_checksum()) {
    trans_mgr_.calc_checksum_all();
  }
  return trans_mgr_.get_checksum();
}

void ObMemtableCtx::print_callbacks()
{
  trans_mgr_.print_callbacks();
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

void ObMemtableCtx::update_max_submitted_seq_no(const transaction::ObTxSEQ seq_no)
{
  if (NULL != ATOMIC_LOAD(&ctx_)) {
    static_cast<ObPartTransCtx *>(ctx_)->update_max_submitted_seq_no(seq_no);
  }
}

int ObMemtableCtx::rollback(const transaction::ObTxSEQ to_seq_no, const transaction::ObTxSEQ from_seq_no)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("remove callbacks for rollback to", 10 * 1000);
  ObByteLockGuard guard(lock_);

  if (!to_seq_no.is_valid() || !from_seq_no.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(from_seq_no), K(to_seq_no));
  } else if (OB_ISNULL(ATOMIC_LOAD(&ctx_))) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "ctx is NULL", K(ret));
  } else if (OB_FAIL(reuse_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else if (OB_FAIL(trans_mgr_.rollback_to(to_seq_no, from_seq_no))) {
    TRANS_LOG(WARN, "rollback to failed", K(ret), K(*this));
  // rollback the table lock that with no tablelock callback
  } else if (OB_FAIL(rollback_table_lock_(to_seq_no))) {
    TRANS_LOG(WARN, "rollback table lock failed", K(ret), K(*this), K(to_seq_no));
  } else {
    TRANS_LOG(INFO, "memtable handle rollback to successfuly", K(from_seq_no), K(to_seq_no), K(*this));
  }
  return ret;
}

bool ObMemtableCtx::is_all_redo_submitted()
{
  ObByteLockGuard guard(lock_);
  return trans_mgr_.is_all_redo_submitted();
}

int ObMemtableCtx::remove_callbacks_for_fast_commit()
{
  int ret = OB_SUCCESS;
  bool meet_generate_cursor = false;
  common::ObTimeGuard timeguard("remove callbacks for fast commit", 10 * 1000);
  ObByteLockGuard guard(lock_);

  if (OB_FAIL(trans_mgr_.remove_callbacks_for_fast_commit(
                log_gen_.get_generate_cursor(),
                meet_generate_cursor))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), KPC(this));
  } else if (meet_generate_cursor && OB_FAIL(reuse_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret), KPC(this));
  }

  return ret;
}

int ObMemtableCtx::remove_callback_for_uncommited_txn(
  const memtable::ObMemtableSet *memtable_set,
  const share::SCN max_applied_scn)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("remove callbacks for uncommitted txn", 10 * 1000);
  ObByteLockGuard guard(lock_);

  if (OB_ISNULL(memtable_set)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is NULL", K(memtable_set));
  } else if (OB_FAIL(reuse_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else if (OB_FAIL(trans_mgr_.remove_callback_for_uncommited_txn(memtable_set,
                                                                   max_applied_scn))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), K(memtable_set));
  }

  return ret;
}

int ObMemtableCtx::clean_unlog_callbacks()
{
  int ret = OB_SUCCESS;
  int64_t removed_cnt = 0;
  {
    ObByteLockGuard guard(lock_);
    if (OB_FAIL(trans_mgr_.clean_unlog_callbacks(removed_cnt))) {
      TRANS_LOG(WARN, "clean unlog callbacks failed", KR(ret));
    } else {
      trans_mgr_.clear_pending_log_size();
    }
  }
  if (removed_cnt > 0) {
    set_partial_rollbacked();
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
                                            uint64_t &checksum,
                                            SCN &checksum_scn)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (OB_FAIL(trans_mgr_.calc_checksum_before_scn(scn, checksum, checksum_scn))) {
    TRANS_LOG(ERROR, "calc checksum before log ts should not report error", K(ret), K(scn));
  }

  return ret;
}

void ObMemtableCtx::update_checksum(const uint64_t checksum,
                                    const SCN checksum_scn)
{
  ObByteLockGuard guard(lock_);

  trans_mgr_.update_checksum(checksum, checksum_scn);
}

bool ObMemtableCtx::pending_log_size_too_large()
{
  bool ret = true;

  if (0 == GCONF._private_buffer_size) {
    ret = false;
  } else {
    ret = trans_mgr_.get_pending_log_size() > GCONF._private_buffer_size;
  }

  return ret;
}

// NB: We also donot hold the memtable context latch because it changes only
// callback list and multi callback lists which is protected by itself
void ObMemtableCtx::merge_multi_callback_lists_for_changing_leader()
{
  trans_mgr_.merge_multi_callback_lists();
}

void ObMemtableCtx::merge_multi_callback_lists_for_immediate_logging()
{
  trans_mgr_.merge_multi_callback_lists();
}

int ObMemtableCtx::get_table_lock_store_info(ObTableLockInfo &table_lock_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_mem_ctx_.get_table_lock_store_info(table_lock_info))) {
    TRANS_LOG(WARN, "get_table_lock_store_info failed", K(ret));
  }
  return ret;
}

int ObMemtableCtx::recover_from_table_lock_durable_info(const ObTableLockInfo &table_lock_info)
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
    } else if (OB_FAIL(lock_mem_ctx_.add_lock_record(lock_op, lock_op_node, true))) {
      TRANS_LOG(ERROR, "add_lock_record failed", K(ret), K(lock_op));
    } else if (OB_FAIL(lock_mem_ctx_.get_lock_memtable(lock_memtable))) {
      TRANS_LOG(ERROR, "get_lock_memtable failed", K(ret));
    } else if (OB_NOT_NULL(lock_memtable)
              && OB_FAIL(lock_memtable->recover_obj_lock(lock_op))) {
      TRANS_LOG(ERROR, "recover_obj_lock failed", K(ret), K(*lock_memtable));
    } else {
      lock_mem_ctx_.set_log_synced(lock_op_node, table_lock_info.max_durable_scn_);
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
                                    ObTableLockMode &lock_mode_in_same_trans) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_FAIL(lock_mem_ctx_.check_lock_exist(lock_id,
                                             owner_id,
                                             mode,
                                             op_type,
                                             is_exist,
                                             lock_mode_in_same_trans))) {
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
  const bool is_replay = false;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  ObLockMemtable *memtable = nullptr;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(lock_op));
  } else if (OB_FAIL(lock_mem_ctx_.add_lock_record(lock_op,
                                                   lock_op_node,
                                                   is_replay))) {
    TRANS_LOG(WARN, "create lock record at memtable failed. ", K(ret), K(lock_op), K(*this));
  } else if (OB_FAIL(register_multi_source_data_if_need_(lock_op, is_replay))) {
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
  const bool is_replay = true;
  ObMemCtxLockOpLinkNode *lock_op_node = nullptr;
  ObLockMemtable *memtable = nullptr;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(lock_op));
  } else if (OB_FAIL(lock_mem_ctx_.add_lock_record(lock_op,
                                                   lock_op_node,
                                                   is_replay))) {
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
    lock_mem_ctx_.set_log_synced(lock_op_node, scn);
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

void ObMemtableCtx::set_log_synced(ObMemCtxLockOpLinkNode *lock_op, const SCN &scn)
{
  lock_mem_ctx_.set_log_synced(lock_op, scn);
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

int ObMemtableCtx::rollback_table_lock_(transaction::ObTxSEQ seq_no)
{
  int ret = OB_SUCCESS;
  if (is_read_only_) {
    // read only trx no need deal with table lock.
  } else if (OB_FAIL(lock_mem_ctx_.rollback_table_lock(seq_no))) {
    TRANS_LOG(WARN, "clear table lock failed", KP(this));
  }

  return ret;
}

int ObMemtableCtx::register_multi_source_data_if_need_(
    const tablelock::ObTableLockOp &lock_op,
    const bool is_replay)
{
  int ret = OB_SUCCESS;
  ObRegisterMdsFlag mds_flag;
  mds_flag.reset();
  if (is_replay) {
    // replay does not need register multi source data, it is dealt
    // by multi source data itself.
  } else if (!lock_op.need_multi_source_data()) {
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

void ObMemtableCtx::inc_unsynced_cnt()
{
  ATOMIC_INC(&unsynced_cnt_);
}

void ObMemtableCtx::dec_unsynced_cnt()
{
  ATOMIC_DEC(&unsynced_cnt_);
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

}
}

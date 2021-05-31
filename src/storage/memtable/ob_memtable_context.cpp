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
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_data.h"
#include "ob_memtable.h"
#include "storage/transaction/ob_trans_part_ctx.h"
#include "storage/transaction/ob_trans_define.h"
#include "share/ob_tenant_mgr.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "share/ob_force_print_log.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction;

namespace memtable {

uint32_t ObMemtableCtx::UID_FOR_PURGE = 0;

ObMemtableCtx::ObMemtableCtx(MemtableIDMap& ctx_map, ObIAllocator& allocator)
    : ObIMemtableCtx(),
      lock_(),
      stmt_relocate_lock_(),
      end_code_(OB_SUCCESS),
      ref_(0),
      ctx_map_(&ctx_map),
      query_allocator_(),
      ctx_cb_allocator_(),
      checksum_log_ts_(0),
      checksum_(0),
      handle_start_time_(0),
      log_conflict_interval_(LOG_CONFLICT_INTERVAL),
      ctx_(NULL),
      mutator_iter_(NULL),
      arena_(),
      memtable_for_cur_log_(NULL),
      memtable_for_batch_commit_(NULL),
      memtable_for_elr_(NULL),
      relocate_cnt_(0),
      truncate_cnt_(0),
      lock_for_read_retry_count_(0),
      lock_for_read_elapse_(0),
      trans_mem_total_size_(0),
      thread_local_ctx_(NULL),
      callback_mem_used_(0),
      callback_alloc_count_(0),
      callback_free_count_(0),
      trans_table_status_(ObTransTableStatusType::RUNNING),
      is_read_only_(false),
      is_master_(true),
      data_relocated_(false),
      read_elr_data_(false),
      is_self_alloc_ctx_(false),
      is_inited_(false)
{
  UNUSED(allocator);
}

ObMemtableCtx::ObMemtableCtx()
    : ObIMemtableCtx(),
      lock_(),
      stmt_relocate_lock_(),
      end_code_(OB_SUCCESS),
      ref_(0),
      ctx_map_(NULL),
      query_allocator_(),
      ctx_cb_allocator_(),
      checksum_log_ts_(0),
      checksum_(0),
      handle_start_time_(0),
      log_conflict_interval_(LOG_CONFLICT_INTERVAL),
      ctx_(NULL),
      mutator_iter_(NULL),
      arena_(),
      memtable_for_cur_log_(NULL),
      memtable_for_batch_commit_(NULL),
      memtable_for_elr_(NULL),
      relocate_cnt_(0),
      truncate_cnt_(0),
      lock_for_read_retry_count_(0),
      lock_for_read_elapse_(0),
      trans_mem_total_size_(0),
      thread_local_ctx_(NULL),
      callback_mem_used_(0),
      callback_alloc_count_(0),
      callback_free_count_(0),
      trans_table_status_(ObTransTableStatusType::RUNNING),
      is_read_only_(false),
      is_master_(true),
      data_relocated_(false),
      read_elr_data_(false),
      is_self_alloc_ctx_(false),
      is_inited_(false)
{}

ObMemtableCtx::~ObMemtableCtx()
{
  // do not invoke virtual function in deconstruct
  ObMemtableCtx::reset();
}

int ObMemtableCtx::init(const uint64_t tenant_id, MemtableIDMap& ctx_map, common::ObIAllocator& malloc_allocator)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {  // use is_inited_ to prevent memtable ctx from being inited repeatedly
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    ModulePageAllocator page_allocator(ObModIds::OB_MEMTABLE_CTX_ARENA, tenant_id, 0);
    if (OB_FAIL(query_allocator_.init(&malloc_allocator, tenant_id))) {
      TRANS_LOG(ERROR, "query_allocator init error", K(ret));
    } else if (OB_FAIL(ctx_cb_allocator_.init(&malloc_allocator, tenant_id))) {
      TRANS_LOG(ERROR, "ctx_allocator_ init error", K(ret));
    } else if (OB_FAIL(arena_.init(ModuleArena::DEFAULT_PAGE_SIZE, page_allocator))) {
      TRANS_LOG(WARN, "arena init failed", K(ret));
    } else if (OB_FAIL(reset_log_generator_())) {
      TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
    } else {
      ctx_map_ = &ctx_map;
      is_inited_ = true;
    }
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
      TRANS_LOG(ERROR, "callback alloc and free count not match", K(*this));
    }
    if (NULL != memtable_for_cur_log_) {
      memtable_for_cur_log_ = NULL;
      TRANS_LOG(ERROR, "memtable_for_cur_log should be NULL", K(*this));
    }
    memtable_for_batch_commit_ = NULL;
    memtable_for_elr_ = NULL;
    is_inited_ = false;
    trans_table_status_ = ObTransTableStatusType::RUNNING;
    callback_free_count_ = 0;
    callback_alloc_count_ = 0;
    callback_mem_used_ = 0;
    thread_local_ctx_ = NULL;
    read_elr_data_ = false;
    trans_mem_total_size_ = 0;
    memtable_arr_wrap_.reset();
    lock_for_read_retry_count_ = 0;
    lock_for_read_elapse_ = 0;
    truncate_cnt_ = 0;
    relocate_cnt_ = 0;
    partition_audit_info_cache_.reset();
    data_relocated_ = false;
    arena_.free();
    if (OB_NOT_NULL(mutator_iter_)) {
      ctx_cb_allocator_.free(mutator_iter_);
      mutator_iter_ = NULL;
    }
    // FIXME: ctx_ is not reset
    log_conflict_interval_.reset();
    mtstat_.reset();
    handle_start_time_ = 0;
    checksum_ = 0;
    checksum_log_ts_ = 0;
    batch_checksum_.reset();
    trans_mgr_.reset();
    log_gen_.reset();
    ctx_cb_allocator_.reset();
    query_allocator_.reset();
    // FIXME: ctx_map_ is not reset
    ref_ = 0;
    is_master_ = true;
    is_read_only_ = false;
    end_code_ = OB_SUCCESS;
    // FIXME: lock_ is not reset
    is_standalone_ = false;  // FIXME: this field is a base class member, should not be reseted by child class
    is_self_alloc_ctx_ = false;
    trans_table_guard_.reset();
    // FIXME: ObIMemtableCtx don't have resetfunction,
    // thus ObIMvccCtx::reset is called, so resource_link_is not reset
    ObIMemtableCtx::reset();
  }
}
void ObMemtableCtx::reset_trans_table_guard()
{
  trans_table_guard_.reset();
}

int64_t ObMemtableCtx::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  pos = ObIMvccCtx::to_string(buf, buf_len);
  common::databuff_printf(buf,
      buf_len,
      pos,
      "end_code=%d is_readonly=%s ref=%ld pkey=%s trans_id=%s data_relocated=%d "
      "relocate_cnt=%ld truncate_cnt=%ld trans_mem_total_size=%ld "
      "callback_alloc_count=%ld callback_free_count=%ld callback_mem_used=%ld "
      "checksum_log_ts=%lu",
      end_code_,
      STR_BOOL(is_read_only_),
      ref_,
      NULL == ctx_ ? "" : S(ctx_->get_partition()),
      NULL == ctx_ ? "" : S(ctx_->get_trans_id()),
      data_relocated_,
      relocate_cnt_,
      truncate_cnt_,
      trans_mem_total_size_,
      callback_alloc_count_,
      callback_free_count_,
      callback_mem_used_,
      checksum_log_ts_);
  return pos;
}

int ObMemtableCtx::check_memstore_count(int64_t& count)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(memtable_arr_wrap_.check_memtable_count(count))) {
    TRANS_LOG(WARN, "check memstore count error", K(ret), K(*this));
  }
  if (OB_FAIL(ret) && NULL != ATOMIC_LOAD(&ctx_)) {
    ctx_->set_need_print_trace_log();
  }
  return ret;
}

int ObMemtableCtx::set_host_(ObMemtable* host, const bool for_replay)
{
  int ret = OB_SUCCESS;
  if (NULL == host) {
    TRANS_LOG(WARN, "invalid argument", KP(host));
    ret = OB_INVALID_ARGUMENT;
    // leader active is not allowed when replay is on going
  } else if (true == for_replay && false == trans_mgr_.is_for_replay()) {
    TRANS_LOG(ERROR, "unexpected for replay status", K(for_replay), K(*this));
    ret = OB_ERR_UNEXPECTED;
  } else if (false == for_replay && true == trans_mgr_.is_for_replay()) {
    ret = OB_NOT_MASTER;
  } else if (host == get_active_mt()) {
    // do nothing
  } else if (memtable_arr_wrap_.is_reach_max_memtable_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "reach max memstore in storage", K(ret), K(*this), KP(host));
  } else {
    if (memtable_arr_wrap_.is_contain_this_memtable(host)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "memstore already exist", K(ret), K(*this), KP(host));
    } else if (OB_FAIL(host->inc_active_trx_count())) {
      TRANS_LOG(ERROR, "increase active_trx_count error", K(ret), K(*this), KP(host));
    } else {
      memtable_arr_wrap_.add_memtable(host);
    }
  }
  return ret;
}

int ObMemtableCtx::set_leader_host(ObMemtable* host, const bool for_replay)
{
  // the lock is already acquired outside, don't need to lock here
  return set_host_(host, for_replay);
}

int ObMemtableCtx::set_replay_host(ObMemtable* host, const bool for_replay)
{
  ObByteLockGuard guard(lock_);
  return set_host_(host, for_replay);
}

void ObMemtableCtx::set_read_only()
{
  ATOMIC_STORE(&is_read_only_, true);
}

void ObMemtableCtx::set_trans_ctx(ObTransCtx* ctx)
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

int ObMemtableCtx::write_auth()
{
  int ret = OB_SUCCESS;
  bool lock_succ = true;
  if (OB_UNLIKELY(0 == lock_.try_lock())) {
    lock_succ = false;
    if (OB_SUCCESS != ATOMIC_LOAD(&end_code_)) {
      ret = ATOMIC_LOAD(&end_code_);
      TRANS_LOG(
          WARN, "WriteAuth: trans is already end", "trans_id", NULL == ctx_ ? "" : S(ctx_->get_trans_id()), K(ret));
    } else if (!ATOMIC_LOAD(&is_master_)) {
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN,
          "WriteAuth: trans is already not master",
          "trans_id",
          NULL == ctx_ ? "" : S(ctx_->get_trans_id()),
          K(ret));
    } else {
      // ret = OB_STATE_NOT_MATCH;
      // TRANS_LOG(WARN, "WriteAuth fail: concurrent update",
      //    "trans_id", NULL == ctx_ ? "" : S(ctx_->get_trans_id()), K(ret));
      lock_.lock();
      lock_succ = true;
    }
  }

  if (lock_succ) {
    if (ATOMIC_LOAD(&is_read_only_)) {
      ret = OB_ERR_READ_ONLY_TRANSACTION;

      TRANS_LOG(ERROR,
          "WriteAuth fail: readonly trans not support update operation",
          "trans_id",
          NULL == ctx_ ? "" : S(ctx_->get_trans_id()),
          K(ret));
    } else if (OB_SUCCESS != ATOMIC_LOAD(&end_code_)) {
      ret = ATOMIC_LOAD(&end_code_);
      TRANS_LOG(
          WARN, "WriteAuth: trans is already end", "trans_id", NULL == ctx_ ? "" : S(ctx_->get_trans_id()), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (lock_succ) {
      lock_.unlock();
    }
  }
  return ret;
}

int ObMemtableCtx::write_done()
{
  int ret = OB_SUCCESS;
  lock_.unlock();
  return ret;
}

int ObMemtableCtx::read_lock_yield()
{
  return ATOMIC_LOAD(&end_code_);
}

int ObMemtableCtx::write_lock_yield()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_done())) {
    TRANS_LOG(WARN, "write_done fail", K(ret));
  } else if (OB_FAIL(write_auth())) {
    TRANS_LOG(WARN, "write_auth fail", K(ret));
  }
  return ret;
}

void ObMemtableCtx::on_wlock_retry(const ObMemtableKey& key, const char* conflict_ctx)
{
  mtstat_.on_wlock_retry();
  if (log_conflict_interval_.reach()) {
    TRANS_LOG(WARN, "lock_for_write conflict", K(*this), K(key), K(conflict_ctx));
  }
}

void ObMemtableCtx::on_tsc_retry(const ObMemtableKey& key)
{
  mtstat_.on_tsc_retry();
  if (log_conflict_interval_.reach()) {
    TRANS_LOG(WARN, "transaction_set_consistency conflict", K(*this), K(key));
  }
}

int ObMemtableCtx::row_compact(ObMvccRow* value, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "value is NULL");
  } else if (NULL == get_active_mt()) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "host not set", K(ret), K(value), K(snapshot_version), K(*this), K(lbt()));
  } else if (!data_relocated_ && OB_FAIL(get_active_mt()->row_compact(value, snapshot_version))) {
    TRANS_LOG(WARN, "row_compact fail", K(ret), K(value), K(snapshot_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObMemtableCtx::wait_trans_version(const uint32_t descriptor, const int64_t version)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = E(EventTable::EN_TRANS_SHARED_LOCK_CONFLICT) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "Inject SHARED_LOCK_CONFLICT", K(ret), K(descriptor), K(version));
    return ret;
  }
#endif
  ObIMemtableCtx* ctx = NULL;
  if (NULL == ctx_map_) {
    TRANS_LOG(ERROR, "id map is null, unexpected error", KP_(ctx_map));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 == descriptor) {
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else if (UID_FOR_PURGE == descriptor) {
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else if (descriptor == get_ctx_descriptor()) {
    // need not wait
  } else if (NULL == (ctx = ctx_map_->fetch(descriptor))) {
    TRANS_LOG(WARN, "trans is finished, no need to wait", K(descriptor));
  } else if (ctx->get_trans_version() <= version && ctx->get_commit_version() <= version) {
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
    ObMemtableCtx* mem_ctx = (ObMemtableCtx*)ctx;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(WARN, "lock_for_read need retry", K(ret), K(*this), K(*mem_ctx), K(version));
    }
  }
  if (NULL != ctx) {
    ctx_map_->revert(descriptor);
    ctx = NULL;
  }
  return ret;
}

int ObMemtableCtx::try_elr_when_lock_for_write(
    const uint32_t descriptor, const int64_t version, const ObMemtableKey* key, const ObMvccRow& row)
{
  int ret = OB_SUCCESS;

  ObIMemtableCtx* ctx = NULL;
  if (NULL == ctx_map_) {
    TRANS_LOG(ERROR, "id map is null, unexpected error", KP_(ctx_map));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 == descriptor) {
    ret = OB_EAGAIN;
  } else if (UID_FOR_PURGE == descriptor) {
    ret = OB_TRY_LOCK_ROW_CONFLICT;
  } else if (descriptor == get_ctx_descriptor()) {
    ret = OB_EAGAIN;
  } else if (const_cast<ObMvccRow&>(row).is_transaction_set_violation(version)) {
    ret = OB_TRANSACTION_SET_VIOLATION;
  } else if (NULL == (ctx = ctx_map_->fetch(descriptor))) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "trans is finished, no need to wait", K(ret), K(descriptor));
  } else {
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx);
    bool elr_prepared = false;
    int64_t elr_commit_version = -1;
    if (OB_FAIL(check_trans_elr_prepared_(mt_ctx->ctx_, elr_prepared, elr_commit_version))) {
      TRANS_LOG(WARN, "check transaction can early release lock error", K(ret), K(row), K(*this));
    } else if (!elr_prepared) {
      ret = OB_TRY_LOCK_ROW_CONFLICT;
    } else if (OB_FAIL(const_cast<ObMvccRow&>(row).row_elr(descriptor, elr_commit_version, key))) {
      TRANS_LOG(WARN, "early release lock prev trans error", K(ret), K(row), K(*this));
    } else {
      // do nothing
    }
  }

  if (NULL != ctx) {
    ctx_map_->revert(descriptor);
    ctx = NULL;
  }

  return ret;
}

int ObMemtableCtx::wait_trans_version_v2(
    const uint32_t descriptor, const int64_t version, const ObMemtableKey* key, const ObMvccRow& row)
{
  int ret = OB_SUCCESS;
  ObIMemtableCtx* ctx = NULL;
  if (NULL == ctx_map_) {
    TRANS_LOG(ERROR, "id map is null, unexpected error", KP_(ctx_map));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 == descriptor) {
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else if (UID_FOR_PURGE == descriptor) {
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else if (descriptor == get_ctx_descriptor()) {
    // need not wait
  } else if (NULL == (ctx = ctx_map_->fetch(descriptor))) {
    TRANS_LOG(WARN, "trans is finished, no need to wait", K(descriptor));
  } else if (ctx->get_trans_version() <= version && ctx->get_commit_version() <= version) {
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx);
    bool elr_prepared = false;
    int64_t elr_commit_version = -1;
    if (OB_FAIL(check_trans_elr_prepared_(mt_ctx->ctx_, elr_prepared, elr_commit_version))) {
      TRANS_LOG(WARN, "check transaction can early release lock error", K(ret), K(row), K(*this));
    } else if (!elr_prepared) {
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    } else if (version > elr_commit_version + MAX_ELR_TRANS_INTERVAL) {
      TRANS_LOG(WARN, "prev transaction commit too slow error", K(version), K(elr_commit_version), K(*this));
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    } else if (version < elr_commit_version) {
      // do nothing
    } else if (NULL == ATOMIC_LOAD(&ctx_)) {
      // a read only transaction which don't create context is not allowed to read elr data
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    } else if (row.get_elr_trans_count() >= GCONF._max_elr_dependent_trx_count * 2) {
      // this row has too many elr transactions
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
      TRANS_LOG(WARN, "too many early lock release trans", K(row));
    } else if (OB_FAIL(const_cast<ObMvccRow&>(row).row_elr(descriptor, elr_commit_version, key))) {
      TRANS_LOG(WARN, "early release lock prev trans error", K(ret), K(row), K(*this));
    } else if (OB_FAIL(insert_prev_trans_(descriptor, mt_ctx->ctx_))) {
      if (OB_ERR_SHARED_LOCK_CONFLICT != ret) {
        TRANS_LOG(WARN, "insert prev trans error", K(ret), K(descriptor), K(*this));
      }
    } else {
      // do nothing
    }
  } else {
    // do nothing
  }
  if (NULL != ctx) {
    ctx_map_->revert(descriptor);
    ctx = NULL;
  }
  return ret;
}

int ObMemtableCtx::check_trans_elr_prepared_(ObTransCtx* trans_ctx, bool& elr_prepared, int64_t& elr_commit_version)
{
  int ret = OB_SUCCESS;

  if (NULL == trans_ctx) {
    elr_prepared = false;
  } else {
    ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(trans_ctx);
    if (OB_FAIL(part_ctx->check_elr_prepared(elr_prepared, elr_commit_version))) {
      TRANS_LOG(WARN, "check transaction elr prepared error", K(ret), K(*this));
    }
  }

  return ret;
}

int ObMemtableCtx::insert_prev_trans(const uint32_t descriptor)
{
  int ret = OB_SUCCESS;
  ObIMemtableCtx* ctx = NULL;
  if (NULL == ctx_map_) {
    TRANS_LOG(ERROR, "id map is null, unexpected error", KP_(ctx_map));
    ret = OB_ERR_UNEXPECTED;
  } else if (0 == descriptor) {
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else if (UID_FOR_PURGE == descriptor) {
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else if (OB_ISNULL(ATOMIC_LOAD(&ctx_))) {
    // a read only transaction which don't create context is not allowed to read elr data
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
  } else if (ctx_->is_for_replay()) {
    // if this ctx has switched to a follwower, lock for read need to wait
    ret = OB_ERR_SHARED_LOCK_CONFLICT;
    TRANS_LOG(WARN, "current trans already leader revoked, need retry", K(ret), K(*this));
  } else if (descriptor == get_ctx_descriptor()) {
    // need not wait
  } else if (NULL == (ctx = ctx_map_->fetch(descriptor))) {
    TRANS_LOG(WARN, "trans is finished, no need to wait", K(descriptor));
  } else {
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx);
    if (OB_FAIL(insert_prev_trans_(descriptor, mt_ctx->ctx_))) {
      if (OB_ERR_SHARED_LOCK_CONFLICT != ret) {
        TRANS_LOG(WARN, "insert prev trans error", K(ret), K(descriptor), K(*this));
      }
    }
    ctx_map_->revert(descriptor);
    ctx = NULL;
  }
  return ret;
}

int ObMemtableCtx::insert_prev_trans_(const uint32_t ctx_id, ObTransCtx* prev_trans_ctx)
{
  int ret = OB_SUCCESS;
  ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx_);

  if (OB_FAIL(part_ctx->insert_prev_trans(ctx_id, prev_trans_ctx))) {
    if (OB_EAGAIN == ret) {
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
    } else {
      TRANS_LOG(WARN, "insert prev trans arr error", K(ret), K(ctx_id), K(*prev_trans_ctx), K(*this));
    }
  } else {
    // elr data is read, set flag, would be skipped in the purge process
    set_read_elr_data(true);
  }

  return ret;
}

void* ObMemtableCtx::callback_alloc(const int64_t size)
{
  void* ret = NULL;
  if (OB_ISNULL(ret = ctx_cb_allocator_.alloc(size))) {
    TRANS_LOG(ERROR, "callback alloc error, no memory", K(size), K(*this));
  } else {
    ATOMIC_FAA(&callback_mem_used_, size);
    ATOMIC_INC(&callback_alloc_count_);
    TRANS_LOG(DEBUG, "callback alloc succ", K(*this), KP(ret), K(lbt()));
  }
  return ret;
}

void* ObMemtableCtx::arena_alloc(const int64_t size)
{
  return arena_.alloc(size);
}

void ObMemtableCtx::callback_free(void* cb)
{
  if (OB_ISNULL(cb)) {
    TRANS_LOG(ERROR, "cb is null, unexpected error", KP(cb), K(*this));
  } else {
    ATOMIC_INC(&callback_free_count_);
    TRANS_LOG(DEBUG, "callback release succ", KP(cb), K(*this), K(lbt()));
    ctx_cb_allocator_.free(cb);
    cb = NULL;
  }
}

ObIAllocator& ObMemtableCtx::get_query_allocator()
{
  return query_allocator_;
}

int ObMemtableCtx::trans_begin()
{
  int ret = OB_SUCCESS;

  set_trans_start_time(get_us());
  trans_mgr_.trans_start();
  trans_mgr_sub_trans_begin_();
  return ret;
}

// INT64_MAX is invalid for snapshot version,
// if merge and warm up need to use, the snapshot version should minus n
int ObMemtableCtx::sub_trans_begin(
    const int64_t snapshot, const int64_t abs_expired_time, const bool is_safe_read, const int64_t trx_lock_timeout)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == snapshot) {
    TRANS_LOG(ERROR, "snapshot verison is INT64_MAX, unexpected error", K(snapshot), K(abs_expired_time), K(*this));
    ret = OB_ERR_UNEXPECTED;
  } else {
    set_is_safe_read(is_safe_read);
    set_read_snapshot(snapshot);
    set_abs_expired_time(abs_expired_time);
    set_trx_lock_timeout(trx_lock_timeout);
    set_stmt_start_time(get_us());
    trans_mgr_sub_trans_end_(true);
    trans_mgr_sub_trans_begin_();
    ATOMIC_STORE(&lock_for_read_retry_count_, 0);
  }
  return ret;
}

int ObMemtableCtx::fast_select_trans_begin(
    const int64_t snapshot, const int64_t abs_expired_time, const int64_t trx_lock_timeout)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == snapshot) {
    TRANS_LOG(ERROR, "snapshot verison is INT64_MAX, unexpected error", K(snapshot), K(abs_expired_time), K(*this));
    ret = OB_ERR_UNEXPECTED;
  } else {
    set_read_snapshot(snapshot);
    set_abs_expired_time(abs_expired_time);
    set_trx_lock_timeout(trx_lock_timeout);
    set_stmt_start_time(get_us());
    ATOMIC_STORE(&lock_for_read_retry_count_, 0);
  }
  return ret;
}

void ObMemtableCtx::trans_mgr_sub_trans_begin_()
{
  trans_mgr_.sub_trans_begin();
}

// it is possile that one partition's physical plan is still
// on going, but other partitions have already failed,
// and statement rollback is triggered
int ObMemtableCtx::sub_trans_end(const bool commit)
{
  ObByteLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  trans_mgr_sub_trans_end_(commit);
  return ret;
}

int ObMemtableCtx::sub_stmt_begin()
{
  int ret = OB_SUCCESS;
  trans_mgr_.sub_stmt_begin();
  return ret;
}

int ObMemtableCtx::sub_stmt_end(const bool commit)
{
  ObByteLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  trans_mgr_.sub_stmt_end(commit);
  return ret;
}

int ObMemtableCtx::trans_end(const bool commit, const int64_t trans_version)
{
  int ret = OB_SUCCESS;

  ret = do_trans_end(commit, trans_version, commit ? OB_TRANS_COMMITED : OB_TRANS_ROLLBACKED);

  return ret;
}

int ObMemtableCtx::trans_clear()
{
  return memtable_arr_wrap_.clear_mt_arr();
}

int ObMemtableCtx::elr_trans_preparing()
{
  ObByteLockGuard guard(lock_);
  // during elr, trans commit may be fast, ans has already triggered trans_commit,
  // elr is not needed under such condition
  if (NULL != ATOMIC_LOAD(&ctx_) && OB_SUCCESS == end_code_) {
    set_commit_version(static_cast<ObPartTransCtx*>(ctx_)->get_commit_version());
    trans_mgr_.elr_trans_preparing();
  }
  return OB_SUCCESS;
}

int ObMemtableCtx::do_trans_end(const bool commit, const int64_t trans_version, const int end_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_SUCCESS == ATOMIC_LOAD(&end_code_)) {
    ATOMIC_STORE(&end_code_, end_code);
    ObByteLockGuard guard(lock_);
    set_commit_version(trans_version);
    if (OB_FAIL(trans_mgr_.trans_end(commit))) {
      TRANS_LOG(WARN, "trans end error", K(ret), K(*this));
    } else {
      // transaction has commited successfully, don't set ret
      if (commit) {
        if (OB_FAIL(memtable_arr_wrap_.update_all_mt_max_trans_version(trans_version))) {
          TRANS_LOG(ERROR, "update all mt max trans verison error", K(ret), K(trans_version), K(*this));
        } else {
          int64_t schema_version = get_max_table_version();
          if (INT64_MAX == schema_version) {
            TRANS_LOG(ERROR, "invalid schema version", K(ret), K(*this));
          } else {
            memtable_arr_wrap_.update_all_mt_max_schema_version(schema_version);
          }
        }
      }
    }
    // set trans status after update
    trans_table_status_ = commit ? ObTransTableStatusType::COMMIT : ObTransTableStatusType::ABORT;
    // after a transaction finishes, callback memory should be released
    // and check memory leakage
    if (OB_UNLIKELY(ATOMIC_LOAD(&callback_alloc_count_) != ATOMIC_LOAD(&callback_free_count_))) {
      TRANS_LOG(ERROR, "callback alloc and free count not match", K(*this));
    }
    // flush partition audit statistics cached in ctx to partition
    if (NULL != ATOMIC_LOAD(&ctx_) && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = flush_audit_partition_cache_(commit)))) {
      TRANS_LOG(WARN, "flush audit partition cache error", K(tmp_ret), K(commit), K(*ctx_));
    }
  }
  clear_memtable_for_cur_log();
  dec_pending_batch_commit_count();
  dec_pending_elr_count();
  return ret;
}

int ObMemtableCtx::trans_kill()
{
  int ret = OB_SUCCESS;
  bool commit = false;
  ret = do_trans_end(commit, INT64_MAX, OB_TRANS_KILLED);
  return ret;
}

int ObMemtableCtx::fake_kill()
{
  int ret = OB_SUCCESS;

  trans_table_status_ = ObTransTableStatusType::ABORT;

  return ret;
}

bool ObMemtableCtx::is_slow_query() const
{
  return false;
  // return get_us() - get_trans_start_time() >= SLOW_QUERY_THRESHOULD;
}

int ObMemtableCtx::trans_publish()
{
  return OB_SUCCESS;
}

int ObMemtableCtx::trans_replay_begin()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_master_, false);
  set_trans_start_time(get_us());
  return ret;
}

int ObMemtableCtx::trans_replay_end(const bool commit, const int64_t trans_version, const uint64_t checksum)
{
  int ret = OB_SUCCESS;
  int cs_ret = OB_SUCCESS;

  if (commit && 0 != checksum && !ObServerConfig::get_instance().ignore_replay_checksum_error) {
    const uint64_t checksum4 = calc_checksum4();
    if (checksum != checksum4) {
      cs_ret = OB_CHECKSUM_ERROR;
      TRANS_LOG(ERROR, "MT_CTX: replay checksum error", K(ret), K(*this), K(commit), K(checksum), K(checksum4));
      // TODO: REMOVE IT
      ob_abort();
    }
  }

  if (OB_FAIL(trans_end(commit, trans_version))) {
    TRANS_LOG(ERROR, "trans_end fail", K(ret), K(*this));
  } else {
    ret = cs_ret;
  }
  clear_memtable_for_cur_log();
  dec_pending_batch_commit_count();
  dec_pending_elr_count();
  return ret;
}

int ObMemtableCtx::trans_pending()
{
  trans_mgr_.trans_pending();
  return OB_SUCCESS;
}

int ObMemtableCtx::trans_link()
{
  trans_mgr_.trans_link();
  return OB_SUCCESS;
}

// leader takeover actions
int ObMemtableCtx::replay_to_commit()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_master_, true);
  ObByteLockGuard guard(lock_);
  trans_mgr_.set_for_replay(false);
  trans_mgr_.clear_pending_log_size();
  ObTransCallbackList& callback_list = trans_mgr_.get_callback_list();
  if (OB_FAIL(trans_link())) {
    TRANS_LOG(ERROR, "link trans failed", K(ret));
  } else if (OB_FAIL(reset_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else {
    // do nothing
  }
  if (OB_SUCCESS == ret) {
    TRANS_LOG(INFO, "replay to commit success", K(this), K(callback_list.get_guard()));
  } else {
    TRANS_LOG(ERROR, "replay to commit failed", K(ret), K(this), K(callback_list.get_guard()));
  }
  return ret;
}

// leader revoke actions
int ObMemtableCtx::commit_to_replay()
{
  ATOMIC_STORE(&is_master_, false);
  ObByteLockGuard guard(lock_);
  trans_mgr_.set_for_replay(true);
  trans_mgr_.clear_pending_log_size();
  return trans_pending();
}

int ObMemtableCtx::get_trans_status() const
{
  return ATOMIC_LOAD(&end_code_);
}

void ObMemtableCtx::trans_mgr_sub_trans_end_(bool commit)
{
  // after each statement, last statement's flag
  // should be cleared: whether elr data has been read
  set_read_elr_data(false);
  trans_mgr_.sub_trans_end(commit);
  // update partition audit
  if (GCONF.enable_sql_audit) {
    (void)partition_audit_info_cache_.stmt_end_update_audit_info(commit);
  }
}

void ObMemtableCtx::half_stmt_commit()
{
  ObByteLockGuard guard(lock_);
  trans_mgr_.half_stmt_commit();
}

int ObMemtableCtx::fill_redo_log(char* buf, const int64_t buf_len, int64_t& buf_pos)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || 0 >= buf_len || buf_len <= buf_pos) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(log_gen_.fill_redo_log(buf, buf_len, buf_pos))) {
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
    TRANS_LOG(WARN,
        "fill_redo_log fail",
        "ret",
        ret,
        "trans_id",
        NULL == ctx_ ? "" : S(ctx_->get_trans_id()),
        "buf",
        buf,
        "buf_len",
        buf_len,
        "buf_pos",
        buf_pos);
  } else {
    inc_pending_log_size(-1 * log_gen_.get_generating_data_size());
    inc_flushed_log_size(log_gen_.get_generating_data_size());
  }

  return ret;
}

int ObMemtableCtx::undo_fill_redo_log()
{
  inc_pending_log_size(log_gen_.get_generating_data_size());
  inc_flushed_log_size(-1 * log_gen_.get_generating_data_size());
  return log_gen_.undo_fill_redo_log();
}

void ObMemtableCtx::sync_log_succ(const int64_t log_id, const int64_t log_ts, bool& has_pending_cb)
{
  has_pending_cb = true;
  return log_gen_.sync_log_succ(log_id, log_ts, has_pending_cb);
}

void ObMemtableCtx::sync_log_succ(const int64_t log_id, const int64_t log_ts)
{
  bool unused = true;
  return sync_log_succ(log_id, log_ts, unused);
}

int ObMemtableCtx::add_crc(const void* key, ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size)
{
  int ret = OB_SUCCESS;
  UNUSED(value);
  UNUSED(data_size);
  if (OB_ISNULL(key) || OB_ISNULL(value) || OB_ISNULL(tnode)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", K(key), K(value), K(tnode));
  } else {
    ((ObMemtableKey*)key)->checksum(batch_checksum_);
    tnode->checksum(batch_checksum_);
    ((ObMemtableDataHeader*)tnode->buf_)->checksum(batch_checksum_);
    batch_checksum_.fill(&commit_version_, sizeof(commit_version_));
  }
  return ret;
}

int ObMemtableCtx::add_crc2(ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size)
{
  int ret = OB_SUCCESS;
  UNUSED(value);
  UNUSED(data_size);
  if (OB_ISNULL(value) || OB_ISNULL(tnode)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", K(value), K(tnode));
  } else {
    tnode->checksum(batch_checksum_);
    ((ObMemtableDataHeader*)tnode->buf_)->checksum(batch_checksum_);
    batch_checksum_.fill(&commit_version_, sizeof(commit_version_));
  }
  return ret;
}

int ObMemtableCtx::add_crc4(ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  UNUSED(value);
  UNUSED(data_size);
  if (OB_ISNULL(value) || OB_ISNULL(tnode)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid_argument", K(value), K(tnode), K(log_ts));
  } else if (ATOMIC_LOAD(&checksum_log_ts_) <= log_ts) {
    tnode->checksum(batch_checksum_);
    ((ObMemtableDataHeader*)tnode->buf_)->checksum(batch_checksum_);
    batch_checksum_.fill(&commit_version_, sizeof(commit_version_));
    ATOMIC_STORE(&checksum_log_ts_, log_ts);
  }
  return ret;
}

uint64_t ObMemtableCtx::calc_checksum(const int64_t trans_version)
{
  if (0 == ATOMIC_LOAD(&checksum_)) {
    set_commit_version(trans_version);
    trans_mgr_.calc_checksum();
    ATOMIC_STORE(&checksum_, batch_checksum_.calc() ?: 1);
  } else if (trans_version != ATOMIC_LOAD(&commit_version_)) {
    TRANS_LOG(ERROR, "MT_CTX: set different trans_version", K(*this), K(trans_version), K(commit_version_));
  }
  return checksum_;
}

uint64_t ObMemtableCtx::calc_checksum2(const int64_t trans_version)
{
  if (0 == checksum_) {
    set_commit_version(trans_version);
    trans_mgr_.calc_checksum2();
    ATOMIC_STORE(&checksum_, batch_checksum_.calc() ?: 1);
  } else if (trans_version != commit_version_) {
    TRANS_LOG(ERROR, "MT_CTX: set different trans_version", K(*this), K(trans_version), K(commit_version_));
  }
  return checksum_;
}

uint64_t ObMemtableCtx::calc_checksum3()
{
  if (0 == checksum_) {
    trans_mgr_.calc_checksum2();
    ATOMIC_STORE(&checksum_, batch_checksum_.calc() ?: 1);
  }
  return checksum_;
}

uint64_t ObMemtableCtx::calc_checksum4()
{
  if (0 == checksum_) {
    trans_mgr_.calc_checksum4();
    ATOMIC_STORE(&checksum_, batch_checksum_.calc() ?: 1);
    ATOMIC_STORE(&checksum_log_ts_, INT64_MAX);
  }
  return checksum_;
}

int ObMemtableCtx::after_link_trans_node(const void* key, ObMvccRow* value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == get_active_mt()) {
    ret = OB_NOT_INIT;
  } else if (!is_commit_version_valid()) {
    // do nothing, statement commit, transaction hasn't committed yet
  } else {
    // do nothing
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

const char* ObMemtableCtx::log_conflict_ctx(const uint32_t descriptor)
{
  const char* ret = NULL;
  ObIMemtableCtx* ctx = NULL;
  ObTransCtx* part_ctx = NULL;
  if (NULL == ctx_map_) {
  } else if (UID_FOR_PURGE == descriptor) {
  } else if (NULL == (ctx = ctx_map_->fetch(descriptor))) {
    TRANS_LOG(WARN, "trans_ctx not exist", K(descriptor));
  } else {
    if (NULL != (part_ctx = ctx->get_trans_ctx())) {
      part_ctx->set_need_print_trace_log();
    }
    ret = to_cstring(*ctx);
  }
  if (NULL != ctx) {
    ctx_map_->revert(descriptor);
    ctx = NULL;
  }
  return ret;
}

int ObMemtableCtx::stmt_data_relocate(const int data_relocate_type, ObMemtable* memstore, bool& relocated)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(stmt_relocate_lock_);

  if (NULL == memstore) {
    TRANS_LOG(ERROR, "memstore is null, unexpected error", KP(memstore), "context", *this);
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_SUCCESS != end_code_) {
    TRANS_LOG(WARN, "tansaction alread end", K(memstore), "context", *this);
    ret = end_code_;
  } else if (NULL == get_active_mt()) {
    // do nothing
  } else {
    const int64_t old_relocate_cnt = relocate_cnt_;
    const int64_t old_truncate_cnt = truncate_cnt_;
    ObTimeGuard tg("mt_stmt_data_relocate", 500 * 1000);
    while (OB_SUCCESS == ret) {
      if (OB_FAIL(trans_mgr_.data_relocate(memstore, data_relocate_type))) {
        if (LEADER_TAKEOVER == data_relocate_type && OB_TRY_LOCK_ROW_CONFLICT == ret) {
          ret = OB_SUCCESS;
          ObTransCond::usleep(10000);
          if (EXECUTE_COUNT_PER_SEC(1)) {
            TRANS_LOG(INFO, "stmt data relocate need retry", "context", *this);
          }
        } else {
          if (EXECUTE_COUNT_PER_SEC(1)) {
            TRANS_LOG(WARN, "trans_mgr relocate data error", K(ret), K(data_relocate_type), "context", *this);
          }
        }
      } else {
        relocated = true;
        data_relocated_ = true;
        memtable_arr_wrap_.dec_active_trx_count_at_active_mt();
        if (NULL != ctx_) {
          ctx_->set_need_print_trace_log();
          if (relocate_cnt_ - old_relocate_cnt > 0) {
            ObTransStatistic::get_instance().add_relocate_row_count(
                ctx_->get_tenant_id(), (relocate_cnt_ - old_relocate_cnt));
            ObTransStatistic::get_instance().add_relocate_total_time(ctx_->get_tenant_id(), tg.get_diff());
          }
        }
        if (trans_mgr_.is_for_replay()) {
          TRANS_LOG(INFO,
              "relocate data success when replay redo log",
              "relocate_used",
              tg.get_diff(),
              "total_relocate_cnt",
              relocate_cnt_,
              "total_truncate_cnt",
              truncate_cnt_,
              "cur_truncate_cnt",
              truncate_cnt_ - old_truncate_cnt,
              "cur_relocate_cnt",
              relocate_cnt_ - old_relocate_cnt,
              "context",
              *this);
        } else {
          TRANS_LOG(INFO,
              "relocate data success when stmt processing",
              "relocate_used",
              tg.get_diff(),
              "total_relocate_cnt",
              relocate_cnt_,
              "total_truncate_cnt",
              truncate_cnt_,
              "cur_truncate_cnt",
              truncate_cnt_ - old_truncate_cnt,
              "cur_relocate_cnt",
              relocate_cnt_ - old_relocate_cnt,
              "context",
              *this);
        }
        break;
      }
    }
  }

  return ret;
}

void ObMemtableCtx::set_need_print_trace_log()
{
  if (NULL != ctx_) {
    ctx_->set_need_print_trace_log();
    TRANS_LOG(INFO, "set print trace log", K(*this));
  }
}

int ObMemtableCtx::audit_partition(const enum ObPartitionAuditOperator op, const int64_t count)
{
  return audit_partition_cache_(op, (int32_t)count);
}

// cache partition audit info in current context
int ObMemtableCtx::audit_partition_cache_(const enum ObPartitionAuditOperator op, const int32_t count)
{
  int ret = OB_SUCCESS;

  if (!GCONF.enable_sql_audit) {
    // do nothing
  } else if (OB_FAIL(partition_audit_info_cache_.update_audit_info(op, count))) {
    TRANS_LOG(WARN, "update audit info", K(ret), K(*ctx_));
  }

  return ret;
}

// flush partition audit info into partition
int ObMemtableCtx::flush_audit_partition_cache_(bool commit)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* partition_mgr = NULL;

  if (!GCONF.enable_sql_audit) {
    // do nothing
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "memtable ctx is NULL", K(ret), KP(ctx_));
  } else if (OB_ISNULL(partition_mgr = ctx_->get_partition_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition mgr is NULL", K(ret), K(*ctx_));
  } else if (OB_FAIL(partition_mgr->audit_partition(partition_audit_info_cache_, commit))) {
    TRANS_LOG(WARN, "partition mgr audit partition error", K(ret), K(*ctx_));
  } else {
    // do nothing
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
    TRANS_LOG(ERROR, "unexpected size", K(size), K(*this));
  } else {
    ATOMIC_FAA(&trans_mem_total_size_, size);
  }
}

void ObMemtableCtx::set_contain_hotspot_row()
{
  int ret = OB_SUCCESS;
  // the memtable pointer read from
  ObMemtable* cur_memtable = NULL;

  // inc active memtable's active transaction count by 1
  if (OB_FAIL(memtable_arr_wrap_.inc_active_trx_count_at_active_mt(cur_memtable))) {
    TRANS_LOG(WARN, "inc_active_trx_count_at_active_mt fail", K(ret));
  } else if (NULL == cur_memtable) {
    TRANS_LOG(WARN, "no active memtable", KP(cur_memtable), K(*this));
  } else {
    if (!cur_memtable->has_hotspot_row() && !data_relocated_) {
      cur_memtable->set_contain_hotspot_row();
      TRANS_LOG(INFO, "set contain hotspot row success", K(*cur_memtable), K(*this));
    }
    // dec reference count by 1
    if (OB_FAIL(cur_memtable->dec_active_trx_count())) {
      TRANS_LOG(WARN, " memtable dec trx count fail", K(ret));
    }
  }
}

int ObMemtableCtx::get_memtable_key_arr(transaction::ObMemtableKeyArray& memtable_key_arr)
{
  int ret = OB_SUCCESS;
  // the memtable pointer read from
  ObMemtable* cur_memtable = NULL;
  if (stmt_relocate_lock_.try_lock()) {
    // inc active memtable's active transaction count by 1
    if (OB_FAIL(memtable_arr_wrap_.inc_active_trx_count_at_active_mt(cur_memtable))) {
      TRANS_LOG(WARN, "inc_active_trx_count_at_active_mt fail", K(ret));
    } else {
      // read row lock data
      if (OB_FAIL(trans_mgr_.get_memtable_key_arr(memtable_key_arr, cur_memtable))) {
        TRANS_LOG(WARN, "get_memtable_key_arr fail", K(ret), K(memtable_key_arr));
      }
      // dec reference count by 1
      if (OB_FAIL(cur_memtable->dec_active_trx_count())) {
        TRANS_LOG(WARN, " memtable dec trx count fail", K(ret));
      }
    }

    // locking on a non-active memstore would generate OB_STATE_NOT_MATCH
    // OB_EAGAIN would be returned when no data is written, or after trans clear
    if (OB_STATE_NOT_MATCH == ret || OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }
    stmt_relocate_lock_.unlock();
  } else {
    // lock failure
    ObMemtableKeyInfo info;
    info.init(1, 1);
    memtable_key_arr.push_back(info);
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

bool ObMemtableCtx::is_can_elr() const
{
  bool bret = false;
  if (OB_NOT_NULL(ATOMIC_LOAD(&ctx_))) {
    bret = ctx_->is_can_elr();
  }
  return bret;
}

bool ObMemtableCtx::is_elr_prepared() const
{
  bool bret = false;
  if (OB_NOT_NULL(ATOMIC_LOAD(&ctx_))) {
    bret = ctx_->is_elr_prepared();
  }
  return bret;
}

bool ObMemtableCtx::has_redo_log() const
{
  bool has_redo = false;
  if (NULL != ATOMIC_LOAD(&ctx_)) {
    has_redo = (static_cast<ObPartTransCtx*>(ctx_)->get_redo_log_no() > 0);
  }

  return has_redo;
}

void ObMemtableCtx::update_max_durable_sql_no(const int64_t sql_no)
{
  if (NULL != ATOMIC_LOAD(&ctx_)) {
    static_cast<ObPartTransCtx*>(ctx_)->update_max_durable_sql_no(sql_no);
  }
}

int ObMemtableCtx::set_memtable_for_cur_log(ObIMemtable* memtable)
{
  int ret = OB_SUCCESS;

  if (NULL == memtable) {
    TRANS_LOG(WARN, "invalid argument", KP(memtable));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != ATOMIC_LOAD(&memtable_for_cur_log_)) {
    TRANS_LOG(INFO, "big row memtable already exists", KP_(memtable_for_cur_log), KP(memtable));
    if (memtable != ATOMIC_LOAD(&memtable_for_cur_log_)) {
      TRANS_LOG(ERROR, "different memtable, unexpected error", KP_(memtable_for_cur_log), KP(memtable));
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    memtable->inc_pending_lob_count();
    ATOMIC_STORE(&memtable_for_cur_log_, memtable);
    ATOMIC_STORE(&lob_start_log_ts_, redo_log_timestamp_);
  }

  return ret;
}

int ObMemtableCtx::set_memtable_for_batch_commit(ObMemtable* memtable)
{
  int ret = OB_SUCCESS;

  if (NULL == memtable) {
    TRANS_LOG(WARN, "invalid argument", KP(memtable));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != memtable_for_batch_commit_) {
    TRANS_LOG(INFO, "batch commit memtable already exists", KP_(memtable_for_batch_commit), KP(memtable));
    if (memtable != memtable_for_batch_commit_) {
      TRANS_LOG(ERROR, "different memtable, unexpected error", KP_(memtable_for_cur_log), KP(memtable));
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    memtable_for_batch_commit_ = memtable;
  }

  return ret;
}

int ObMemtableCtx::set_memtable_for_elr(ObMemtable* memtable)
{
  int ret = OB_SUCCESS;

  if (NULL == memtable) {
    TRANS_LOG(WARN, "invalid argument", KP(memtable));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != memtable_for_elr_) {
    TRANS_LOG(INFO, "batch commit memtable already exists", KP_(memtable_for_elr), KP(memtable));
    if (memtable != memtable_for_elr_) {
      TRANS_LOG(ERROR, "different memtable, unexpected error", KP_(memtable_for_elr), KP(memtable));
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    memtable_for_elr_ = memtable;
  }

  return ret;
}

int ObMemtableCtx::rollback_to(const int32_t sql_no, const bool is_replay, const bool need_write_log,
    const int64_t max_durable_log_ts, bool& has_calc_checksum, uint64_t& checksum, int64_t& checksum_log_ts)
{
  int ret = OB_SUCCESS;

  ObByteLockGuard guard(lock_);
  const ObPartitionKey& pkey = ctx_->get_partition();
  has_calc_checksum = false;

  if (0 > sql_no) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(sql_no));
  } else if (OB_ISNULL(ATOMIC_LOAD(&ctx_))) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "ctx is NULL", K(ret));
  } else if (need_write_log && OB_FAIL(reset_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else if (OB_FAIL(trans_mgr_.rollback_to(
                 pkey, sql_no, is_replay, need_write_log, max_durable_log_ts, has_calc_checksum))) {
    TRANS_LOG(WARN, "rollback to failed", K(ret), K(*this), K(need_write_log), K(max_durable_log_ts), K(is_replay));
  } else {
    if (has_calc_checksum) {
      // To ensure that the checksum calculation only take into account those after checksum_log_ts_,
      // checksum_log_ts_ need to inc by 1. So the data before checksum_log_ts_ need all to be take
      // into account here and calculated in checksum_
      if (is_replay) {
        checksum_log_ts_ = MAX(checksum_log_ts_, max_durable_log_ts);
      } else {
        checksum_log_ts_ = MAX(checksum_log_ts_, max_durable_log_ts + 1);
      }
      checksum = batch_checksum_.calc();
      checksum_log_ts = checksum_log_ts_;
    }

    TRANS_LOG(INFO,
        "memtable handle rollback to successfuly",
        K(sql_no),
        K(is_replay),
        K(need_write_log),
        K(max_durable_log_ts),
        K(*this),
        K(has_calc_checksum),
        K(checksum),
        K(checksum_log_ts));
  }

  return ret;
}

int ObMemtableCtx::truncate_to(const int32_t sql_no)
{
  int ret = OB_SUCCESS;

  ObByteLockGuard guard(lock_);

  if (0 > sql_no) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(sql_no));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "ctx is NULL", K(ret));
  } else if (OB_FAIL(reset_log_generator_())) {
    TRANS_LOG(ERROR, "fail to reset log generator", K(ret));
  } else if (OB_FAIL(trans_mgr_.truncate_to(sql_no))) {
    TRANS_LOG(WARN, "rollback to failed", K(ret), K(*this));
  }

  TRANS_LOG(INFO, "callback list truncate successfuly", K(sql_no), K(*this));

  return ret;
}

bool ObMemtableCtx::is_all_redo_submitted()
{
  ObByteLockGuard guard(lock_);

  ObTransCallbackList& callback_list = trans_mgr_.get_callback_list();
  ObMvccRowCallback* last_callback = (ObMvccRowCallback*)(callback_list.get_guard()->get_prev());
  ObMvccRowCallback* generate_cursor = log_gen_.get_generate_cursor();

  return generate_cursor == last_callback;
}

ObMemtableMutatorIterator* ObMemtableCtx::alloc_memtable_mutator_iter()
{
  int ret = OB_SUCCESS;
  ObMemtableMutatorIterator* mmi = NULL;

  void* buf = ctx_cb_allocator_.alloc(sizeof(ObMemtableMutatorIterator));
  if (OB_ISNULL(buf) || OB_ISNULL((mmi = new (buf) ObMemtableMutatorIterator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "memtable mutator iter alloc fail", K(ret), KP(buf));
  } else if (OB_ISNULL(ATOMIC_LOAD(&ctx_))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(mmi));
  } else if (OB_FAIL(mmi->set_tenant_id(ctx_->get_tenant_id()))) {
    TRANS_LOG(WARN, "mutator iter init error", K(ret));
  } else {
    ATOMIC_STORE(&mutator_iter_, mmi);
  }
  UNUSED(ret);
  return ATOMIC_LOAD(&mutator_iter_);
}

int ObMemtableCtx::get_cur_max_sql_no(int64_t& sql_no)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (OB_FAIL(trans_mgr_.get_cur_max_sql_no(sql_no))) {
    TRANS_LOG(WARN, "get cur max sql no error", K(ret), K(*this));
  }

  return ret;
}

int ObMemtableCtx::remove_callback_for_uncommited_txn(ObMemtable* mt, int64_t& cnt)
{
  int ret = OB_SUCCESS;

  ObByteLockGuard guard(lock_);

  ObTransCallbackList& callback_list = trans_mgr_.get_callback_list();
  cnt = 0;

  if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is NULL", K(mt));
  } else if (OB_FAIL(log_gen_.set_if_necessary((ObMvccRowCallback*)callback_list.get_guard(), mt))) {
    TRANS_LOG(WARN, "fail to reset log gen if necessary", K(ret), K(mt));
  } else if (OB_FAIL(trans_mgr_.remove_callback_for_uncommited_txn(mt, cnt))) {
    TRANS_LOG(WARN, "fail to remove callback for uncommitted txn", K(ret), K(mt));
  }

  return ret;
}

int ObMemtableCtx::mark_frozen_data(
    const ObMemtable* const frozen_memtable, const ObMemtable* const active_memtable, bool& marked, int64_t& cb_cnt)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);
  ObTimeGuard tg(__func__, 10 * 1000);

  ret = trans_mgr_.mark_frozen_data(frozen_memtable, active_memtable, marked, cb_cnt);

  return ret;
}

int ObMemtableCtx::reset_log_generator_()
{
  int ret = OB_SUCCESS;

  ObTransCallbackList& callback_list = trans_mgr_.get_callback_list();
  if (OB_FAIL(log_gen_.set(
          (ObMvccRowCallback*)callback_list.get_guard(), (ObMvccRowCallback*)callback_list.get_guard(), this))) {
    TRANS_LOG(ERROR, "reset log_gen fail", K(ret), K(this), K(callback_list.get_guard()));
  }

  return ret;
}

int ObMemtableCtx::remove_mem_ctx_for_trans_ctx(ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is invalid", K(ret), K(this), K(*mt));
  } else if (OB_FAIL(memtable_arr_wrap_.remove_mem_ctx_for_trans_ctx(mt))) {
    TRANS_LOG(WARN, "remove_mem_ctx_for_trans_ctx fail", K(ret), K(this), K(*mt));
  }

  return ret;
}

int ObMemtableCtx::update_and_get_trans_table_with_minor_freeze(const int64_t log_ts, uint64_t& checksum)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);
  checksum = 0;

  if (0 == log_ts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "log_ts is invalid", K(ret), K(*this), K(log_ts));
  } else if (ATOMIC_LOAD(&checksum_log_ts_) >= log_ts + 1) {
    checksum = batch_checksum_.calc();
    TRANS_LOG(INFO, "already calc checksum for trans", K(log_ts), K(*this), K(checksum), K(checksum_log_ts_));
  } else if (OB_FAIL(trans_mgr_.calc_checksum_before_log_ts(log_ts))) {
    TRANS_LOG(WARN, "calc checksum with_minor_freeze failed", K(ret), K(*this), K(log_ts));
  } else {
    // To ensure that the checksum calculation only take into account those after checksum_log_ts_,
    // checksum_log_ts_ need to inc by 1. So the data before checksum_log_ts_ need all to be take
    // into account here and calculated in checksum_
    checksum = batch_checksum_.calc();
    ATOMIC_STORE(&checksum_log_ts_, log_ts + 1);
    TRANS_LOG(INFO, "calc checksum for trans", K(log_ts), K(*this), K(checksum), K(checksum_log_ts_));
  }

  return ret;
}

int ObMemtableCtx::update_batch_checksum(const uint64_t checksum, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(lock_);

  batch_checksum_.set_base(checksum);
  ATOMIC_STORE(&checksum_log_ts_, log_ts);

  return ret;
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

void ObMemtableCtx::dec_pending_batch_commit_count()
{
  if (NULL != memtable_for_batch_commit_) {
    memtable_for_batch_commit_->dec_pending_batch_commit_count();
    memtable_for_batch_commit_ = NULL;
  }
}

bool ObMemtableCtx::is_sql_need_be_submit(const int64_t sql_no)
{
  bool ret = false;
  ObMvccRowCallback* generate_cursor = NULL;
  if (OB_ISNULL(generate_cursor = log_gen_.get_generate_cursor())) {
    if (sql_no > generate_cursor->get_sql_no()) {
      ret = true;
    }
  }

  return ret;
}

void ObMemtableCtx::dec_pending_elr_count()
{
  if (NULL != memtable_for_elr_) {
    memtable_for_elr_->dec_pending_elr_count();
    memtable_for_elr_ = NULL;
  }
}

}  // namespace memtable
}  // namespace oceanbase

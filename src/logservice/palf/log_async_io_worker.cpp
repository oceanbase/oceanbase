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
#define USING_LOG_PREFIX PALF

#include "log_async_io_worker.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/thread/thread_mgr.h"
#include "lib/thread/threads.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "log_async_palf_ctx.h"
#include "log_io_task.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{

AsyncPalfIOCtxEntry::AsyncPalfIOCtxEntry()
  : ctx_(NULL),
    unregistering_(false),
    active_ref_(0)
{
}

AsyncPalfIOCtxEntry::~AsyncPalfIOCtxEntry()
{
  reset();
}

int AsyncPalfIOCtxEntry::init(IAsyncPalfIOCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid async PALF ctx entry argument", KR(ret), KP(ctx));
  } else if (is_valid()) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "async PALF ctx entry already initialized", KR(ret), KPC(this));
  } else {
    ctx_ = ctx;
    unregistering_ = false;
    ATOMIC_STORE(&active_ref_, 0);
  }
  return ret;
}

void AsyncPalfIOCtxEntry::reset()
{
  const int64_t active_ref = get_active_ref();
  if (0 != active_ref) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                 "reset async PALF ctx entry with active references",
                 K(active_ref), KPC(this));
  }
  ctx_ = NULL;
  unregistering_ = false;
  ATOMIC_STORE(&active_ref_, 0);
}

bool AsyncPalfIOCtxEntry::is_valid() const
{
  return OB_NOT_NULL(ctx_);
}

void AsyncPalfIOCtxEntry::acquire_ref()
{
  ATOMIC_INC(&active_ref_);
}

void AsyncPalfIOCtxEntry::release_ref()
{
  const int64_t active_ref = ATOMIC_AAF(&active_ref_, -1);
  if (active_ref < 0) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                 "async PALF ctx entry reference underflow",
                 K(active_ref), KPC(this));
  }
}

int64_t AsyncPalfIOCtxEntry::get_active_ref() const
{
  return ATOMIC_LOAD(&active_ref_);
}

AsyncPalfIOCtxEntryGuard::AsyncPalfIOCtxEntryGuard()
  : entry_(NULL)
{
}

AsyncPalfIOCtxEntryGuard::~AsyncPalfIOCtxEntryGuard()
{
  reset();
}

int AsyncPalfIOCtxEntryGuard::set_entry(AsyncPalfIOCtxEntry *entry)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async PALF ctx entry", KR(ret), KPC(entry));
  } else if (!entry->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "hold uninitialized async PALF ctx entry",
             KR(ret), KPC(entry));
  } else {
    entry->acquire_ref();
    entry_ = entry;
  }
  return ret;
}

void AsyncPalfIOCtxEntryGuard::reset()
{
  if (OB_NOT_NULL(entry_)) {
    entry_->release_ref();
    entry_ = NULL;
  }
}

bool AsyncPalfIOCtxEntryGuard::is_valid() const
{
  return OB_NOT_NULL(entry_) && entry_->is_valid();
}

IAsyncPalfIOCtx *AsyncPalfIOCtxEntryGuard::get_ctx() const
{
  return is_valid() ? entry_->get_ctx() : NULL;
}

IAsyncPalfIOCtx *AsyncPalfIOCtxEntryGuard::operator->() const
{
  return get_ctx();
}

class LogAsyncIOWorker::EntryGuardSet
{
public:
  EntryGuardSet();
  ~EntryGuardSet();
  void reset();
  int hold_and_push(AsyncPalfIOCtxEntry *entry);
  int64_t count() const;
  int drive_write_at(const int64_t idx, int64_t &next_drive_interval_us);
  bool is_drained_at(const int64_t idx) const;
  int64_t get_oldest_pending_io_start_ts_at(const int64_t idx) const;
  int64_t get_throttle_next_admit_ts_at(const int64_t idx) const;

private:
  IAsyncPalfIOCtx *at(const int64_t idx) const;
  common::ObSEArray<AsyncPalfIOCtxEntry *, 32> entries_;

  DISALLOW_COPY_AND_ASSIGN(EntryGuardSet);
};

LogAsyncIOWorker::EntryGuardSet::EntryGuardSet()
  : entries_()
{
}

LogAsyncIOWorker::EntryGuardSet::~EntryGuardSet()
{
  reset();
}

void LogAsyncIOWorker::EntryGuardSet::reset()
{
  for (int64_t i = 0; i < entries_.count(); i++) {
    AsyncPalfIOCtxEntry *entry = entries_.at(i);
    if (OB_NOT_NULL(entry)) {
      entry->release_ref();
    }
  }
  entries_.reset();
}

int LogAsyncIOWorker::EntryGuardSet::hold_and_push(AsyncPalfIOCtxEntry *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry) || !entry->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    entry->acquire_ref();
    if (OB_FAIL(entries_.push_back(entry))) {
      entry->release_ref();
    }
  }
  return ret;
}

int64_t LogAsyncIOWorker::EntryGuardSet::count() const
{
  return entries_.count();
}

IAsyncPalfIOCtx *LogAsyncIOWorker::EntryGuardSet::at(const int64_t idx) const
{
  IAsyncPalfIOCtx *ctx = NULL;
  AsyncPalfIOCtxEntry *entry = NULL;
  if (idx < 0 || idx >= entries_.count()) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid async ctx snapshot index",
                 K(idx), "entry_count", entries_.count());
  } else if (OB_ISNULL(entry = entries_.at(idx)) || !entry->is_valid()) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid async ctx snapshot entry",
                 K(idx), KP(entry));
  } else if (OB_ISNULL(ctx = entry->get_ctx())) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "null ctx in async ctx snapshot entry",
                 K(idx), KPC(entry));
  }
  return ctx;
}

int LogAsyncIOWorker::EntryGuardSet::drive_write_at(
    const int64_t idx,
    int64_t &next_drive_interval_us)
{
  int ret = OB_SUCCESS;
  IAsyncPalfIOCtx *ctx = at(idx);
  next_drive_interval_us = INT64_MAX;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = ctx->drive_write(next_drive_interval_us);
  }
  return ret;
}

bool LogAsyncIOWorker::EntryGuardSet::is_drained_at(const int64_t idx) const
{
  bool is_drained = true;
  IAsyncPalfIOCtx *ctx = at(idx);
  if (OB_ISNULL(ctx)) {
    is_drained = false;
  } else {
    is_drained = ctx->is_drained();
  }
  return is_drained;
}

int64_t LogAsyncIOWorker::EntryGuardSet::get_oldest_pending_io_start_ts_at(
    const int64_t idx) const
{
  int64_t oldest_ts = OB_INVALID_TIMESTAMP;
  IAsyncPalfIOCtx *ctx = at(idx);
  if (OB_NOT_NULL(ctx)) {
    oldest_ts = ctx->get_oldest_pending_io_start_ts();
  }
  return oldest_ts;
}

int64_t LogAsyncIOWorker::EntryGuardSet::get_throttle_next_admit_ts_at(
    const int64_t idx) const
{
  int64_t next_admit_ts = 0;
  IAsyncPalfIOCtx *ctx = at(idx);
  if (OB_NOT_NULL(ctx)) {
    next_admit_ts = ctx->get_throttle_next_admit_ts();
  }
  return next_admit_ts;
}

LogAsyncIOWorker::AsyncMarkTask::AsyncMarkTask()
  : LogIOTask(INVALID_PALF_ID, -1)
{}

LogAsyncIOWorker::AsyncMarkTask::~AsyncMarkTask()
{}

int LogAsyncIOWorker::AsyncMarkTask::do_task_(int /*tg_id*/,
                                               IPalfHandleImplGuard &/*guard*/)
{
  return OB_SUCCESS;
}

int LogAsyncIOWorker::AsyncMarkTask::after_consume_(IPalfHandleImplGuard &/*guard*/)
{
  return OB_SUCCESS;
}

LogIOTaskType LogAsyncIOWorker::AsyncMarkTask::get_io_task_type_() const
{
  return LogIOTaskType::ASYNC_MARK_TYPE;
}

void LogAsyncIOWorker::AsyncMarkTask::free_this_(IPalfEnvImpl */*palf_env_impl*/)
{
  PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cannot free reusable async mark task");
}

int64_t LogAsyncIOWorker::AsyncMarkTask::get_io_size_() const
{
  return 0;
}

bool LogAsyncIOWorker::AsyncMarkTask::need_purge_throttling_() const
{
  return false;
}

// Hold one entry reference while the map bucket is locked, then expose it
// through the caller's guard for lock-free ctx access.
class LogAsyncIOWorker::GetEntryOp
{
public:
  GetEntryOp(const bool allow_unregistering,
             AsyncPalfIOCtxEntryGuard &guard)
    : allow_unregistering_(allow_unregistering),
      guard_(guard),
      op_ret_(OB_SUCCESS)
  {}

  void operator()(
      common::hash::HashMapPair<int64_t, AsyncPalfIOCtxEntry *> &entry_pair)
  {
    AsyncPalfIOCtxEntry *entry = entry_pair.second;
    if (OB_ISNULL(entry) || !entry->is_valid()) {
      op_ret_ = OB_ERR_UNEXPECTED;
    } else if (!allow_unregistering_ && entry->is_unregistering()) {
      op_ret_ = OB_STATE_NOT_MATCH;
    } else {
      op_ret_ = guard_.set_entry(entry);
    }
  }

  int get_ret() const { return op_ret_; }

private:
  bool allow_unregistering_;
  AsyncPalfIOCtxEntryGuard &guard_;
  int op_ret_;
};

// Mark one entry while its map bucket is write-locked. A concurrent submit
// either acquires its entry reference before this mark or observes
// unregistering_ and rejects the task; it cannot observe an intermediate state.
class LogAsyncIOWorker::MarkUnregisteringOp
{
public:
  MarkUnregisteringOp()
    : op_ret_(OB_SUCCESS)
  {}

  void operator()(
      common::hash::HashMapPair<int64_t, AsyncPalfIOCtxEntry *> &entry_pair)
  {
    AsyncPalfIOCtxEntry *entry = entry_pair.second;
    if (OB_ISNULL(entry) || !entry->is_valid()) {
      op_ret_ = OB_ERR_UNEXPECTED;
    } else if (entry->is_unregistering()) {
      op_ret_ = OB_STATE_NOT_MATCH;
    } else {
      entry->set_unregistering();
    }
  }

  int get_ret() const { return op_ret_; }

private:
  int op_ret_;
};

// Collect a map snapshot and hold every entry until EntryGuardSet is reset.
class LogAsyncIOWorker::SnapshotEntryOp
{
public:
  explicit SnapshotEntryOp(EntryGuardSet &entries)
    : entries_(entries)
  {}

  int operator()(
      common::hash::HashMapPair<int64_t, AsyncPalfIOCtxEntry *> &entry_pair)
  {
    int ret = OB_SUCCESS;
    AsyncPalfIOCtxEntry *entry = entry_pair.second;
    if (OB_ISNULL(entry) || !entry->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = entries_.hold_and_push(entry);
    }
    return ret;
  }

private:
  EntryGuardSet &entries_;
};

// 在 bucket lock 内重新核对同一个 entry、注销标记以及 entry/ctx 引用，避免
// 锁外检查通过后状态又发生变化。只有最终检查仍满足条件时才允许删除。
class LogAsyncIOWorker::EraseDrainedEntryOp
{
public:
  explicit EraseDrainedEntryOp(AsyncPalfIOCtxEntry *expected_entry)
    : expected_entry_(expected_entry),
      op_ret_(OB_SUCCESS)
  {}

  bool operator()(
      common::hash::HashMapPair<int64_t, AsyncPalfIOCtxEntry *> &entry_pair)
  {
    bool can_erase = false;
    AsyncPalfIOCtxEntry *entry = entry_pair.second;
    if (OB_ISNULL(entry)
        || entry != expected_entry_
        || !entry->is_valid()
        || !entry->is_unregistering()) {
      op_ret_ = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(entry->get_ctx())) {
      op_ret_ = OB_ERR_UNEXPECTED;
    } else {
      can_erase = (0 == entry->get_active_ref()
                   && 0 == entry->get_ctx()->get_active_ref());
    }
    return can_erase;
  }

  int get_ret() const { return op_ret_; }

private:
  AsyncPalfIOCtxEntry *expected_entry_;
  int op_ret_;
};

LogAsyncIOWorkerDiagnostics::LogAsyncIOWorkerDiagnostics()
  : submitted_task_count_(0),
    dropped_submit_count_(0),
    handled_task_count_(0),
    drive_wake_count_(0),
    coalesced_drive_wake_count_(0),
    next_drive_deadline_(0)
{
}

void LogAsyncIOWorkerDiagnostics::reset()
{
  ATOMIC_STORE(&submitted_task_count_, 0);
  ATOMIC_STORE(&dropped_submit_count_, 0);
  ATOMIC_STORE(&handled_task_count_, 0);
  ATOMIC_STORE(&drive_wake_count_, 0);
  ATOMIC_STORE(&coalesced_drive_wake_count_, 0);
  next_drive_deadline_ = 0;
}

void LogAsyncIOWorkerDiagnostics::set_next_drive_deadline(const int64_t ts)
{
  next_drive_deadline_ = ts;
}

void LogAsyncIOWorkerDiagnostics::inc_submitted_task_count()
{
  ATOMIC_INC(&submitted_task_count_);
}

void LogAsyncIOWorkerDiagnostics::inc_dropped_submit_count()
{
  ATOMIC_INC(&dropped_submit_count_);
}

void LogAsyncIOWorkerDiagnostics::inc_handled_task_count()
{
  ATOMIC_INC(&handled_task_count_);
}

void LogAsyncIOWorkerDiagnostics::inc_drive_wake_count()
{
  ATOMIC_INC(&drive_wake_count_);
}

void LogAsyncIOWorkerDiagnostics::inc_coalesced_drive_wake_count()
{
  ATOMIC_INC(&coalesced_drive_wake_count_);
}

void LogAsyncIOWorkerDiagnostics::snapshot_from(const LogAsyncIOWorkerDiagnostics &other)
{
  submitted_task_count_ = other.get_submitted_task_count();
  dropped_submit_count_ = other.get_dropped_submit_count();
  handled_task_count_ = other.get_handled_task_count();
  drive_wake_count_ = other.get_drive_wake_count();
  coalesced_drive_wake_count_ = other.get_coalesced_drive_wake_count();
  next_drive_deadline_ = other.get_next_drive_deadline();
}

void LogAsyncIOWorkerDiagnostics::subtract(const LogAsyncIOWorkerDiagnostics &base,
                                           LogAsyncIOWorkerDiagnostics &delta) const
{
  delta.submitted_task_count_ = get_submitted_task_count() - base.get_submitted_task_count();
  delta.dropped_submit_count_ = get_dropped_submit_count() - base.get_dropped_submit_count();
  delta.handled_task_count_ = get_handled_task_count() - base.get_handled_task_count();
  delta.drive_wake_count_ = get_drive_wake_count() - base.get_drive_wake_count();
  delta.coalesced_drive_wake_count_ =
      get_coalesced_drive_wake_count() - base.get_coalesced_drive_wake_count();
  delta.next_drive_deadline_ = next_drive_deadline_;
}

bool LogAsyncIOWorkerDiagnostics::has_task_activity_since(
    const LogAsyncIOWorkerDiagnostics &base) const
{
  return get_submitted_task_count() != base.get_submitted_task_count()
      || get_dropped_submit_count() != base.get_dropped_submit_count()
      || get_handled_task_count() != base.get_handled_task_count();
}

int64_t LogAsyncIOWorkerDiagnostics::get_submitted_task_count() const
{
  return ATOMIC_LOAD(&submitted_task_count_);
}

int64_t LogAsyncIOWorkerDiagnostics::get_dropped_submit_count() const
{
  return ATOMIC_LOAD(&dropped_submit_count_);
}

int64_t LogAsyncIOWorkerDiagnostics::get_handled_task_count() const
{
  return ATOMIC_LOAD(&handled_task_count_);
}

int64_t LogAsyncIOWorkerDiagnostics::get_drive_wake_count() const
{
  return ATOMIC_LOAD(&drive_wake_count_);
}

int64_t LogAsyncIOWorkerDiagnostics::get_coalesced_drive_wake_count() const
{
  return ATOMIC_LOAD(&coalesced_drive_wake_count_);
}

int64_t LogAsyncIOWorkerDiagnostics::get_next_drive_deadline() const
{
  return next_drive_deadline_;
}

int64_t LogAsyncIOWorker::get_oldest_pending_io_start_ts() const
{
  int ret = OB_SUCCESS;
  int64_t oldest_ts = OB_INVALID_TIMESTAMP;
  EntryGuardSet snapshot;
  {
    // Entry references keep every collected ctx alive after the map lock ends.
    ObQSyncLockReadGuard guard(lifecycle_lock_);
    if (is_inited_ && !is_destroying_) {
      ret = snapshot_entries_(snapshot);
    }
  }
  if (OB_SUCCESS != ret) {
    PALF_LOG(WARN, "snapshot ctx failed when checking oldest pending async io", KR(ret));
  } else {
    for (int64_t i = 0; i < snapshot.count(); ++i) {
      const int64_t ctx_ts = snapshot.get_oldest_pending_io_start_ts_at(i);
      if (ctx_ts > 0 && (OB_INVALID_TIMESTAMP == oldest_ts || ctx_ts < oldest_ts)) {
        oldest_ts = ctx_ts;
      }
    }
  }
  return oldest_ts;
}

LogAsyncIOWorker::LogAsyncIOWorker()
  : lifecycle_lock_(common::ObLatchIds::OB_LOG_IO_WORKER_LOCK),
    is_inited_(false),
    is_running_(false),
    is_destroying_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    palf_env_impl_(NULL),
    input_queue_(),
    drive_mark_task_(),
    ctx_map_(),
    dropped_task_count_(0),
    dispatched_task_count_(0),
    pending_dispatch_task_(NULL),
    drive_pending_(0),
    diagnostics_(),
    input_queue_size_stat_("[PALF STAT ASYNC IO WORKER QUEUE SIZE]", PALF_STAT_PRINT_INTERVAL_US),
    wait_cost_stat_("[PALF STAT ASYNC IO TASK IN QUEUE TIME]", PALF_STAT_PRINT_INTERVAL_US),
    dispatch_cost_stat_("[PALF STAT ASYNC IO TASK DISPATCH COST]", PALF_STAT_PRINT_INTERVAL_US),
    drive_cost_stat_("[PALF STAT ASYNC IO WORKER DRIVE COST]", PALF_STAT_PRINT_INTERVAL_US),
    print_log_interval_(OB_INVALID_TIMESTAMP),
    event_summary_interval_(OB_INVALID_TIMESTAMP),
    last_summary_stat_(),
    last_event_stat_()
{
}

LogAsyncIOWorker::~LogAsyncIOWorker()
{
  destroy();
}

int LogAsyncIOWorker::init(const int64_t tenant_id,
                           IPalfEnvImpl *palf_env_impl,
                           const int64_t input_queue_capacity)
{
  ObQSyncLockWriteGuard guard(lifecycle_lock_);
  return init_(tenant_id, palf_env_impl, input_queue_capacity);
}

int LogAsyncIOWorker::init_(const int64_t tenant_id,
                            IPalfEnvImpl *palf_env_impl,
                            const int64_t input_queue_capacity)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "LogAsyncIOWorker already inited", KR(ret));
  } else if (OB_UNLIKELY(tenant_id <= 0)
             || input_queue_capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(tenant_id),
             K(input_queue_capacity));
  } else if (OB_FAIL(input_queue_.init(input_queue_capacity,
                                       "AsyncWInLQ", tenant_id))) {
    PALF_LOG(WARN, "input queue init failed", KR(ret), K(input_queue_capacity));
  } else if (OB_FAIL(ctx_map_.create(CTX_MAP_BUCKET_NUM,
                                     ObMemAttr(tenant_id, "AsyncLogCtx")))) {
    PALF_LOG(WARN, "async ctx map create failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(this->set_thread_count(THREAD_COUNT))) {
    PALF_LOG(WARN, "set_thread_count failed", KR(ret));
  } else {
    // 绑定当前租户的运行上下文，确保线程启动后进入正确的 MTL；如果缺少
    // run_wrapper_，AsyncWriteLoop 会落到 T0，并在首次访问 MTL 时返回 -4016。
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    tenant_id_ = tenant_id;
    palf_env_impl_ = palf_env_impl;
    diagnostics_.reset();
    dropped_task_count_ = 0;
    dispatched_task_count_ = 0;
    ATOMIC_STORE(&pending_dispatch_task_, static_cast<LogIOTask *>(NULL));
    ATOMIC_STORE(&drive_pending_, 0);
    print_log_interval_ = OB_INVALID_TIMESTAMP;
    event_summary_interval_ = OB_INVALID_TIMESTAMP;
    last_summary_stat_.reset();
    last_event_stat_.reset();
    is_destroying_ = false;
    is_inited_ = true;
    add_worker_server_event_("worker_init", OB_SUCCESS, INVALID_PALF_ID);
    PALF_LOG(INFO, "LogAsyncIOWorker init success",
             K(tenant_id), K(input_queue_capacity));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    ctx_map_.destroy();
    (void) input_queue_.destroy();
  }
  return ret;
}

void LogAsyncIOWorker::destroy()
{
  // Stop admission under the lifecycle lock, then join without holding it.
  // The owning wrapper unregisters every PALF ctx before worker destruction.
  bool need_destroy = false;
  {
    ObQSyncLockWriteGuard guard(lifecycle_lock_);
    if (is_inited_ && !is_destroying_) {
      is_destroying_ = true;
      stop_();
      need_destroy = true;
    }
  }
  if (need_destroy) {
    wait();
    ObQSyncLockWriteGuard guard(lifecycle_lock_);
    destroy_();
  }
}

void LogAsyncIOWorker::destroy_()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    const int64_t tenant_id = tenant_id_;
    const int64_t ctx_count = get_ctx_count_();
    if (0 != ctx_count) {
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                   "async ctx is not empty before worker destroy", K(ctx_count),
                   K_(dropped_task_count), K_(dispatched_task_count));
    }
    add_worker_server_event_("worker_destroy", OB_SUCCESS, INVALID_PALF_ID);
    input_queue_.destroy();
    tmp_ret = ctx_map_.destroy();
    if (OB_SUCCESS != tmp_ret) {
      PALF_LOG_RET(WARN, tmp_ret, "destroy async ctx map failed", K(ctx_count));
    }
    palf_env_impl_ = NULL;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_destroying_ = false;
    is_inited_ = false;
    PALF_LOG(INFO, "LogAsyncIOWorker destroy success", K(tenant_id), K(ctx_count),
             K_(dropped_task_count), K_(dispatched_task_count), K_(diagnostics));
  }
}

bool LogAsyncIOWorker::is_valid() const
{
  ObQSyncLockReadGuard guard(lifecycle_lock_);
  return is_valid_();
}

bool LogAsyncIOWorker::is_valid_() const
{
  return is_inited_ && !is_destroying_;
}

int LogAsyncIOWorker::start()
{
  ObQSyncLockWriteGuard guard(lifecycle_lock_);
  return start_();
}

int LogAsyncIOWorker::start_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogAsyncIOWorker is not initialized", KR(ret));
  } else if (is_destroying_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "worker is being destroyed", KR(ret));
  } else if (is_running_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "worker already running", KR(ret));
  } else {
    is_running_ = true;
    if (OB_FAIL(LogIOWorkerBase::start())) {
      is_running_ = false;
      PALF_LOG(WARN, "ObThreadPool start failed", KR(ret));
    } else {
      add_worker_server_event_("worker_start", OB_SUCCESS, INVALID_PALF_ID);
      PALF_LOG(INFO, "LogAsyncIOWorker start success");
    }
  }
  return ret;
}

void LogAsyncIOWorker::stop()
{
  ObQSyncLockWriteGuard guard(lifecycle_lock_);
  stop_();
}

void LogAsyncIOWorker::stop_()
{
  if (is_inited_ && is_running_) {
    is_running_ = false;
    LogIOWorkerBase::stop();
    add_worker_server_event_("worker_stop", OB_SUCCESS, INVALID_PALF_ID);
  }
}

void LogAsyncIOWorker::wait()
{
  // Observer graceful shutdown unregisters every PALF ctx in PalfEnvImpl::wait()
  // before it waits for workers. Keep the worker alive until ctx_map_ is empty
  // so accepted tasks, entry guards, and AIO callback pins cannot outlive it.
  int64_t last_warn_ts = 0;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t ctx_count = 0;
  bool is_inited = false;
  do {
    {
      ObQSyncLockReadGuard guard(lifecycle_lock_);
      is_inited = is_inited_;
      tenant_id = tenant_id_;
      ctx_count = get_ctx_count_();
    }
    if (is_inited && 0 != ctx_count) {
      if (palf_reach_time_interval(5_s, last_warn_ts)) {
        PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                     "wait for all async ctx to unregister before worker join",
                     K(ctx_count), K(tenant_id));
      }
      ob_usleep(UNREGISTER_WAIT_INTERVAL_US);
    }
  } while (is_inited && 0 != ctx_count);
  LogIOWorkerBase::wait();
}

int LogAsyncIOWorker::create_ctx_entry_(
    const int64_t palf_id,
    const int cb_tg_id,
    IPalfEnvImpl *palf_env_impl,
    const AsyncThrottleContext &throttle_ctx,
    AsyncPalfIOCtxEntry *&entry)
{
  int ret = OB_SUCCESS;
  AsyncPalfIOCtx *ctx = NULL;
  const uint64_t tenant_id = MTL_ID();
  entry = NULL;
  if (palf_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(palf_id));
  } else if (OB_ISNULL(ctx = OB_NEW(AsyncPalfIOCtx,
                                    ObMemAttr(tenant_id, "PalfAsyncCtx")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloc AsyncPalfIOCtx failed", KR(ret), K(palf_id));
  } else if (OB_FAIL(ctx->init(palf_id, cb_tg_id, palf_env_impl,
                               this, throttle_ctx))) {
    PALF_LOG(WARN, "AsyncPalfIOCtx init failed", KR(ret), K(palf_id));
  } else if (OB_ISNULL(entry = OB_NEW(AsyncPalfIOCtxEntry,
                                      ObMemAttr(tenant_id, "PalfAsyncEntry")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloc async PALF ctx entry failed", KR(ret), K(palf_id));
  } else if (OB_FAIL(entry->init(ctx))) {
    PALF_LOG(WARN, "init async PALF ctx entry failed", KR(ret), K(palf_id));
  } else {
    // The initialized entry owns ctx from this point.
    ctx = NULL;
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(entry)) {
      entry->reset();
      OB_DELETE(AsyncPalfIOCtxEntry, "PalfAsyncEntry", entry);
      entry = NULL;
    }
    if (OB_NOT_NULL(ctx)) {
      ctx->free_this();
    }
    ctx = NULL;
  }
  return ret;
}

int LogAsyncIOWorker::get_entry_guard_(const int64_t palf_id,
                                       const bool allow_unregistering,
                                       AsyncPalfIOCtxEntryGuard &guard)
{
  int ret = OB_SUCCESS;
  guard.reset();
  GetEntryOp op(allow_unregistering, guard);
  if (OB_FAIL(ctx_map_.read_atomic(palf_id, op))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(TRACE, "async ctx entry does not exist", KR(ret), K(palf_id));
      }
    } else {
      PALF_LOG(ERROR, "ctx map get failed", KR(ret), K(palf_id));
    }
  } else if (OB_FAIL(op.get_ret())) {
    if (OB_STATE_NOT_MATCH == ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(TRACE, "async ctx entry rejects new task", KR(ret), K(palf_id));
      }
    } else {
      PALF_LOG(ERROR, "hold async ctx entry failed", KR(ret), K(palf_id),
               K(allow_unregistering));
    }
  } else if (!guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "holding async ctx entry returned invalid guard",
             KR(ret), K(palf_id));
  }
  return ret;
}

int LogAsyncIOWorker::dispatch_task_(LogIOTask *task)
{
  int ret = OB_SUCCESS;
  int64_t palf_id = INVALID_PALF_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "dispatch accepted task on uninitialized async worker",
             KR(ret), KP(task));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "dispatch null accepted async task", KR(ret));
  } else {
    palf_id = task->get_palf_id();
    AsyncPalfIOCtxEntryGuard entry_guard;
    // This task was accepted before unregister started and must still enter
    // the ctx so unregister can drain every accepted task.
    if (OB_FAIL(get_entry_guard_(palf_id, true /* allow_unregistering */,
                                 entry_guard))) {
      ++dropped_task_count_;
      PALF_LOG(ERROR, "accepted async task lost its ctx entry",
               KR(ret), K(palf_id), KPC(task));
    } else if (OB_FAIL(entry_guard->enqueue_task(task))) {
      // Keep task ownership and its reserved slot until a later retry succeeds.
      ++dropped_task_count_;
      if (OB_EAGAIN == ret || OB_SIZE_OVERFLOW == ret
          || OB_ALLOCATE_MEMORY_FAILED == ret) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(TRACE, "ctx enqueue task failed, keep task for retry",
                   KR(ret), K(palf_id), KPC(task));
        }
      } else {
        PALF_LOG(ERROR, "ctx rejected accepted async task unexpectedly",
                 KR(ret), K(palf_id), KPC(task));
      }
    } else {
      ++dispatched_task_count_;
    }
  }
  return ret;
}

int LogAsyncIOWorker::snapshot_entries_(EntryGuardSet &out) const
{
  int ret = OB_SUCCESS;
  out.reset();
  SnapshotEntryOp op(out);
  ret = ctx_map_.foreach_refactored(op);
  return ret;
}

int LogAsyncIOWorker::drive_write_all_(int64_t &next_drive_interval_us)
{
  int ret = OB_SUCCESS;
  int64_t next_throttle_deadline = 0;
  next_drive_interval_us = INT64_MAX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "drive uninitialized async worker", KR(ret));
  } else {
    EntryGuardSet snapshot;
    if (OB_FAIL(snapshot_entries_(snapshot))) {
      PALF_LOG(ERROR, "snapshot ctx failed", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < snapshot.count(); ++i) {
      int64_t ctx_next_drive_interval_us = INT64_MAX;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(snapshot.drive_write_at(i, ctx_next_drive_interval_us))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(TRACE, "drive async ctx failed", KR(tmp_ret), K(i));
        }
      } else if (ctx_next_drive_interval_us < 0) {
        PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                     "async ctx returned invalid next drive interval",
                     K(i), K(ctx_next_drive_interval_us));
      } else if (ctx_next_drive_interval_us < next_drive_interval_us) {
        next_drive_interval_us = ctx_next_drive_interval_us;
      }
      const int64_t throttle_deadline = snapshot.get_throttle_next_admit_ts_at(i);
      if (throttle_deadline > 0
          && (0 == next_throttle_deadline || throttle_deadline < next_throttle_deadline)) {
        next_throttle_deadline = throttle_deadline;
      }
    }
    if (next_throttle_deadline > 0) {
      const int64_t now = ObTimeUtility::fast_current_time();
      const int64_t throttle_interval_us = next_throttle_deadline <= now
          ? 0 : next_throttle_deadline - now;
      next_drive_interval_us = MIN(next_drive_interval_us, throttle_interval_us);
    }
  }
  return ret;
}

bool LogAsyncIOWorker::is_drained_() const
{
  bool is_drained = true;
  if (is_inited_) {
    int ret = OB_SUCCESS;
    EntryGuardSet snapshot;
    if (OB_FAIL(snapshot_entries_(snapshot))) {
      is_drained = false;
      PALF_LOG(WARN, "snapshot ctx failed when checking worker drain state", KR(ret));
    } else {
      for (int64_t i = 0; i < snapshot.count() && is_drained; ++i) {
        is_drained = snapshot.is_drained_at(i);
      }
    }
  }
  return is_drained;
}

int LogAsyncIOWorker::try_erase_ctx_(const int64_t palf_id,
                                     AsyncPalfIOCtxEntry *&entry_to_destroy)
{
  // 第一阶段在锁外读取 drain/ref 状态；满足条件后释放临时 guard，再由
  // erase_if 在 bucket lock 内对同一个 entry 做最终检查并删除。
  int ret = OB_SUCCESS;
  bool can_try_erase = false;
  AsyncPalfIOCtxEntryGuard entry_guard;
  AsyncPalfIOCtxEntry *expected_entry = NULL;
  entry_to_destroy = NULL;
  // Unregister must inspect the marked entry until its ctx and references drain.
  if (OB_FAIL(get_entry_guard_(palf_id, true /* allow_unregistering */,
                               entry_guard))) {
    PALF_LOG(WARN, "hold async ctx entry while checking drain state failed",
             KR(ret), K(palf_id));
  } else {
    expected_entry = entry_guard.get_entry();
    IAsyncPalfIOCtx *ctx = entry_guard.get_ctx();
    const bool is_drained = OB_NOT_NULL(ctx) && ctx->is_drained();
    const int64_t entry_active_ref = expected_entry->get_active_ref();
    const int64_t ctx_active_ref = OB_ISNULL(ctx) ? 0 : ctx->get_active_ref();
    if (OB_ISNULL(ctx) || entry_active_ref <= 0 || ctx_active_ref < 0) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid guarded async ctx entry during unregister",
               KR(ret), K(palf_id), KP(ctx), K(entry_active_ref),
               K(ctx_active_ref), KPC(expected_entry));
    } else if (!is_drained || 1 != entry_active_ref || 0 != ctx_active_ref) {
      if (REACH_THREAD_TIME_INTERVAL(5_s)) {
        PALF_LOG(WARN, "wait async ctx drain before unregister",
                 K(palf_id), K(is_drained),
                 "inflight_count", ctx->get_inflight_count(),
                 "entry_active_ref", entry_active_ref - 1,
                 K(ctx_active_ref));
      }
    } else {
      can_try_erase = true;
    }
  }
  if (OB_SUCC(ret) && can_try_erase) {
    AsyncPalfIOCtxEntry *erased_entry = NULL;
    bool is_erased = false;
    entry_guard.reset();
    EraseDrainedEntryOp op(expected_entry);
    if (OB_FAIL(ctx_map_.erase_if(palf_id, op, is_erased, &erased_entry))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      PALF_LOG(WARN, "ctx entry disappeared during unregister",
               KR(ret), K(palf_id));
    } else if (OB_FAIL(op.get_ret())) {
      PALF_LOG(ERROR, "final async ctx entry erase check failed",
               KR(ret), K(palf_id), KPC(expected_entry));
    } else if (!is_erased) {
      PALF_LOG(TRACE, "async ctx gained active reference before erase, retry",
               K(palf_id));
    } else if (erased_entry != expected_entry) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "unexpected async ctx entry erase result",
               KR(ret), K(palf_id), KPC(erased_entry), KPC(expected_entry));
    } else {
      entry_to_destroy = erased_entry;
      PALF_LOG(INFO, "unregister async ctx done", K(palf_id), KPC(erased_entry));
    }
  }
  return ret;
}

void LogAsyncIOWorker::drain_and_erase_ctx_(const int64_t palf_id,
                                            AsyncPalfIOCtxEntry *&entry_to_destroy)
{
  bool drain_finished = false;
  entry_to_destroy = NULL;
  while (!drain_finished) {
    const int tmp_ret = try_erase_ctx_(palf_id, entry_to_destroy);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      drain_finished = true;
    } else if (OB_SUCCESS != tmp_ret) {
      if (REACH_THREAD_TIME_INTERVAL(5_s)) {
        PALF_LOG_RET(ERROR, tmp_ret,
                     "retry erasing unregistering ctx to keep lifecycle safe",
                     K(palf_id));
      }
    } else if (OB_NOT_NULL(entry_to_destroy)) {
      drain_finished = true;
    }
    if (!drain_finished) {
      ob_usleep(UNREGISTER_WAIT_INTERVAL_US);
    }
  }
}

int64_t LogAsyncIOWorker::get_ctx_count_() const
{
  return is_inited_ ? ctx_map_.size() : 0;
}

int64_t LogAsyncIOWorker::get_dropped_task_count_() const
{
  return dropped_task_count_;
}

int64_t LogAsyncIOWorker::get_dispatched_task_count_() const
{
  return dispatched_task_count_;
}

void LogAsyncIOWorker::destroy_entry_(AsyncPalfIOCtxEntry *entry)
{
  if (OB_NOT_NULL(entry)) {
    IAsyncPalfIOCtx *ctx = entry->get_ctx();
    entry->reset();
    if (OB_NOT_NULL(ctx)) {
      ctx->free_this();
    }
    OB_DELETE(AsyncPalfIOCtxEntry, "PalfAsyncEntry", entry);
  }
}

int LogAsyncIOWorker::register_and_create_ctx(
    const int64_t palf_id,
    const int cb_tg_id,
    IPalfEnvImpl *palf_env_impl,
    const AsyncThrottleContext &throttle_ctx)
{
  ObQSyncLockReadGuard guard(lifecycle_lock_);
  return register_and_create_ctx_(palf_id, cb_tg_id,
                                  palf_env_impl, throttle_ctx);
}

int LogAsyncIOWorker::register_and_create_ctx_(
    const int64_t palf_id,
    const int cb_tg_id,
    IPalfEnvImpl *palf_env_impl,
    const AsyncThrottleContext &throttle_ctx)
{
  int ret = OB_SUCCESS;
  AsyncPalfIOCtxEntry *entry = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (is_destroying_) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    ret = create_ctx_entry_(palf_id, cb_tg_id, palf_env_impl,
                            throttle_ctx, entry);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ctx_map_.set_refactored(palf_id, entry))) {
      ret = (OB_HASH_EXIST == ret) ? OB_ENTRY_EXIST : ret;
    } else {
      entry = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    PALF_LOG(INFO, "register async ctx", K(palf_id));
  } else {
    PALF_LOG(WARN, "register async ctx failed", KR(ret), K(palf_id));
  }
  if (OB_NOT_NULL(entry)) {
    destroy_entry_(entry);
  }
  add_worker_server_event_("ctx_register", ret, palf_id);
  return ret;
}

int LogAsyncIOWorker::unregister_palf_ctx_and_wait(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  AsyncPalfIOCtxEntry *entry_to_destroy = NULL;
  {
    // Only the unregister boundary needs lifecycle protection. The drain must
    // not block start/stop because accepted queue tasks may need the worker
    // thread to run before this ctx can become empty.
    ObQSyncLockReadGuard guard(lifecycle_lock_);
    ret = mark_ctx_unregistering_(palf_id);
  }
  if (OB_SUCC(ret)) {
    drain_and_erase_ctx_(palf_id, entry_to_destroy);
    if (OB_NOT_NULL(entry_to_destroy)) {
      destroy_entry_(entry_to_destroy);
    }
    PALF_LOG(INFO, "LogAsyncIOWorker unregister palf ctx success", K(palf_id));
  }
  add_worker_server_event_("ctx_unregister", ret, palf_id);
  return ret;
}

int LogAsyncIOWorker::mark_ctx_unregistering_(const int64_t palf_id)
{
  // The map bucket write lock establishes the unregister boundary. The caller
  // holds the lifecycle read lock while checking state and marking the entry.
  int ret = OB_SUCCESS;
  MarkUnregisteringOp mark_op;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogAsyncIOWorker is not initialized", KR(ret), K(palf_id));
  } else if (is_destroying_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "LogAsyncIOWorker is being destroyed", KR(ret), K(palf_id));
  } else if (OB_FAIL(ctx_map_.atomic_refactored(palf_id, mark_op))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      PALF_LOG(TRACE, "async ctx entry does not exist before unregister",
               KR(ret), K(palf_id));
    } else {
      PALF_LOG(ERROR, "access async ctx entry before unregister failed",
               KR(ret), K(palf_id));
    }
  } else if (OB_FAIL(mark_op.get_ret())) {
    if (OB_STATE_NOT_MATCH == ret) {
      PALF_LOG(WARN, "async ctx entry is already unregistering",
               KR(ret), K(palf_id));
    } else {
      PALF_LOG(ERROR, "mark async ctx entry unregistering failed",
               KR(ret), K(palf_id));
    }
  }
  return ret;
}

int LogAsyncIOWorker::submit_io_task(LogIOTask *task)
{
  ObQSyncLockReadGuard guard(lifecycle_lock_);
  return submit_io_task_(task);
}

int LogAsyncIOWorker::submit_io_task_(LogIOTask *task)
{
  int ret = OB_SUCCESS;
  AsyncPalfIOCtxEntryGuard entry_guard;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogAsyncIOWorker is not initialized", KR(ret), KP(task));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "submit null async io task", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(TRACE, "worker not running, reject submit", KR(ret), KP(task));
    }
  } else if (OB_FAIL(get_entry_guard_(task->get_palf_id(),
                                      false /* allow_unregistering */,
                                      entry_guard))) {
    diagnostics_.inc_dropped_submit_count();
    if (OB_ENTRY_NOT_EXIST == ret || OB_STATE_NOT_MATCH == ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(TRACE, "async io task admission rejected", KR(ret), KP(task),
                 "task_type", log_io_task_type_str(task->get_io_task_type()));
      }
    } else {
      PALF_LOG(ERROR, "async io task admission failed unexpectedly",
               KR(ret), KP(task),
               "task_type", log_io_task_type_str(task->get_io_task_type()));
    }
  } else if (OB_FAIL(entry_guard->try_reserve_task_slot(task->get_io_task_type()))) {
    diagnostics_.inc_dropped_submit_count();
    if (OB_SIZE_OVERFLOW == ret || OB_EAGAIN == ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(TRACE, "reserve async ctx task slot failed", KR(ret), KP(task),
                 "task_type", log_io_task_type_str(task->get_io_task_type()));
      }
    } else {
      PALF_LOG(ERROR, "reserve async ctx task slot failed unexpectedly",
               KR(ret), KP(task),
               "task_type", log_io_task_type_str(task->get_io_task_type()));
    }
  } else if (OB_FAIL(input_queue_.push(static_cast<void *>(task)))) {
    entry_guard->release_task_slot(task->get_io_task_type());
    diagnostics_.inc_dropped_submit_count();
    if (OB_SIZE_OVERFLOW == ret || OB_EAGAIN == ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(TRACE, "push async io task failed", KR(ret), KP(task),
                 "task_type", log_io_task_type_str(task->get_io_task_type()));
      }
    } else {
      PALF_LOG(ERROR, "push async io task failed unexpectedly", KR(ret), KP(task),
               "task_type", log_io_task_type_str(task->get_io_task_type()));
    }
  } else {
    task = NULL;
    diagnostics_.inc_submitted_task_count();
  }
  return ret;
}

int LogAsyncIOWorker::wake_up_for_drive()
{
  ObQSyncLockReadGuard guard(lifecycle_lock_);
  return wake_up_for_drive_();
}

int LogAsyncIOWorker::wake_up_for_drive_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogAsyncIOWorker is not initialized", KR(ret));
  } else if (!is_running_ && !has_set_stop()) {
    ret = OB_NOT_RUNNING;
    PALF_LOG(TRACE, "worker not running, skip async mark", KR(ret));
  } else if (!is_running_ && can_exit_write_loop_()) {
    ret = OB_NOT_RUNNING;
    PALF_LOG(TRACE, "worker has drained and exited, skip async mark", KR(ret));
  } else if (OB_FAIL(enqueue_drive_mark_())) {
    PALF_LOG(WARN, "enqueue async drive mark failed", KR(ret));
  }
  return ret;
}

int LogAsyncIOWorker::enqueue_drive_mark_()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&drive_pending_, 0, 1)) {
    // 只有把 drive_pending_ 从 0 改成 1 的线程负责入队 marker；其他并发
    // 唤醒只计入合并统计，不重复占用队列。
    if (OB_FAIL(input_queue_.push(static_cast<void *>(&drive_mark_task_)))) {
      ATOMIC_STORE(&drive_pending_, 0);
      PALF_LOG(WARN, "push async mark task failed", KR(ret));
    } else {
      diagnostics_.inc_drive_wake_count();
    }
  } else {
    diagnostics_.inc_coalesced_drive_wake_count();
  }
  return ret;
}

void LogAsyncIOWorker::run1()
{
  int ret = OB_SUCCESS;
  ObDIActionGuard ag("LogService", "LogAsyncIOWorker", NULL);
  lib::set_thread_name("AsyncWriteLoop");
  if (OB_FAIL(write_loop_())) {
    PALF_LOG(WARN, "async write loop exited with error", KR(ret), KP(this));
  }
}

int LogAsyncIOWorker::write_loop_()
{
  // 始终先重试 pending_dispatch_task_，防止后续任务越过暂时无法接收的队头。
  // 连续分发最多 256 个任务后 drive 全部 ctx；队列空、marker 到达或分发受阻
  // 也会触发 drive。stop 后继续该流程，直到 pending、队列和所有 ctx 排空。
  int ret = OB_SUCCESS;
  static const int64_t MAX_DISPATCH_BATCH_BEFORE_DRIVE = 256;
  static const int64_t DISPATCH_RETRY_INTERVAL_US = 1000;
  while (!can_exit_write_loop_()) {
    void *raw = NULL;
    int64_t wait_us = 0;
    int64_t dispatch_batch_count = 0;
    bool dispatch_blocked = false;
    int pop_ret = OB_SUCCESS;
    LogIOTask *pending_task = ATOMIC_LOAD(&pending_dispatch_task_);
    if (OB_NOT_NULL(pending_task)) {
      raw = pending_task;
    } else {
      pop_ret = input_queue_.pop(raw, 0);
    }
    if (OB_ENTRY_NOT_EXIST == pop_ret) {
      (void) drive_all_ctx_once_();
      if (can_exit_write_loop_()) {
        // 已接收任务全部排空后直接退出，不再进入阻塞等待。
      } else {
        wait_us = get_next_queue_wait_us_();
        raw = NULL;
        pop_ret = input_queue_.pop(raw, wait_us);
      }
    }
    while (OB_SUCCESS == pop_ret && OB_NOT_NULL(raw)) {
      LogIOTask *task = static_cast<LogIOTask *>(raw);
      raw = NULL;
      bool need_drive_after_task = false;
      if (is_async_mark_task_(task)) {
        // 必须在 drive 前清零。这样 drive 期间若 callback 又产生新工作，仍能
        // 入队下一枚 marker；若在 drive 后清零，会丢掉这次并发唤醒。
        ATOMIC_STORE(&drive_pending_, 0);
        need_drive_after_task = true;
      } else {
        ATOMIC_STORE(&pending_dispatch_task_, task);
        LogIOTask *dispatch_task = task;
        if (OB_SUCCESS != handle_queued_log_io_task_(dispatch_task)) {
          dispatch_blocked = true;
          need_drive_after_task = true;
        } else {
          ATOMIC_STORE(&pending_dispatch_task_, static_cast<LogIOTask *>(NULL));
          diagnostics_.inc_handled_task_count();
          ++dispatch_batch_count;
          need_drive_after_task =
              dispatch_batch_count >= MAX_DISPATCH_BATCH_BEFORE_DRIVE;
        }
      }
      if (need_drive_after_task) {
        (void) drive_all_ctx_once_();
        dispatch_batch_count = 0;
      }
      if (dispatch_blocked) {
        ob_usleep(DISPATCH_RETRY_INTERVAL_US);
        break;
      } else {
        pop_ret = input_queue_.pop(raw, 0);
      }
    }
    if (!dispatch_blocked && OB_SUCCESS == pop_ret) {
      PALF_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid null async worker queue task");
    } else if (!dispatch_blocked && OB_ENTRY_NOT_EXIST != pop_ret) {
      PALF_LOG(WARN, "pop async worker queue failed", KR(pop_ret));
    }
    if (input_queue_.size() > 0) {
      input_queue_size_stat_.stat(input_queue_.size());
    }
    print_worker_stat_();
  }
  PALF_LOG(INFO, "write_loop_ exit", KR(ret));
  return ret;
}

bool LogAsyncIOWorker::can_exit_write_loop_() const
{
  bool can_exit = false;
  if (has_set_stop()) {
    can_exit = (OB_ISNULL(ATOMIC_LOAD(&pending_dispatch_task_))
                && 0 == input_queue_.size()
                && is_drained_());
  }
  return can_exit;
}

int64_t LogAsyncIOWorker::calc_queue_wait_us_(const int64_t deadline,
                                              const int64_t now)
{
  const int64_t remaining = deadline - now;
  return remaining <= 0 ? 0 : MIN(remaining, QUEUE_WAIT_TIME_US);
}

int64_t LogAsyncIOWorker::get_next_queue_wait_us_() const
{
  // Use the backstop when there is no timed work. For a drive deadline,
  // wake immediately when due or wait no longer than the backstop.
  const int64_t deadline = diagnostics_.get_next_drive_deadline();
  return deadline > 0
      ? calc_queue_wait_us_(deadline, common::ObTimeUtility::fast_current_time())
      : QUEUE_WAIT_TIME_US;
}

bool LogAsyncIOWorker::is_async_mark_task_(LogIOTask *task) const
{
  bool is_async_mark = false;
  if (OB_NOT_NULL(task)) {
    is_async_mark = (LogIOTaskType::ASYNC_MARK_TYPE == task->get_io_task_type());
  }
  return is_async_mark;
}

int LogAsyncIOWorker::handle_queued_log_io_task_(LogIOTask *&task)
{
  ObDIActionGuard ag(OB_ISNULL(task) ? "NULL_ASYNC_LOG_IO_TASK" :
      log_io_task_type_str(task->get_io_task_type()));
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid null async queued task", KR(ret));
  } else {
    wait_cost_stat_.stat(start_ts - task->get_init_task_ts());
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      PALF_LOG(WARN, "async worker is not initialized, keep queued task for retry", KR(ret),
               KP(task), "task_type", log_io_task_type_str(task->get_io_task_type()));
    } else if (OB_FAIL(dispatch_task_(task))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "dispatch async queued task failed, keep task for retry",
                 KR(ret), KP(task), "task_type", log_io_task_type_str(task->get_io_task_type()));
      }
    } else {
      task = NULL;
    }
  }
  dispatch_cost_stat_.stat(ObTimeUtility::current_time() - start_ts);
  return ret;
}

int LogAsyncIOWorker::drive_all_ctx_once_()
{
  ObDIActionGuard ag("drive async palf ctx");
  int ret = OB_SUCCESS;
  if (is_inited_) {
    const int64_t start_ts = ObTimeUtility::current_time();
    int64_t next_drive_interval_us = INT64_MAX;
    int64_t next_drive_deadline = 0;
    ret = drive_write_all_(next_drive_interval_us);
    if (INT64_MAX != next_drive_interval_us) {
      const int64_t now = ObTimeUtility::fast_current_time();
      next_drive_deadline = next_drive_interval_us > INT64_MAX - now
          ? INT64_MAX : now + next_drive_interval_us;
    }
    diagnostics_.set_next_drive_deadline(next_drive_deadline);
    drive_cost_stat_.stat(ObTimeUtility::current_time() - start_ts);
    if (OB_SUCCESS != ret) {
      PALF_LOG(TRACE, "drive all async ctx failed", KR(ret));
    }
  }
  return ret;
}

void LogAsyncIOWorker::print_worker_stat_()
{
  LogAsyncIOWorkerDiagnostics current_stat;
  LogAsyncIOWorkerDiagnostics delta_stat;
  current_stat.snapshot_from(diagnostics_);
  current_stat.subtract(last_summary_stat_, delta_stat);
  if (palf_reach_time_interval(5LL * 1000 * 1000, print_log_interval_)) {
    PALF_LOG(INFO, "[PALF STAT ASYNC IO WORKER]",
             K_(tenant_id),
             "input_queue_size", input_queue_.size(),
             "ctx_count", get_ctx_count_(),
             "submitted_delta", delta_stat.get_submitted_task_count(),
             "dropped_delta", delta_stat.get_dropped_submit_count(),
             "handled_delta", delta_stat.get_handled_task_count(),
             "drive_wake_delta", delta_stat.get_drive_wake_count(),
             "coalesced_drive_wake_delta", delta_stat.get_coalesced_drive_wake_count(),
             "total_submitted", current_stat.get_submitted_task_count(),
             "total_dropped", current_stat.get_dropped_submit_count(),
             "total_handled", current_stat.get_handled_task_count(),
             "ctx_dispatch_dropped", get_dropped_task_count_(),
             "ctx_dispatched", get_dispatched_task_count_(),
             "next_drive_deadline", current_stat.get_next_drive_deadline());
    last_summary_stat_.snapshot_from(current_stat);
  }
  add_worker_summary_server_event_();
}

void LogAsyncIOWorker::add_worker_server_event_(const char *event,
                                                const int ret,
                                                const int64_t palf_id)
{
  const int64_t queue_cnt = is_inited_ ? input_queue_.size() : 0;
  const int64_t ctx_cnt = is_inited_ ? get_ctx_count_() : 0;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(event)) {
    PALF_LOG(WARN, "invalid null async worker server event", K(ret), K(palf_id));
  } else if (OB_SUCCESS != (tmp_ret = SERVER_EVENT_ADD("PALF_ASYNC_IO",
                                                        event,
                                                        "tenant_id",
                                                        tenant_id_,
                                                        "palf_id",
                                                        palf_id,
                                                        "queue_cnt",
                                                        queue_cnt,
                                                        "ctx_cnt",
                                                        ctx_cnt,
                                                        "ret",
                                                        ret))) {
    PALF_LOG(WARN, "add async worker server event failed",
             KR(tmp_ret), K(event), K(ret), K(palf_id));
  }
}

void LogAsyncIOWorker::add_worker_summary_server_event_()
{
  static constexpr int64_t EVENT_SUMMARY_INTERVAL_US = 60LL * 1000 * 1000;
  int ret = OB_SUCCESS;
  const int64_t queue_cnt = input_queue_.size();
  const int64_t ctx_cnt = get_ctx_count_();
  LogAsyncIOWorkerDiagnostics current_stat;
  current_stat.snapshot_from(diagnostics_);
  const bool has_activity = current_stat.has_task_activity_since(last_event_stat_) || queue_cnt > 0;
  if (has_activity && palf_reach_time_interval(EVENT_SUMMARY_INTERVAL_US, event_summary_interval_)) {
    if (OB_SUCCESS != (ret = SERVER_EVENT_ADD("PALF_ASYNC_IO",
                                              "worker_summary",
                                              "tenant_id",
                                              tenant_id_,
                                              "queue_cnt",
                                              queue_cnt,
                                              "ctx_cnt",
                                              ctx_cnt,
                                              "submitted",
                                              current_stat.get_submitted_task_count(),
                                              "dropped",
                                              current_stat.get_dropped_submit_count(),
                                              "handled",
                                              current_stat.get_handled_task_count()))) {
      PALF_LOG(WARN, "add async worker summary server event failed", KR(ret), KP(this));
    } else {
      last_event_stat_.snapshot_from(current_stat);
    }
  }
}

} // end namespace palf
} // end namespace oceanbase

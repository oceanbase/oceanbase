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

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>

#define private public
#define protected public

#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/log_async_io_struct.h"
#include "logservice/palf/log_async_io_worker.h"
#include "logservice/palf/log_async_palf_ctx.h"
#include "logservice/palf/log_io_worker_wrapper.h"
#include "logservice/palf/log_io_task.h"
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/lsn.h"
#include "unittest/logservice/mock_palf_handle_impl_for_async.h"

namespace oceanbase
{
namespace palf
{
using namespace common;

class FakePalfEnvImpl : public IPalfEnvImpl
{
public:
  FakePalfEnvImpl() : handle_() {}
  ~FakePalfEnvImpl() override {}

  int get_palf_handle_impl(const int64_t palf_id,
                           IPalfHandleImplGuard &guard) override
  {
    guard.palf_id_ = palf_id;
    guard.palf_handle_impl_ = &handle_;
    guard.palf_env_impl_ = this;
    return OB_SUCCESS;
  }
  int get_palf_handle_impl(const int64_t palf_id,
                           IPalfHandleImpl *&handle) override
  {
    UNUSED(palf_id);
    handle = &handle_;
    return OB_SUCCESS;
  }
  int create_palf_handle_impl(const int64_t palf_id,
                              const AccessMode &access_mode,
                              const PalfBaseInfo &base_info,
                              const LogReplicaType replica_type,
                              IPalfHandleImpl *&handle) override
  {
    UNUSED(palf_id);
    UNUSED(access_mode);
    UNUSED(base_info);
    UNUSED(replica_type);
    handle = NULL;
    return OB_NOT_SUPPORTED;
  }
  int remove_palf_handle_impl(const int64_t palf_id) override
  { UNUSED(palf_id); return OB_NOT_SUPPORTED; }
  void revert_palf_handle_impl(IPalfHandleImpl *handle) override { UNUSED(handle); }
  common::ObILogAllocator *get_log_allocator() override { return NULL; }
  int for_each(const common::ObFunction<int(IPalfHandleImpl *)> &func) override
  { return func(&handle_); }
  int create_directory(const char *base_dir) override
  { UNUSED(base_dir); return OB_NOT_SUPPORTED; }
  int remove_directory(const char *base_dir) override
  { UNUSED(base_dir); return OB_NOT_SUPPORTED; }
  bool check_disk_space_enough() override { return true; }
  int64_t get_rebuild_replica_log_lag_threshold() const override { return 0; }
  int get_io_statistic_info(int64_t &last_working_time,
                            int64_t &pending_write_size,
                            int64_t &pending_write_count,
                            int64_t &pending_write_rt,
                            int64_t &accum_write_size,
                            int64_t &accum_write_count,
                            int64_t &accum_write_rt) override
  {
    last_working_time = 0;
    pending_write_size = 0;
    pending_write_count = 0;
    pending_write_rt = 0;
    accum_write_size = 0;
    accum_write_count = 0;
    accum_write_rt = 0;
    return OB_SUCCESS;
  }
  int64_t get_tenant_id() override { return OB_SERVER_TENANT_ID; }
  int update_replayable_point(const share::SCN &replayable_scn) override
  { UNUSED(replayable_scn); return OB_SUCCESS; }
  int get_throttling_options(PalfThrottleOptions &option) override
  { option.reset(); return OB_SUCCESS; }
  void period_calc_disk_usage() override {}
  LogSharedQueueTh *get_log_shared_queue_thread() override { return NULL; }
  int get_options(PalfOptions &options) override
  { options.reset(); return OB_SUCCESS; }

private:
  oceanbase::unittest::MockAsyncPalfHandleImpl handle_;
};

class FakeLogIOTask : public LogIOTask
{
public:
  explicit FakeLogIOTask(int64_t palf_id)
    : LogIOTask(palf_id, 1)
  {}
  FakeLogIOTask(int64_t palf_id, int64_t task_epoch)
    : LogIOTask(palf_id, task_epoch)
  {}
  ~FakeLogIOTask() override {}
  int do_task_(int /*tg_id*/, IPalfHandleImplGuard &/*guard*/) override { return OB_SUCCESS; }
  int after_consume_(IPalfHandleImplGuard &/*guard*/) override { return OB_SUCCESS; }
  LogIOTaskType get_io_task_type_() const override { return LogIOTaskType::FLUSH_LOG_TYPE; }
  void free_this_(IPalfEnvImpl *) override { delete this; }
  int64_t get_io_size_() const override { return 0; }
  bool need_purge_throttling_() const override { return false; }
};

class TypedFakeLogIOTask : public LogIOTask
{
public:
  TypedFakeLogIOTask(int64_t palf_id, LogIOTaskType task_type)
    : LogIOTask(palf_id, 1),
      task_type_(task_type)
  {}
  ~TypedFakeLogIOTask() override {}
  int do_task_(int /*tg_id*/, IPalfHandleImplGuard &/*guard*/) override { return OB_SUCCESS; }
  int after_consume_(IPalfHandleImplGuard &/*guard*/) override { return OB_SUCCESS; }
  LogIOTaskType get_io_task_type_() const override { return task_type_; }
  void free_this_(IPalfEnvImpl *) override { delete this; }
  int64_t get_io_size_() const override { return 0; }
  bool need_purge_throttling_() const override { return false; }
private:
  LogIOTaskType task_type_;
};

// Fake ctx that records worker dispatch, drive, and completion behavior.
class FakeAsyncPalfIOCtx : public IAsyncPalfIOCtx
{
public:
  explicit FakeAsyncPalfIOCtx(int64_t palf_id)
    : palf_id_(palf_id),
      inflight_(0),
      active_ref_(0),
      need_wake_(false)
  {}
  ~FakeAsyncPalfIOCtx() override {}

  int64_t get_palf_id() const override { return palf_id_; }

  int try_reserve_task_slot(const LogIOTaskType /*task_type*/) override
  {
    task_reserve_entered_.store(true);
    task_reserve_count_.fetch_add(1);
    while (hold_task_reserve_.load()) {
      ob_usleep(100);
    }
    const int ret = task_reserve_ret_.load();
    if (OB_SUCCESS == ret) {
      reserved_task_count_.fetch_add(1);
    }
    return ret;
  }
  void release_task_slot(const LogIOTaskType /*task_type*/) override
  {
    task_release_count_.fetch_add(1);
    reserved_task_count_.fetch_sub(1);
  }
  int enqueue_task(LogIOTask *task) override
  {
    const int ret = enqueue_ret_.load();
    if (OB_SUCCESS == ret) {
      enqueued_tasks_.fetch_add(1);
      pending_tasks_.fetch_add(1);
      last_task_ = task;
      if (reserved_task_count_.load() > 0) {
        release_task_slot(task->get_io_task_type());
      }
    }
    return ret;
  }
  int on_aio_complete(const AsyncIOCompletionEvent &event,
                      bool &need_wake_worker) override
  {
    completions_.fetch_add(1);
    if (inflight_.load() > 0) {
      inflight_.fetch_sub(1);
    }
    last_event_ = event;
    need_wake_worker = need_wake_;
    return OB_SUCCESS;
  }
  int drive_write(int64_t &next_drive_interval_us) override
  {
    const int64_t old_drive_writes = drive_writes_.fetch_add(1);
    if (0 == old_drive_writes) {
      first_drive_enqueued_tasks_.store(enqueued_tasks_.load());
    }
    if (!hold_pending_.load() && pending_tasks_.load() > 0) {
      pending_tasks_.fetch_sub(1);
    }
    if (next_drive_interval_override_.load() >= 0) {
      next_drive_interval_us = next_drive_interval_override_.load();
    } else {
      next_drive_interval_us = pending_tasks_.load() > 0 && 0 == inflight_.load()
          ? 0 : INT64_MAX;
    }
    return drive_ret_.load();
  }
  bool is_drained() const override
  {
    return 0 == reserved_task_count_.load()
        && 0 == pending_tasks_.load()
        && 0 == inflight_.load();
  }
  // Record request_drive() calls so tests can verify callback wake requests.
  int request_drive() override
  {
    request_drives_.fetch_add(1);
    return OB_SUCCESS;
  }
  int64_t get_inflight_count() const override
  { return reserved_task_count_.load() + pending_tasks_.load() + inflight_.load(); }
  int64_t get_oldest_pending_io_start_ts() const override
  {
    return oldest_pending_io_start_ts_.load();
  }
  int64_t get_throttle_next_admit_ts() const override { return throttle_next_admit_ts_; }
  void pin() override { active_ref_.fetch_add(1); }
  void unpin() override { active_ref_.fetch_sub(1); }
  int64_t get_active_ref() const override { return active_ref_.load(); }
  void free_this() override { free_this_count_.fetch_add(1); }

  void set_inflight(int64_t n) { inflight_.store(n); }
  void set_active_ref(int64_t active_ref) { active_ref_.store(active_ref); }
  void set_need_wake(bool v) { need_wake_ = v; }
  void set_hold_pending(bool v) { hold_pending_.store(v); }
  void set_enqueue_ret(int ret) { enqueue_ret_.store(ret); }
  void set_hold_task_reserve(bool hold) { hold_task_reserve_.store(hold); }
  void set_task_reserve_ret(int ret) { task_reserve_ret_.store(ret); }
  void set_drive_ret(int ret) { drive_ret_.store(ret); }
  void set_next_drive_interval(const int64_t next_drive_interval_us)
  {
    next_drive_interval_override_.store(next_drive_interval_us);
  }
  void set_oldest_pending_io_start_ts(int64_t ts)
  {
    oldest_pending_io_start_ts_.store(ts);
  }
  void set_throttle_next_admit_ts(const int64_t ts) { throttle_next_admit_ts_ = ts; }
  bool is_task_reserve_entered() const { return task_reserve_entered_.load(); }
  int64_t get_task_reserve_count() const { return task_reserve_count_.load(); }
  int64_t get_task_release_count() const { return task_release_count_.load(); }
  int64_t get_reserved_task_count() const { return reserved_task_count_.load(); }
  int64_t get_enqueued_tasks() const { return enqueued_tasks_.load(); }
  int64_t get_pending_tasks() const { return pending_tasks_.load(); }
  int64_t get_completions() const { return completions_.load(); }
  int64_t get_drive_writes() const { return drive_writes_.load(); }
  int64_t get_first_drive_enqueued_tasks() const { return first_drive_enqueued_tasks_.load(); }
  int64_t get_request_drives() const { return request_drives_.load(); }
  int64_t get_free_this_count() const { return free_this_count_.load(); }
  LogIOTask *get_last_task() const { return last_task_; }
  const AsyncIOCompletionEvent &get_last_event() const { return last_event_; }
  TO_STRING_KV(K_(palf_id),
               "inflight", inflight_.load(),
               "active_ref", active_ref_.load(),
               "reserved_task_count", reserved_task_count_.load(),
               K_(need_wake),
               "enqueued_tasks", enqueued_tasks_.load(),
               "pending_tasks", pending_tasks_.load(),
               "completions", completions_.load(),
               "drive_writes", drive_writes_.load(),
               "request_drives", request_drives_.load());

private:
  int64_t palf_id_;
  std::atomic<int64_t> inflight_;
  std::atomic<int64_t> active_ref_;
  bool need_wake_;
  std::atomic<int64_t> enqueued_tasks_{0};
  std::atomic<int64_t> pending_tasks_{0};
  std::atomic<int64_t> completions_{0};
  std::atomic<int64_t> drive_writes_{0};
  std::atomic<int64_t> first_drive_enqueued_tasks_{0};
  std::atomic<int64_t> request_drives_{0};
  std::atomic<int64_t> free_this_count_{0};
  std::atomic<bool> hold_pending_{false};
  std::atomic<int> enqueue_ret_{OB_SUCCESS};
  std::atomic<bool> hold_task_reserve_{false};
  std::atomic<bool> task_reserve_entered_{false};
  std::atomic<int> task_reserve_ret_{OB_SUCCESS};
  std::atomic<int64_t> task_reserve_count_{0};
  std::atomic<int64_t> task_release_count_{0};
  std::atomic<int64_t> reserved_task_count_{0};
  std::atomic<int> drive_ret_{OB_SUCCESS};
  std::atomic<int64_t> next_drive_interval_override_{-1};
  std::atomic<int64_t> oldest_pending_io_start_ts_{0};
  int64_t throttle_next_admit_ts_ = 0;
  LogIOTask *last_task_ = NULL;
  AsyncIOCompletionEvent last_event_;
};

static AsyncIOCompletionEvent make_event(int64_t palf_id,
                                         int64_t slot = 0,
                                         int64_t gen = 1)
{
  AsyncIOCompletionEvent ev;
  ev.ctx.palf_id = palf_id;
  ev.ctx.fragment_ref.slot_id = slot;
  ev.ctx.fragment_ref.generation = gen;
  ev.ctx.begin_lsn = LSN(0);
  ev.ctx.end_lsn = LSN(1);
  ev.ret_code = 0;
  ev.finish_ts = 1;
  return ev;
}

TEST(TestPalfAsyncIOWorker, DiagnosticsSnapshotAndDelta)
{
  LogAsyncIOWorkerDiagnostics base;
  LogAsyncIOWorkerDiagnostics current;
  LogAsyncIOWorkerDiagnostics delta;
  base.inc_submitted_task_count();
  base.inc_dropped_submit_count();
  base.inc_handled_task_count();
  base.inc_drive_wake_count();
  base.inc_coalesced_drive_wake_count();

  current.snapshot_from(base);
  current.inc_submitted_task_count();
  current.inc_submitted_task_count();
  current.inc_dropped_submit_count();
  current.inc_handled_task_count();
  current.inc_drive_wake_count();
  current.inc_coalesced_drive_wake_count();
  current.set_next_drive_deadline(123);

  current.subtract(base, delta);
  EXPECT_EQ(2, delta.get_submitted_task_count());
  EXPECT_EQ(1, delta.get_dropped_submit_count());
  EXPECT_EQ(1, delta.get_handled_task_count());
  EXPECT_EQ(1, delta.get_drive_wake_count());
  EXPECT_EQ(1, delta.get_coalesced_drive_wake_count());
  EXPECT_EQ(123, delta.get_next_drive_deadline());
  EXPECT_TRUE(current.has_task_activity_since(base));

  base.snapshot_from(current);
  EXPECT_FALSE(current.has_task_activity_since(base));
}

TEST(TestPalfAsyncIOWorker, QueueWaitHonorsBackstopAndDeadline)
{
  LogAsyncIOWorker worker;
  EXPECT_EQ(100 * 1000, LogAsyncIOWorker::QUEUE_WAIT_TIME_US);
  EXPECT_EQ(LogAsyncIOWorker::QUEUE_WAIT_TIME_US,
            worker.get_next_queue_wait_us_());

  const int64_t now = 100 * 1000;
  EXPECT_EQ(0, LogAsyncIOWorker::calc_queue_wait_us_(now - 1, now));

  EXPECT_EQ(1000, LogAsyncIOWorker::calc_queue_wait_us_(now + 1000, now));

  EXPECT_EQ(LogAsyncIOWorker::QUEUE_WAIT_TIME_US,
            LogAsyncIOWorker::calc_queue_wait_us_(INT64_MAX, now));
}

TEST(TestPalfAsyncIOWorker, DriveIntervalControlsNextWaitWithoutSelfWake)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(101);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(101, &ctx));

  ctx.set_next_drive_interval(0);
  ASSERT_EQ(OB_SUCCESS, worker.drive_all_ctx_once_());
  EXPECT_EQ(0, worker.get_next_queue_wait_us_());
  EXPECT_EQ(0, worker.input_queue_.size());

  const int64_t interval_us = 1000 * 1000;
  const int64_t before_drive = ObTimeUtility::fast_current_time();
  ctx.set_next_drive_interval(interval_us);
  ASSERT_EQ(OB_SUCCESS, worker.drive_all_ctx_once_());
  const int64_t after_drive = ObTimeUtility::fast_current_time();
  const int64_t deadline = worker.diagnostics_.get_next_drive_deadline();
  EXPECT_GE(deadline, before_drive + interval_us);
  EXPECT_LE(deadline, after_drive + interval_us);

  ctx.set_next_drive_interval(INT64_MAX);
  ASSERT_EQ(OB_SUCCESS, worker.drive_all_ctx_once_());
  EXPECT_EQ(0, worker.diagnostics_.get_next_drive_deadline());
  EXPECT_EQ(LogAsyncIOWorker::QUEUE_WAIT_TIME_US,
            worker.get_next_queue_wait_us_());

  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(101));
  worker.destroy();
}

static void drive_until_drained(LogAsyncIOWorker &worker, FakeAsyncPalfIOCtx &ctx)
{
  int64_t next_drive_interval_us = INT64_MAX;
  for (int64_t i = 0; i < 100 && !ctx.is_drained(); i++) {
    next_drive_interval_us = INT64_MAX;
    EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  }
  EXPECT_TRUE(ctx.is_drained());
}

int LogAsyncIOWorker::register_ctx_entry_(const int64_t palf_id, IAsyncPalfIOCtx *ctx)
{
  int ret = OB_SUCCESS;
  AsyncPalfIOCtxEntry *entry = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogAsyncIOWorker is not initialized", KR(ret), K(palf_id));
  } else if (palf_id < 0 || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(palf_id), KP(ctx));
  } else if (OB_ISNULL(entry = OB_NEW(
                 AsyncPalfIOCtxEntry,
                 ObMemAttr(OB_SERVER_TENANT_ID, "TestAsyncEntry")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloc test async ctx entry failed", KR(ret), K(palf_id));
  } else if (OB_FAIL(entry->init(ctx))) {
    PALF_LOG(WARN, "init test async ctx entry failed", KR(ret), K(palf_id));
  } else if (OB_FAIL(ctx_map_.set_refactored(palf_id, entry))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_ENTRY_EXIST;
      PALF_LOG(WARN, "ctx already registered", KR(ret), K(palf_id));
    } else {
      PALF_LOG(WARN, "register ctx entry failed", KR(ret), K(palf_id));
    }
  } else {
    entry = NULL;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(entry)) {
    destroy_entry_(entry);
  }
  return ret;
}

int LogAsyncIOWorker::unregister_ctx_entry_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  AsyncPalfIOCtxEntry *entry_to_destroy = NULL;
  AsyncPalfIOCtxEntryGuard entry_guard;
  // This test-only unregister path may revisit an entry already being drained.
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogAsyncIOWorker is not initialized", KR(ret), K(palf_id));
  } else if (palf_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), K(palf_id));
  } else if (OB_FAIL(get_entry_guard_(palf_id,
                                      true /* allow_unregistering */,
                                      entry_guard))) {
    PALF_LOG(WARN, "hold test async ctx entry failed", KR(ret), K(palf_id));
  } else {
    entry_guard.get_entry()->set_unregistering();
    entry_guard.reset();
    drain_and_erase_ctx_(palf_id, entry_to_destroy);
    if (OB_NOT_NULL(entry_to_destroy)) {
      destroy_entry_(entry_to_destroy);
    }
  }
  return ret;
}

static int register_worker_fake_ctx(LogAsyncIOWorker &worker,
                                    const int64_t palf_id,
                                    FakeAsyncPalfIOCtx &ctx)
{
  return worker.register_ctx_entry_(palf_id, &ctx);
}

static int get_entry_active_ref(LogAsyncIOWorker &worker,
                                const int64_t palf_id,
                                int64_t &active_ref)
{
  int ret = OB_SUCCESS;
  AsyncPalfIOCtxEntryGuard entry_guard;
  active_ref = -1;
  // Tests inspect reference counts while unregister is draining the entry.
  if (OB_FAIL(worker.get_entry_guard_(palf_id,
                                      true /* allow_unregistering */,
                                      entry_guard))) {
    PALF_LOG(WARN, "hold async ctx entry failed in unittest", KR(ret), K(palf_id));
  } else {
    active_ref = entry_guard.get_entry()->get_active_ref() - 1;
  }
  return ret;
}

// ============================================================
// Routing tests for LogAsyncIOWorker
// ============================================================

TEST(RoutingTest, init_destroy)
{
  LogAsyncIOWorker worker;
  EXPECT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  EXPECT_EQ(OB_INIT_TWICE, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  EXPECT_EQ(0, worker.get_ctx_count_());
  worker.destroy();
}

TEST(RoutingTest, register_dispatch_unregister)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(1001);
  EXPECT_EQ(OB_SUCCESS, worker.register_ctx_entry_(1001, &ctx));
  EXPECT_EQ(1, worker.get_ctx_count_());

  EXPECT_EQ(OB_ENTRY_EXIST, worker.register_ctx_entry_(1001, &ctx));

  FakeLogIOTask *t = new FakeLogIOTask(1001);
  EXPECT_EQ(OB_SUCCESS, worker.dispatch_task_(t));
  EXPECT_EQ(1, ctx.get_enqueued_tasks());
  EXPECT_EQ(1, worker.get_dispatched_task_count_());
  drive_until_drained(worker, ctx);
  delete t;

  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1001));
  EXPECT_EQ(0, worker.get_ctx_count_());
  worker.destroy();
}

TEST(RoutingTest, register_and_create_ctx_installs_owned_ctx)
{
  share::ObTenantBase tenant_base(OB_SERVER_TENANT_ID);
  share::ObTenantSwitchGuard tenant_guard(&tenant_base);
  LogAsyncIOWorker worker;
  FakePalfEnvImpl fake_env;
  AsyncThrottleContext throttle_ctx;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  EXPECT_EQ(OB_SUCCESS, worker.register_and_create_ctx(
      1002, 1 /* cb_tg_id */, &fake_env,
      throttle_ctx));
  EXPECT_EQ(1, worker.get_ctx_count_());

  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1002));
  EXPECT_EQ(0, worker.get_ctx_count_());
  worker.destroy();
}

TEST(RoutingTest, register_and_create_ctx_rejects_invalid_dependencies)
{
  share::ObTenantBase tenant_base(OB_SERVER_TENANT_ID);
  share::ObTenantSwitchGuard tenant_guard(&tenant_base);
  LogAsyncIOWorker worker;
  FakePalfEnvImpl fake_env;
  AsyncThrottleContext throttle_ctx;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            worker.register_and_create_ctx(1002, 0 /* cb_tg_id */,
                                           &fake_env, throttle_ctx));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            worker.register_and_create_ctx(1002, 1 /* cb_tg_id */,
                                           NULL, throttle_ctx));
  EXPECT_EQ(0, worker.get_ctx_count_());
  worker.destroy();
}

TEST(RoutingTest, unregister_invokes_ctx_free_this_once)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(1003);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(1003, &ctx));
  EXPECT_EQ(1, worker.get_ctx_count_());
  int64_t active_ref = -1;
  ASSERT_EQ(OB_SUCCESS, get_entry_active_ref(worker, 1003, active_ref));
  EXPECT_EQ(0, active_ref);
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1003));
  EXPECT_EQ(1, ctx.get_free_this_count());
  worker.destroy();

  EXPECT_EQ(1003, ctx.get_palf_id());
}

TEST(RoutingTest, unregister_waits_until_ctx_pending_tasks_are_drained)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(1004);
  ctx.set_hold_pending(true);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(1004, &ctx));

  FakeLogIOTask *task = new FakeLogIOTask(1004);
  ASSERT_EQ(OB_SUCCESS, worker.dispatch_task_(task));
  ASSERT_EQ(1, ctx.get_pending_tasks());
  ASSERT_EQ(1, ctx.get_inflight_count());
  int64_t active_ref = -1;
  ASSERT_EQ(OB_SUCCESS, get_entry_active_ref(worker, 1004, active_ref));
  ASSERT_EQ(0, active_ref);

  std::atomic<bool> unreg_done{false};
  std::thread unreg([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1004));
    unreg_done.store(true);
  });

  ob_usleep(30 * 1000);
  EXPECT_FALSE(unreg_done.load());
  EXPECT_EQ(1, worker.get_ctx_count_());

  ctx.set_hold_pending(false);
  int64_t next_drive_interval_us = INT64_MAX;
  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  for (int i = 0; i < 1000 && !unreg_done.load(); ++i) {
    ob_usleep(1000);
  }

  EXPECT_TRUE(unreg_done.load());
  EXPECT_EQ(0, worker.get_ctx_count_());
  EXPECT_TRUE(ctx.is_drained());

  unreg.join();
  delete task;
  worker.destroy();
}

TEST(RoutingTest, unregister_waits_until_ctx_pin_is_released)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(1005);
  std::atomic<bool> unreg_done(false);
  int unreg_ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(1005, &ctx));
  ctx.pin();

  std::thread unreg([&]() {
    unreg_ret = worker.unregister_ctx_entry_(1005);
    unreg_done.store(true);
  });
  ob_usleep(30 * 1000);
  EXPECT_FALSE(unreg_done.load());
  EXPECT_EQ(1, worker.get_ctx_count_());

  ctx.unpin();
  for (int64_t i = 0; i < 1000 && !unreg_done.load(); ++i) {
    ob_usleep(1000);
  }
  unreg.join();
  EXPECT_TRUE(unreg_done.load());
  EXPECT_EQ(OB_SUCCESS, unreg_ret);
  EXPECT_EQ(0, worker.get_ctx_count_());
  EXPECT_EQ(1, ctx.get_free_this_count());
  worker.destroy();
}

TEST(RoutingTest, dispatch_task_missing_ctx_returns_error)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  FakeLogIOTask *t = new FakeLogIOTask(999);
  int ret = worker.dispatch_task_(t);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
  EXPECT_EQ(1, worker.get_dropped_task_count_());
  delete t;

  worker.destroy();
}

TEST(RoutingTest, dispatch_task_ignores_task_epoch)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(1, &ctx));

  FakeLogIOTask *t = new FakeLogIOTask(1, 6 /* task_epoch */);
  EXPECT_EQ(OB_SUCCESS, worker.dispatch_task_(t));
  EXPECT_EQ(1, ctx.get_enqueued_tasks());
  EXPECT_EQ(0, worker.get_dropped_task_count_());
  drive_until_drained(worker, ctx);
  delete t;

  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1));
  worker.destroy();
}

TEST(RoutingTest, dispatch_task_to_unregistered_ctx_dropped)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(7);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(7, &ctx));

  // Unregister the ctx; dispatch_task for that palf is then not found.
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(7));
  FakeLogIOTask *t = new FakeLogIOTask(7);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, worker.dispatch_task_(t));
  EXPECT_EQ(0, ctx.get_enqueued_tasks());
  EXPECT_GE(worker.get_dropped_task_count_(), 1);
  delete t;

  worker.destroy();
}

TEST(RoutingTest, drive_write_all_invokes_each_ctx)
{
  LogAsyncIOWorker worker;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx_a(1), ctx_b(2), ctx_c(3);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(1, &ctx_a));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(2, &ctx_b));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(3, &ctx_c));

  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  EXPECT_EQ(1, ctx_a.get_drive_writes());
  EXPECT_EQ(1, ctx_b.get_drive_writes());
  EXPECT_EQ(1, ctx_c.get_drive_writes());
  EXPECT_EQ(INT64_MAX, next_drive_interval_us);

  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1));
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(2));
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(3));
  worker.destroy();
}

TEST(RoutingTest, drive_write_all_returns_min_drive_interval)
{
  LogAsyncIOWorker worker;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx_a(1), ctx_b(2), ctx_c(3);
  ctx_a.set_next_drive_interval(80);
  ctx_b.set_next_drive_interval(30);
  ctx_c.set_next_drive_interval(INT64_MAX);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(1, &ctx_a));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(2, &ctx_b));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(3, &ctx_c));

  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  EXPECT_EQ(30, next_drive_interval_us);

  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1));
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(2));
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(3));
  worker.destroy();
}

TEST(RoutingTest, drive_write_all_keeps_throttle_deadline)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(4);
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ctx.set_next_drive_interval(2 * 1000 * 1000);
  ctx.set_throttle_next_admit_ts(ObTimeUtility::fast_current_time() + 1000 * 1000);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(4, &ctx));

  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  EXPECT_GT(next_drive_interval_us, 0);
  EXPECT_LE(next_drive_interval_us, 1000 * 1000);

  ctx.set_throttle_next_admit_ts(ObTimeUtility::fast_current_time() - 1);
  next_drive_interval_us = INT64_MAX;
  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);

  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(4));
  worker.destroy();
}

TEST(RoutingTest, drive_write_all_returns_zero_for_immediate_ctx_work)
{
  LogAsyncIOWorker worker;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(301);
  FakeLogIOTask task(301);
  ctx.set_hold_pending(true);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(301, &ctx));
  ASSERT_EQ(OB_SUCCESS, worker.dispatch_task_(&task));

  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);
  EXPECT_EQ(1, ctx.get_drive_writes());

  ctx.set_hold_pending(false);
  next_drive_interval_us = 0;
  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  EXPECT_EQ(INT64_MAX, next_drive_interval_us);
  EXPECT_TRUE(ctx.is_drained());
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(301));
  worker.destroy();
}

TEST(RoutingTest, drive_write_all_keeps_min_interval_from_any_ctx)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx first_ctx(31);
  FakeAsyncPalfIOCtx second_ctx(32);
  first_ctx.set_next_drive_interval(0);
  second_ctx.set_next_drive_interval(30);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(31, &first_ctx));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(32, &second_ctx));

  int64_t next_drive_interval_us = INT64_MAX;
  EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
  EXPECT_EQ(1, first_ctx.get_drive_writes());
  EXPECT_EQ(1, second_ctx.get_drive_writes());
  EXPECT_EQ(0, next_drive_interval_us);

  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(31));
  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(32));
  worker.destroy();
}

TEST(RoutingTest, oldest_pending_io_start_ts_ignores_nonpositive_values)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx no_pending_ctx(41);
  FakeAsyncPalfIOCtx older_ctx(42);
  FakeAsyncPalfIOCtx oldest_ctx(43);
  no_pending_ctx.set_oldest_pending_io_start_ts(0);
  older_ctx.set_oldest_pending_io_start_ts(80);
  oldest_ctx.set_oldest_pending_io_start_ts(30);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(41, &no_pending_ctx));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(42, &older_ctx));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(43, &oldest_ctx));

  EXPECT_EQ(30, worker.get_oldest_pending_io_start_ts());
  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(43));
  EXPECT_EQ(80, worker.get_oldest_pending_io_start_ts());

  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(41));
  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(42));
  worker.destroy();
}

// ============================================================
// Lifecycle tests for LogAsyncIOWorker
// ============================================================

TEST(LifecycleTest, unregister_waits_for_inflight_zero)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(10);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(10, &ctx));
  ctx.set_inflight(3);

  std::atomic<bool> finished{false};
  std::thread t([&]() {
    int ret = worker.unregister_ctx_entry_(10);
    EXPECT_EQ(OB_SUCCESS, ret);
    finished.store(true);
  });

  ob_usleep(20 * 1000);
  EXPECT_FALSE(finished.load());

  // Simulate AIO completions via direct ctx call (direct ctx callback path).
  for (int i = 0; i < 3; ++i) {
    AsyncIOCompletionEvent ev = make_event(10);
    bool need_wake = false;
    EXPECT_EQ(OB_SUCCESS, ctx.on_aio_complete(ev, need_wake));
  }
  for (int i = 0; i < 1000 && !finished.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(finished.load());
  t.join();
  worker.destroy();
}

TEST(LifecycleTest, completion_during_unregister_still_consumed)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(20);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(20, &ctx));
  ctx.set_inflight(1);

  std::atomic<bool> done{false};
  std::thread t([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(20));
    done.store(true);
  });
  ob_usleep(10 * 1000);

  // Simulate AIO completion via direct ctx call (direct ctx callback path).
  AsyncIOCompletionEvent ev = make_event(20);
  bool need_wake = false;
  EXPECT_EQ(OB_SUCCESS, ctx.on_aio_complete(ev, need_wake));
  EXPECT_EQ(1, ctx.get_completions());

  for (int i = 0; i < 1000 && !done.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(done.load());
  t.join();
  worker.destroy();
}

TEST(LifecycleTest, register_after_unregister_succeeds)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx1(30);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(30, &ctx1));
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(30));

  FakeAsyncPalfIOCtx ctx2(30);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(30, &ctx2));
  EXPECT_EQ(1, worker.get_ctx_count_());

  FakeLogIOTask *t = new FakeLogIOTask(30);
  EXPECT_EQ(OB_SUCCESS, worker.dispatch_task_(t));
  EXPECT_EQ(1, ctx2.get_enqueued_tasks());
  delete t;

  drive_until_drained(worker, ctx2);

  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(30));
  worker.destroy();
}

TEST(LifecycleTest, unregister_missing_ctx_returns_not_exist)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, worker.unregister_ctx_entry_(42));
  worker.destroy();
}

TEST(LifecycleTest, wait_blocks_until_all_ctx_unregistered)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(43);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(43, &ctx));
  ASSERT_EQ(OB_SUCCESS, worker.start());
  worker.stop();

  std::atomic<bool> wait_finished{false};
  std::thread wait_thread([&]() {
    worker.wait();
    wait_finished.store(true);
  });
  ob_usleep(20 * 1000);
  EXPECT_FALSE(wait_finished.load());
  EXPECT_EQ(1, worker.get_ctx_count_());

  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(43));
  for (int64_t i = 0; i < 2000 && !wait_finished.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(wait_finished.load());
  wait_thread.join();
  EXPECT_EQ(0, worker.get_ctx_count_());
  worker.destroy();
}

TEST(LifecycleTest, public_state_access_waits_for_lifecycle_writer)
{
  LogAsyncIOWorker worker;
  AsyncThrottleContext throttle_ctx;
  std::atomic<int64_t> done_count(0);
  int register_ret = OB_SUCCESS;
  int unregister_ret = OB_SUCCESS;
  int wake_ret = OB_SUCCESS;
  std::thread register_thread;
  std::thread unregister_thread;
  std::thread wake_thread;
  std::thread wait_thread;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  {
    ObQSyncLockWriteGuard guard(worker.lifecycle_lock_);
    register_thread = std::thread([&]() {
      register_ret = worker.register_and_create_ctx(
          INVALID_PALF_ID, -1 /* cb_tg_id */, NULL /* palf_env_impl */, throttle_ctx);
      ++done_count;
    });
    unregister_thread = std::thread([&]() {
      unregister_ret = worker.unregister_palf_ctx_and_wait(1);
      ++done_count;
    });
    wake_thread = std::thread([&]() {
      wake_ret = worker.wake_up_for_drive();
      ++done_count;
    });
    wait_thread = std::thread([&]() {
      worker.wait();
      ++done_count;
    });
    ob_usleep(20 * 1000);
    EXPECT_EQ(0, done_count.load());
  }

  register_thread.join();
  unregister_thread.join();
  wake_thread.join();
  wait_thread.join();
  EXPECT_EQ(OB_INVALID_ARGUMENT, register_ret);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, unregister_ret);
  EXPECT_EQ(OB_NOT_RUNNING, wake_ret);
  EXPECT_EQ(4, done_count.load());
  worker.destroy();
}

TEST(LifecycleTest, unregister_retries_until_ctx_state_is_safe)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(44);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(44, &ctx));
  ctx.set_active_ref(-1);

  std::atomic<bool> unregister_finished{false};
  std::thread unregister_thread([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(44));
    unregister_finished.store(true);
  });
  ob_usleep(20 * 1000);
  EXPECT_FALSE(unregister_finished.load());
  EXPECT_EQ(1, worker.get_ctx_count_());

  ctx.set_active_ref(0);
  for (int64_t i = 0; i < 2000 && !unregister_finished.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(unregister_finished.load());
  unregister_thread.join();
  EXPECT_EQ(0, worker.get_ctx_count_());
  worker.destroy();
}

TEST(LifecycleTest, register_invalid_args_rejected)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(1);
  EXPECT_EQ(OB_INVALID_ARGUMENT, worker.register_ctx_entry_(-1, &ctx));
  EXPECT_EQ(OB_INVALID_ARGUMENT, worker.register_ctx_entry_(1, NULL));
  worker.destroy();
}

TEST(FaultInjectionTest, unregister_waits_until_inflight_drains_without_timeout)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  FakeAsyncPalfIOCtx ctx(50);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(50, &ctx));
  ctx.set_inflight(1);

  std::atomic<bool> done{false};
  std::thread t([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(50));
    done.store(true);
  });

  ob_usleep(150 * 1000);
  EXPECT_FALSE(done.load());
  EXPECT_EQ(1, worker.get_ctx_count_());
  ctx.set_inflight(0);
  for (int i = 0; i < 2000 && !done.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(done.load());
  t.join();
  EXPECT_EQ(0, worker.get_ctx_count_());

  FakeAsyncPalfIOCtx ctx2(50);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(50, &ctx2));
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(50));
  worker.destroy();
}

// ============================================================
// H-2: unregister vs worker drive concurrency (no re-hold / no UAF)
// ============================================================

// A ctx that lets tests hold a drive pass or pending-time query for as long as
// needed. drive_write() spins inside an entry/exit gate so we can interleave a
// concurrent unregister_palf_ctx_and_wait against an in-progress
// snapshot_entries_ -> guard -> drive_write pass. It also records whether
// any method touches it AFTER the test marks it "erased" (which models
// LogAsyncIOWorker::destroy_entry_() calling ctx->free_this()); a
// touch-after-erase is the UAF signature this test guards against.
class GatedAsyncPalfIOCtx : public IAsyncPalfIOCtx
{
public:
  explicit GatedAsyncPalfIOCtx(int64_t palf_id) : palf_id_(palf_id) {}
  ~GatedAsyncPalfIOCtx() override {}

  int64_t get_palf_id() const override { note_touch_(); return palf_id_; }
  int try_reserve_task_slot(const LogIOTaskType /*task_type*/) override
  { note_touch_(); reserved_task_count_.fetch_add(1); return OB_SUCCESS; }
  void release_task_slot(const LogIOTaskType /*task_type*/) override
  { note_touch_(); reserved_task_count_.fetch_sub(1); }
  int enqueue_task(LogIOTask *task) override
  {
    note_touch_();
    if (OB_NOT_NULL(task)) {
      release_task_slot(task->get_io_task_type());
    }
    return OB_SUCCESS;
  }
  int on_aio_complete(const AsyncIOCompletionEvent &,
                      bool &need_wake_worker) override
  { note_touch_(); need_wake_worker = false; return OB_SUCCESS; }
  int request_drive() override { note_touch_(); return OB_SUCCESS; }
  int64_t get_inflight_count() const override
  { note_touch_(); return reserved_task_count_.load() + inflight_.load(); }
  int64_t get_oldest_pending_io_start_ts() const override
  {
    note_touch_();
    oldest_query_entered_.store(true);
    while (hold_oldest_query_.load()) {
      ob_usleep(200);
    }
    return 123;
  }
  int64_t get_throttle_next_admit_ts() const override { note_touch_(); return 0; }
  void pin() override { note_touch_(); active_ref_.fetch_add(1); }
  void unpin() override { note_touch_(); active_ref_.fetch_sub(1); }
  int64_t get_active_ref() const override { note_touch_(); return active_ref_.load(); }

  int drive_write(int64_t &next_drive_interval_us) override
  {
    note_touch_();
    in_drive_.store(true);
    // Block until the test releases us, so the guard held by snapshot_entries_
    // stays active across a concurrent unregister attempt.
    while (!release_drive_.load()) {
      ob_usleep(200);
    }
    in_drive_.store(false);
    drive_writes_.fetch_add(1);
    next_drive_interval_us = INT64_MAX;
    return OB_SUCCESS;
  }
  bool is_drained() const override
  {
    note_touch_();
    return 0 == reserved_task_count_.load() && 0 == inflight_.load();
  }

  void free_this() override { note_touch_(); }

  void set_inflight(int64_t n) { inflight_.store(n); }
  void release_drive() { release_drive_.store(true); }
  void set_hold_oldest_query(const bool hold) { hold_oldest_query_.store(hold); }
  bool is_oldest_query_entered() const { return oldest_query_entered_.load(); }
  bool in_drive() const { return in_drive_.load(); }
  int64_t get_drive_writes() const { return drive_writes_.load(); }
  // Called after unregister erases the map entry and calls ctx->free_this().
  // The test object deliberately stays in memory, so any later method call is
  // recorded as the equivalent production use-after-free.
  void mark_erased() { erased_.store(true); }
  bool touched_after_erase() const { return touched_after_erase_.load(); }
  TO_STRING_KV(K_(palf_id),
               "inflight", inflight_.load(),
               "active_ref", active_ref_.load(),
               "reserved_task_count", reserved_task_count_.load(),
               "in_drive", in_drive_.load(),
               "release_drive", release_drive_.load(),
               "drive_writes", drive_writes_.load(),
               "erased", erased_.load(),
               "touched_after_erase", touched_after_erase_.load());

private:
  void note_touch_() const
  {
    if (erased_.load()) {
      touched_after_erase_.store(true);
    }
  }
  int64_t palf_id_;
  std::atomic<int64_t> inflight_{0};
  std::atomic<int64_t> active_ref_{0};
  std::atomic<int64_t> reserved_task_count_{0};
  std::atomic<bool> in_drive_{false};
  std::atomic<bool> release_drive_{false};
  std::atomic<int64_t> drive_writes_{0};
  mutable std::atomic<bool> hold_oldest_query_{false};
  mutable std::atomic<bool> oldest_query_entered_{false};
  mutable std::atomic<bool> erased_{false};
  mutable std::atomic<bool> touched_after_erase_{false};
};

TEST(H2ConcurrencyTest, oldest_pending_query_releases_lifecycle_lock_before_ctx_call)
{
  LogAsyncIOWorker worker;
  GatedAsyncPalfIOCtx ctx(302);
  std::atomic<bool> query_done(false);
  std::atomic<bool> stop_done(false);
  int64_t oldest_ts = OB_INVALID_TIMESTAMP;
  bool query_entered = false;
  bool stop_finished_before_query_release = false;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(302, &ctx));
  ctx.set_hold_oldest_query(true);

  std::thread query_thread([&]() {
    oldest_ts = worker.get_oldest_pending_io_start_ts();
    query_done.store(true);
  });
  for (int64_t i = 0; i < 2000 && !query_entered; ++i) {
    query_entered = ctx.is_oldest_query_entered();
    if (!query_entered) {
      ob_usleep(1000);
    }
  }
  EXPECT_TRUE(query_entered);
  EXPECT_FALSE(query_done.load());

  std::thread stop_thread([&]() {
    worker.stop();
    stop_done.store(true);
  });
  for (int64_t i = 0; i < 2000 && !stop_done.load(); ++i) {
    ob_usleep(1000);
  }
  stop_finished_before_query_release = stop_done.load();
  ctx.set_hold_oldest_query(false);
  query_thread.join();
  stop_thread.join();

  EXPECT_TRUE(stop_finished_before_query_release);
  EXPECT_TRUE(query_done.load());
  EXPECT_EQ(123, oldest_ts);
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(302));
  worker.destroy();
}

struct DriveWriteAllThread
{
  DriveWriteAllThread(LogAsyncIOWorker *worker, std::atomic<bool> *done, int *ret)
    : worker_(worker), done_(done), ret_(ret)
  {}
  void operator()()
  {
    int64_t next_drive_interval_us = INT64_MAX;
    *ret_ = worker_->drive_write_all_(next_drive_interval_us);
    done_->store(true);
  }
  LogAsyncIOWorker *worker_;
  std::atomic<bool> *done_;
  int *ret_;
};

struct RegisterCtxThread
{
  RegisterCtxThread(LogAsyncIOWorker *worker,
                    const int64_t palf_id,
                    IAsyncPalfIOCtx *ctx,
                    std::atomic<bool> *done,
                    int *ret)
    : worker_(worker),
      palf_id_(palf_id),
      ctx_(ctx),
      done_(done),
      ret_(ret)
  {}
  void operator()()
  {
    *ret_ = worker_->register_ctx_entry_(palf_id_, ctx_);
    done_->store(true);
  }
  LogAsyncIOWorker *worker_;
  int64_t palf_id_;
  IAsyncPalfIOCtx *ctx_;
  std::atomic<bool> *done_;
  int *ret_;
};

TEST(H2ConcurrencyTest, drive_write_runs_outside_ctx_map_bucket_lock)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  GatedAsyncPalfIOCtx ctx(301);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(301, &ctx));

  std::atomic<bool> drive_done{false};
  int drive_ret = OB_SUCCESS;
  DriveWriteAllThread drive_op(&worker, &drive_done, &drive_ret);
  std::thread drive_thread(drive_op);
  for (int i = 0; i < 5000 && !ctx.in_drive(); ++i) {
    ob_usleep(200);
  }
  EXPECT_TRUE(ctx.in_drive());
  int64_t active_ref = 0;
  ASSERT_EQ(OB_SUCCESS, get_entry_active_ref(worker, 301, active_ref));
  EXPECT_GE(active_ref, 1);

  FakeAsyncPalfIOCtx ctx2(305);
  std::atomic<bool> register_done{false};
  int register_ret = OB_SUCCESS;
  RegisterCtxThread register_op(&worker, 305, &ctx2, &register_done, &register_ret);
  std::thread register_thread(register_op);
  for (int i = 0; i < 250 && !register_done.load(); ++i) {
    ob_usleep(200);
  }
  const bool registered_before_drive_release = register_done.load();
  EXPECT_TRUE(registered_before_drive_release);

  ctx.release_drive();
  for (int i = 0; i < 5000 && (!drive_done.load() || !register_done.load()); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(drive_done.load());
  EXPECT_TRUE(register_done.load());
  drive_thread.join();
  register_thread.join();

  EXPECT_EQ(OB_SUCCESS, drive_ret);
  EXPECT_EQ(OB_SUCCESS, register_ret);
  EXPECT_EQ(1, ctx.get_drive_writes());
  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(301));
  if (OB_SUCCESS == register_ret) {
    ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(305));
  }
  worker.destroy();
}

// unregister must NOT erase the ctx while a concurrent worker drive pass still
// holds the map-callback entry guard (active_ref > 0). The worker's authoritative
// zero-count recheck and erase happen in erase_if(), and snapshot_entries_ holds
// each entry in foreach_refactored(), so unregister cannot erase it until the
// guard drops. Verifies: (1) unregister blocks during drive_write, (2) erase
// completes after the guard is released, (3) ctx is never touched after erase
// (no UAF).
TEST(H2ConcurrencyTest, unregister_blocks_until_worker_entry_guard_drains)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  GatedAsyncPalfIOCtx ctx(101);
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(101, &ctx));

  // Worker thread runs one drive pass that holds the entry through
  // snapshot_entries_, then blocks inside drive_write().
  std::atomic<bool> drive_done{false};
  std::thread drive_thread([&]() {
    int64_t next_drive_interval_us = INT64_MAX;
    EXPECT_EQ(OB_SUCCESS, worker.drive_write_all_(next_drive_interval_us));
    drive_done.store(true);
  });
  // Wait until the worker is parked inside drive_write() with the entry held.
  for (int i = 0; i < 5000 && !ctx.in_drive(); ++i) {
    ob_usleep(200);
  }
  ASSERT_TRUE(ctx.in_drive());
  int64_t active_ref = 0;
  ASSERT_EQ(OB_SUCCESS, get_entry_active_ref(worker, 101, active_ref));
  ASSERT_GE(active_ref, 1);

  // Kick off unregister concurrently. It must NOT erase while the entry is held.
  std::atomic<bool> unreg_done{false};
  std::thread unreg([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(101));
    // Erase has completed: from here the owner could free ctx. Mark it so any
    // post-erase deref by the worker is caught as a UAF.
    ctx.mark_erased();
    unreg_done.store(true);
  });

  // Give unregister time to attempt the erase; it must still be blocked because
  // the worker holds the entry guard.
  ob_usleep(50 * 1000);
  EXPECT_FALSE(unreg_done.load());
  EXPECT_EQ(1, worker.get_ctx_count_());
  EXPECT_EQ(OB_STATE_NOT_MATCH, worker.unregister_palf_ctx_and_wait(101));

  // Release the worker's drive pass so unregister can finally erase the entry.
  ctx.release_drive();

  for (int i = 0; i < 5000 && (!unreg_done.load() || !drive_done.load()); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(drive_done.load());
  EXPECT_TRUE(unreg_done.load());
  EXPECT_EQ(0, worker.get_ctx_count_());
  EXPECT_EQ(1, ctx.get_drive_writes());
  // No method was invoked on ctx after the erase: no new guard, no dangling deref.
  EXPECT_FALSE(ctx.touched_after_erase());

  drive_thread.join();
  unreg.join();
  worker.destroy();
}

// A new drive pass that starts after unregister begins must still be safe:
// snapshot_entries_ either holds-and-the-erase-waits, or the entry is already gone.
// Hammer drive_write_all() on one thread while unregister runs on another; the
// ctx must never be touched after erase even across many interleavings.
TEST(H2ConcurrencyTest, repeated_drive_during_unregister_no_use_after_free)
{
  for (int iter = 0; iter < 20; ++iter) {
    LogAsyncIOWorker worker;
    ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
    GatedAsyncPalfIOCtx ctx(202);
    ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(202, &ctx));
    // This ctx variant does not block in drive_write (release pre-armed) so the
    // guard is taken and dropped quickly, maximizing interleaving with erase.
    ctx.release_drive();

    std::atomic<bool> stop{false};
    std::thread driver([&]() {
      int64_t next_drive_interval_us = INT64_MAX;
      while (!stop.load()) {
        next_drive_interval_us = INT64_MAX;
        (void) worker.drive_write_all_(next_drive_interval_us);
      }
    });

    for (int64_t i = 0; i < 5000 && 0 == ctx.get_drive_writes(); ++i) {
      ob_usleep(200);
    }
    EXPECT_GT(ctx.get_drive_writes(), 0);

    EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(202));
    // Erase done: any further deref of ctx by the driver is a UAF.
    ctx.mark_erased();
    // Let the driver spin a bit more against the now-erased map.
    ob_usleep(5 * 1000);
    stop.store(true);
    driver.join();

    EXPECT_EQ(0, worker.get_ctx_count_());
    EXPECT_FALSE(ctx.touched_after_erase());
    worker.destroy();
  }
}

// ============================================================
// Worker tests (LogAsyncIOWorker)
// ============================================================

TEST(WorkerTest, init_destroy)
{
  LogAsyncIOWorker worker;
  EXPECT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  EXPECT_EQ(OB_INIT_TWICE, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  EXPECT_TRUE(worker.is_valid());
  worker.destroy();
}

TEST(WorkerTest, register_ctx_failure_keeps_existing_entry)
{
  const int64_t palf_id = 1001;
  share::ObTenantBase tenant_base(OB_SERVER_TENANT_ID);
  share::ObTenantSwitchGuard tenant_guard(&tenant_base);
  LogAsyncIOWorker worker;
  FakePalfEnvImpl fake_env;
  FakeAsyncPalfIOCtx existing_ctx(palf_id);
  AsyncThrottleContext throttle_ctx;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(palf_id, &existing_ctx));
  EXPECT_EQ(OB_ENTRY_EXIST,
            worker.register_and_create_ctx(palf_id, 1 /* cb_tg_id */,
                                           &fake_env,
                                           throttle_ctx));
  EXPECT_EQ(1, worker.get_ctx_count_());
  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(palf_id));
  worker.destroy();
}

TEST(WorkerTest, submit_before_start_is_rejected)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  FakeLogIOTask *t = new FakeLogIOTask(1);
  EXPECT_EQ(OB_NOT_RUNNING, worker.submit_io_task(t));
  EXPECT_EQ(0, worker.diagnostics_.get_dropped_submit_count());

  EXPECT_EQ(OB_NOT_RUNNING, worker.wake_up_for_drive());

  delete t;
  worker.destroy();
}

TEST(WorkerTest, task_reservation_failure_keeps_caller_ownership)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 8));
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  worker.is_running_ = true;
  ctx.set_task_reserve_ret(OB_EAGAIN);
  TypedFakeLogIOTask task(1, LogIOTaskType::TRUNCATE_LOG_TYPE);

  EXPECT_EQ(OB_EAGAIN, worker.submit_io_task(&task));
  EXPECT_EQ(1, ctx.get_task_reserve_count());
  EXPECT_EQ(0, ctx.get_task_release_count());
  EXPECT_EQ(0, ctx.get_reserved_task_count());

  worker.is_running_ = false;
  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.destroy();
}

TEST(WorkerTest, unregistering_entry_rejects_task_before_reservation)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 8));
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  AsyncPalfIOCtxEntryGuard entry_guard;
  // The test needs lifecycle access to install the unregistering state.
  ASSERT_EQ(OB_SUCCESS,
            worker.get_entry_guard_(1, true /* allow_unregistering */, entry_guard));
  entry_guard.get_entry()->set_unregistering();
  entry_guard.reset();
  worker.is_running_ = true;
  TypedFakeLogIOTask task(1, LogIOTaskType::TRUNCATE_LOG_TYPE);

  EXPECT_EQ(OB_STATE_NOT_MATCH, worker.submit_io_task(&task));
  EXPECT_EQ(0, ctx.get_task_reserve_count());
  EXPECT_EQ(0, ctx.get_task_release_count());
  EXPECT_EQ(0, ctx.get_reserved_task_count());

  worker.is_running_ = false;
  EXPECT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1));
  worker.destroy();
}

TEST(WorkerTest, admission_ignores_task_epoch)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(1);
  FakeLogIOTask task(1, 2 /* task_epoch */);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 8));
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  worker.is_running_ = true;

  ASSERT_EQ(OB_SUCCESS, worker.submit_io_task(&task));
  EXPECT_EQ(1, ctx.get_task_reserve_count());
  EXPECT_EQ(1, ctx.get_reserved_task_count());
  void *raw = NULL;
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.pop(raw, 0));
  LogIOTask *queued_task = static_cast<LogIOTask *>(raw);
  ASSERT_EQ(&task, queued_task);
  ASSERT_EQ(OB_SUCCESS, worker.handle_queued_log_io_task_(queued_task));
  EXPECT_TRUE(OB_ISNULL(queued_task));
  EXPECT_EQ(1, ctx.get_task_release_count());
  EXPECT_EQ(0, ctx.get_reserved_task_count());
  drive_until_drained(worker, ctx);

  worker.is_running_ = false;
  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.destroy();
}

TEST(WorkerTest, queue_overflow_releases_control_task_slot)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 1));
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  worker.is_running_ = true;
  FakeLogIOTask filler(1);
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(&filler));
  TypedFakeLogIOTask task(1, LogIOTaskType::TRUNCATE_LOG_TYPE);

  EXPECT_NE(OB_SUCCESS, worker.submit_io_task(&task));
  EXPECT_EQ(1, ctx.get_task_reserve_count());
  EXPECT_EQ(1, ctx.get_task_release_count());
  EXPECT_EQ(0, ctx.get_reserved_task_count());
  EXPECT_EQ(1, worker.diagnostics_.get_dropped_submit_count());

  void *raw = NULL;
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.pop(raw, 0));
  EXPECT_EQ(&filler, raw);
  worker.is_running_ = false;
  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.destroy();
}

TEST(WorkerTest, flush_queue_overflow_releases_task_slot)
{
  LogAsyncIOWorker worker;
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 1));
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  worker.is_running_ = true;
  FakeLogIOTask filler(1);
  FakeLogIOTask task(1);
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(&filler));

  EXPECT_NE(OB_SUCCESS, worker.submit_io_task(&task));
  EXPECT_EQ(1, ctx.get_task_reserve_count());
  EXPECT_EQ(1, ctx.get_task_release_count());
  EXPECT_EQ(0, ctx.get_reserved_task_count());

  void *raw = NULL;
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.pop(raw, 0));
  EXPECT_EQ(&filler, raw);
  worker.is_running_ = false;
  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.destroy();
}

TEST(WorkerTest, drive_mark_queue_failure_can_rearm)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 1));
  worker.is_running_ = true;
  FakeLogIOTask filler(1);
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(&filler));

  EXPECT_NE(OB_SUCCESS, worker.wake_up_for_drive());
  EXPECT_EQ(0, ATOMIC_LOAD(&worker.drive_pending_));
  void *raw = NULL;
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.pop(raw, 0));
  EXPECT_EQ(&filler, raw);

  ASSERT_EQ(OB_SUCCESS, worker.wake_up_for_drive());
  EXPECT_EQ(1, ATOMIC_LOAD(&worker.drive_pending_));
  EXPECT_EQ(1, worker.input_queue_.size());
  ASSERT_EQ(OB_SUCCESS, worker.wake_up_for_drive());
  EXPECT_EQ(1, worker.input_queue_.size());
  EXPECT_EQ(1, worker.diagnostics_.get_drive_wake_count());
  EXPECT_EQ(1, worker.diagnostics_.get_coalesced_drive_wake_count());

  raw = NULL;
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.pop(raw, 0));
  LogIOTask *mark_task = static_cast<LogIOTask *>(raw);
  EXPECT_EQ(&worker.drive_mark_task_, mark_task);
  ASSERT_TRUE(worker.is_async_mark_task_(mark_task));
  ATOMIC_STORE(&worker.drive_pending_, 0);
  EXPECT_EQ(OB_SUCCESS, worker.wake_up_for_drive());
  EXPECT_EQ(1, worker.input_queue_.size());

  raw = NULL;
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.pop(raw, 0));
  mark_task = static_cast<LogIOTask *>(raw);
  EXPECT_EQ(&worker.drive_mark_task_, mark_task);
  ATOMIC_STORE(&worker.drive_pending_, 0);
  worker.is_running_ = false;
  worker.destroy();
}

TEST(WorkerTest, drive_mark_rearms_while_previous_mark_is_driving)
{
  LogAsyncIOWorker worker;
  GatedAsyncPalfIOCtx ctx(11);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 8));
  ASSERT_EQ(OB_SUCCESS, worker.register_ctx_entry_(11, &ctx));
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(&worker.drive_mark_task_));
  ATOMIC_STORE(&worker.drive_pending_, 1);
  ASSERT_EQ(OB_SUCCESS, worker.start());

  for (int64_t i = 0; i < 5000 && !ctx.in_drive(); ++i) {
    ob_usleep(200);
  }
  const bool drive_entered = ctx.in_drive();
  EXPECT_TRUE(drive_entered);
  if (drive_entered) {
    EXPECT_EQ(0, ATOMIC_LOAD(&worker.drive_pending_));
    EXPECT_EQ(OB_SUCCESS, worker.wake_up_for_drive());
    EXPECT_EQ(1, ATOMIC_LOAD(&worker.drive_pending_));
  }

  ctx.release_drive();
  for (int64_t i = 0; i < 5000 && ctx.get_drive_writes() < 2; ++i) {
    ob_usleep(200);
  }
  EXPECT_GE(ctx.get_drive_writes(), 2);
  worker.stop();
  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(11));
  worker.wait();
  worker.destroy();
}

TEST(WorkerTest, write_loop_dispatches_tasks_to_ctx)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));

  ASSERT_EQ(OB_SUCCESS, worker.start());

  FakeLogIOTask *t = new FakeLogIOTask(1);
  EXPECT_EQ(OB_SUCCESS, worker.submit_io_task(t));

  for (int i = 0;
       i < 2000 && (ctx.get_enqueued_tasks() == 0
                    || worker.diagnostics_.get_handled_task_count() == 0
                    || ctx.get_drive_writes() == 0);
       ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, ctx.get_enqueued_tasks());
  EXPECT_EQ(1, worker.diagnostics_.get_submitted_task_count());
  EXPECT_EQ(1, worker.diagnostics_.get_handled_task_count());
  EXPECT_GT(ctx.get_drive_writes(), 0);

  worker.stop();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.wait();
  delete t;
  worker.destroy();
}

TEST(WorkerTest, write_loop_drains_ready_tasks_before_drive)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));

  FakeLogIOTask *t1 = new FakeLogIOTask(1);
  FakeLogIOTask *t2 = new FakeLogIOTask(1);
  EXPECT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(t1->get_io_task_type()));
  EXPECT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(t2->get_io_task_type()));
  EXPECT_EQ(OB_SUCCESS, worker.input_queue_.push(t1));
  EXPECT_EQ(OB_SUCCESS, worker.input_queue_.push(t2));

  ASSERT_EQ(OB_SUCCESS, worker.start());
  for (int i = 0; i < 2000 && ctx.get_drive_writes() == 0; ++i) {
    ob_usleep(1000);
  }
  EXPECT_GT(ctx.get_drive_writes(), 0);
  EXPECT_EQ(2, ctx.get_enqueued_tasks());
  EXPECT_EQ(2, ctx.get_first_drive_enqueued_tasks());

  worker.stop();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.wait();
  delete t1;
  delete t2;
  worker.destroy();
}

TEST(WorkerTest, unregister_missing_entry_returns_not_exist)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(1);
  FakeAsyncPalfIOCtx retry_ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));

  ASSERT_EQ(OB_SUCCESS, worker.unregister_ctx_entry_(1));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, worker.unregister_palf_ctx_and_wait(1));
  EXPECT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, retry_ctx));
  EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));

  worker.destroy();
}

TEST(WorkerTest, direct_completion_publishes_inline)
{
  // Simulate the callback thread calling the ctx directly. The ctx decides
  // whether deferred work requires another worker drive.
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(2);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 2, ctx));
  ctx.set_inflight(2);
  ctx.set_need_wake(true);

  ASSERT_EQ(OB_SUCCESS, worker.start());

  // Simulate direct completions via direct ctx callback path.
  AsyncIOCompletionEvent ev1 = make_event(2, 0, 1);
  AsyncIOCompletionEvent ev2 = make_event(2, 1, 1);
  bool need_wake = false;
  EXPECT_EQ(OB_SUCCESS, ctx.on_aio_complete(ev1, need_wake));
  EXPECT_TRUE(need_wake);
  need_wake = false;
  EXPECT_EQ(OB_SUCCESS, ctx.on_aio_complete(ev2, need_wake));
  EXPECT_TRUE(need_wake);

  EXPECT_EQ(2, ctx.get_completions());
  // The worker drives once after a non-blocking queue miss, before entering
  // the blocking queue wait path.
  for (int i = 0; i < 2000 && ctx.get_drive_writes() == 0; ++i) {
    ob_usleep(1000);
  }
  EXPECT_GT(ctx.get_drive_writes(), 0);

  worker.stop();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(2));
  worker.wait();
  worker.destroy();
}

TEST(WorkerTest, wake_up_for_drive_coalesces)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  // Set the pending flag manually so the queue stays cold.
  ATOMIC_STORE(&worker.drive_pending_, 1);
  // start to flip is_running_ true (we won't actually drain).
  ASSERT_EQ(OB_SUCCESS, worker.start());
  // wake_up_for_drive must NOT push another ASYNC_MARK while drive_pending_ is 1.
  EXPECT_EQ(OB_SUCCESS, worker.wake_up_for_drive());
  EXPECT_EQ(OB_SUCCESS, worker.wake_up_for_drive());
  // drive_wake_count must remain 0 because we lost the CAS each time.
  EXPECT_EQ(0, worker.diagnostics_.get_drive_wake_count());
  worker.stop();
  worker.wait();
  worker.destroy();
}

TEST(WorkerTest, async_mark_task_uses_log_io_task_type)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  LogIOTask *mark_task = &worker.drive_mark_task_;
  EXPECT_TRUE(worker.is_async_mark_task_(mark_task));
  EXPECT_EQ(LogIOTaskType::ASYNC_MARK_TYPE, mark_task->get_io_task_type());

  worker.destroy();
}

TEST(WorkerTest, async_mark_drives_before_following_tasks)
{
  LogAsyncIOWorker worker;
  LogIOTask *mark_task = NULL;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));

  FakeLogIOTask *t1 = new FakeLogIOTask(1);
  FakeLogIOTask *t2 = new FakeLogIOTask(1);
  mark_task = &worker.drive_mark_task_;
  ASSERT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(t1->get_io_task_type()));
  ASSERT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(t2->get_io_task_type()));
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(mark_task));
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(t1));
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(t2));

  ASSERT_EQ(OB_SUCCESS, worker.start());
  for (int i = 0; i < 2000 && ctx.get_drive_writes() == 0; ++i) {
    ob_usleep(1000);
  }
  EXPECT_GT(ctx.get_drive_writes(), 0);
  EXPECT_EQ(0, ctx.get_first_drive_enqueued_tasks());

  worker.stop();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.wait();
  delete t1;
  delete t2;
  worker.destroy();
}

TEST(WorkerTest, write_loop_drives_during_long_dispatch_batch)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 512));
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  std::vector<FakeLogIOTask *> tasks;
  FakeLogIOTask *task = NULL;

  for (int64_t i = 0; i < 257; ++i) {
    task = new FakeLogIOTask(1);
    ASSERT_TRUE(NULL != task);
    tasks.push_back(task);
    ASSERT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(task->get_io_task_type()));
    ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(task));
    task = NULL;
  }

  ASSERT_EQ(OB_SUCCESS, worker.start());
  for (int i = 0; i < 2000 && ctx.get_drive_writes() == 0; ++i) {
    ob_usleep(1000);
  }
  EXPECT_GT(ctx.get_drive_writes(), 0);
  EXPECT_EQ(256, ctx.get_first_drive_enqueued_tasks());

  worker.stop();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.wait();
  for (int64_t i = 0; i < static_cast<int64_t>(tasks.size()); ++i) {
    delete tasks.at(i);
  }
  worker.destroy();
}

TEST(WorkerTest, stop_rejects_new_submit_and_wait_returns)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.start());
  ob_usleep(20 * 1000);
  worker.stop();
  worker.wait();
  EXPECT_FALSE(worker.is_running_);

  FakeLogIOTask *t = new FakeLogIOTask(1);
  EXPECT_EQ(OB_NOT_RUNNING, worker.submit_io_task(t));
  delete t;

  worker.destroy();
}

TEST(WorkerTest, stop_waits_until_dispatched_ctx_tasks_are_drained)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(1);
  ctx.set_hold_pending(true);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  ASSERT_EQ(OB_SUCCESS, worker.start());

  FakeLogIOTask *task = new FakeLogIOTask(1);
  EXPECT_EQ(OB_SUCCESS, worker.submit_io_task(task));
  for (int i = 0; i < 3000 && ctx.get_enqueued_tasks() == 0; ++i) {
    ob_usleep(1000);
  }
  ASSERT_EQ(1, ctx.get_enqueued_tasks());
  ASSERT_EQ(1, ctx.get_pending_tasks());

  std::atomic<bool> wait_finished{false};
  std::thread stopper([&]() {
    worker.stop();
    EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
    worker.wait();
    wait_finished.store(true);
  });

  ob_usleep(30 * 1000);
  EXPECT_FALSE(wait_finished.load());
  EXPECT_GT(ctx.get_drive_writes(), 0);

  ctx.set_hold_pending(false);
  for (int i = 0; i < 3000 && !wait_finished.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(wait_finished.load());
  stopper.join();
  EXPECT_TRUE(ctx.is_drained());

  delete task;
  worker.destroy();
}

TEST(WorkerTest, submit_rejects_task_after_unregister_starts)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(2);
  ctx.set_hold_pending(true);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 2, ctx));
  ASSERT_EQ(OB_SUCCESS, worker.start());

  FakeLogIOTask *first_task = new FakeLogIOTask(2);
  EXPECT_EQ(OB_SUCCESS, worker.submit_io_task(first_task));
  for (int i = 0; i < 3000 && ctx.get_enqueued_tasks() == 0; ++i) {
    ob_usleep(1000);
  }
  ASSERT_EQ(1, ctx.get_enqueued_tasks());

  std::atomic<bool> unreg_done{false};
  std::thread unreg([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(2));
    unreg_done.store(true);
  });

  bool unregister_waiting = false;
  AsyncPalfIOCtxEntryGuard entry_guard;
  for (int64_t i = 0; i < 3000 && !unregister_waiting; ++i) {
    // Observe the unregistering transition while the unregister thread waits.
    if (OB_SUCCESS == worker.get_entry_guard_(
                          2, true /* allow_unregistering */, entry_guard)) {
      unregister_waiting = entry_guard.get_entry()->is_unregistering();
      entry_guard.reset();
    }
    if (!unregister_waiting) {
      ob_usleep(1000);
    }
  }
  EXPECT_TRUE(unregister_waiting);
  EXPECT_FALSE(unreg_done.load());

  const int64_t reserve_count = ctx.get_task_reserve_count();
  FakeLogIOTask late_task(2);
  EXPECT_EQ(OB_STATE_NOT_MATCH, worker.submit_io_task(&late_task));
  EXPECT_EQ(1, ctx.get_enqueued_tasks());
  EXPECT_EQ(reserve_count, ctx.get_task_reserve_count());
  EXPECT_EQ(0, ctx.get_reserved_task_count());

  ctx.set_hold_pending(false);
  for (int i = 0; i < 3000 && !unreg_done.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(unreg_done.load());

  worker.stop();
  worker.wait();
  unreg.join();
  delete first_task;
  worker.destroy();
}

TEST(WorkerTest, unregister_waits_for_reserved_queue_task_before_ctx_unregister)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(3);
  ctx.set_hold_pending(true);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 3, ctx));

  FakeLogIOTask *reserved_task = new FakeLogIOTask(3);
  ASSERT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(reserved_task->get_io_task_type()));
  ASSERT_EQ(OB_SUCCESS, worker.input_queue_.push(reserved_task));

  std::atomic<bool> unreg_done{false};
  std::thread unreg([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(3));
    unreg_done.store(true);
  });

  ob_usleep(30 * 1000);
  EXPECT_FALSE(unreg_done.load());
  ASSERT_EQ(OB_SUCCESS, worker.start());
  for (int i = 0; i < 3000 && ctx.get_enqueued_tasks() == 0; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, ctx.get_enqueued_tasks());

  FakeLogIOTask *late_task = new FakeLogIOTask(3);
  EXPECT_EQ(OB_STATE_NOT_MATCH, worker.submit_io_task(late_task));
  delete late_task;

  ctx.set_hold_pending(false);
  for (int i = 0; i < 3000 && !unreg_done.load(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(unreg_done.load());
  unreg.join();

  worker.stop();
  worker.wait();
  delete reserved_task;
  worker.destroy();
}

TEST(WorkerTest, stop_wait_clears_input_queue)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, worker.start());
  ob_usleep(20 * 1000);
  worker.stop();
  worker.wait();
  // The worker loop exits only after the accepted input queue is empty.
  EXPECT_EQ(0, worker.input_queue_.size());
  worker.destroy();
}

TEST(WorkerTest, wake_after_stop_does_not_drive_inline)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));

  ASSERT_EQ(OB_SUCCESS, worker.start());
  worker.stop();

  const int64_t before_wake_drive_count = ctx.get_drive_writes();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.wait();
  EXPECT_EQ(OB_NOT_RUNNING, worker.wake_up_for_drive());
  EXPECT_EQ(before_wake_drive_count, ctx.get_drive_writes());

  worker.destroy();
}

// ============================================================
// Task leak fix tests
// ============================================================

class TrackableLogIOTask : public LogIOTask
{
public:
  explicit TrackableLogIOTask(int64_t palf_id,
                              int do_task_ret = OB_SUCCESS,
                              LogIOTaskType type = LogIOTaskType::FLUSH_LOG_TYPE)
    : LogIOTask(palf_id, 1),
      do_task_ret_(do_task_ret),
      type_(type),
      freed_(false)
  {}
  ~TrackableLogIOTask() override {}
  int do_task_(int, IPalfHandleImplGuard &) override { return do_task_ret_; }
  int after_consume_(IPalfHandleImplGuard &) override { return OB_SUCCESS; }
  LogIOTaskType get_io_task_type_() const override { return type_; }
  void free_this_(IPalfEnvImpl *) override { freed_ = true; }
  int64_t get_io_size_() const override { return 0; }
  bool need_purge_throttling_() const override
  { return type_ == LogIOTaskType::PURGE_THROTTLING_TYPE; }

  bool is_freed() const { return freed_; }

private:
  int do_task_ret_;
  LogIOTaskType type_;
  bool freed_;
};

TEST(WorkerTest, missing_ctx_rejected_before_worker_owns_task)
{
  LogAsyncIOWorker worker;
  // Use a mock "palf_env" (just needs to be non-null for free_this routing;
  // TrackableLogIOTask::free_this_ ignores it).
  IPalfEnvImpl *fake_env = reinterpret_cast<IPalfEnvImpl *>(0xDEAD);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, fake_env, 64));
  ASSERT_EQ(OB_SUCCESS, worker.start());

  TrackableLogIOTask task(999);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, worker.submit_io_task(&task));
  EXPECT_FALSE(task.is_freed());
  EXPECT_EQ(1, worker.diagnostics_.get_dropped_submit_count());

  worker.stop();
  worker.wait();
  worker.destroy();
}

TEST(WorkerTest, stop_wait_dispatches_accepted_queued_tasks)
{
  LogAsyncIOWorker worker;
  IPalfEnvImpl *fake_env = reinterpret_cast<IPalfEnvImpl *>(0xDEAD);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, fake_env, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  TrackableLogIOTask t1(1), t2(1);

  // Seed accepted queue entries before start so stop races cannot hide this
  // path. The worker must dispatch them before wait() returns.
  EXPECT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(t1.get_io_task_type()));
  EXPECT_EQ(OB_SUCCESS, ctx.try_reserve_task_slot(t2.get_io_task_type()));
  EXPECT_EQ(OB_SUCCESS, worker.input_queue_.push(&t1));
  EXPECT_EQ(OB_SUCCESS, worker.input_queue_.push(&t2));
  ASSERT_EQ(OB_SUCCESS, worker.start());
  worker.stop();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.wait();

  EXPECT_FALSE(t1.is_freed());
  EXPECT_FALSE(t2.is_freed());
  EXPECT_EQ(2, ctx.get_enqueued_tasks());
  EXPECT_GT(ctx.get_drive_writes(), 0);
  worker.destroy();
}

TEST(WorkerTest, dispatch_failure_keeps_accepted_task_for_retry)
{
  LogAsyncIOWorker worker;
  IPalfEnvImpl *fake_env = reinterpret_cast<IPalfEnvImpl *>(0xDEAD);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, fake_env, 64));
  FakeAsyncPalfIOCtx ctx(1);
  ctx.set_enqueue_ret(OB_EAGAIN);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  ASSERT_EQ(OB_SUCCESS, worker.start());

  TrackableLogIOTask task(1);
  ASSERT_EQ(OB_SUCCESS, worker.submit_io_task(&task));
  for (int64_t i = 0;
       i < 2000 && OB_ISNULL(ATOMIC_LOAD(&worker.pending_dispatch_task_));
       i++) {
    ob_usleep(1000);
  }
  EXPECT_EQ(&task, ATOMIC_LOAD(&worker.pending_dispatch_task_));
  EXPECT_FALSE(task.is_freed());
  EXPECT_EQ(1, ctx.get_reserved_task_count());
  EXPECT_EQ(0, ctx.get_task_release_count());

  std::atomic<bool> wait_done{false};
  std::thread wait_thread([&]() {
    worker.stop();
    EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
    worker.wait();
    wait_done.store(true);
  });
  ob_usleep(50 * 1000);
  EXPECT_FALSE(wait_done.load());
  EXPECT_FALSE(task.is_freed());

  ctx.set_enqueue_ret(OB_SUCCESS);
  for (int64_t i = 0; i < 2000 && !wait_done.load(); i++) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(wait_done.load());
  wait_thread.join();
  EXPECT_TRUE(OB_ISNULL(ATOMIC_LOAD(&worker.pending_dispatch_task_)));
  EXPECT_EQ(1, ctx.get_enqueued_tasks());
  EXPECT_EQ(0, ctx.get_reserved_task_count());
  EXPECT_EQ(1, ctx.get_task_release_count());
  EXPECT_FALSE(task.is_freed());
  worker.destroy();
}

TEST(WorkerTest, stop_waits_for_submit_before_queue_push)
{
  LogAsyncIOWorker worker;
  IPalfEnvImpl *fake_env = reinterpret_cast<IPalfEnvImpl *>(0xDEAD);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, fake_env, 64));
  FakeAsyncPalfIOCtx ctx(1);
  ctx.set_hold_task_reserve(true);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  ASSERT_EQ(OB_SUCCESS, worker.start());

  TrackableLogIOTask task(1, OB_SUCCESS, LogIOTaskType::TRUNCATE_LOG_TYPE);
  int submit_ret = OB_SUCCESS;
  std::thread submit_thread([&]() { submit_ret = worker.submit_io_task(&task); });
  for (int64_t i = 0; i < 2000 && !ctx.is_task_reserve_entered(); i++) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(ctx.is_task_reserve_entered());

  std::atomic<bool> stop_done{false};
  std::atomic<bool> wait_done{false};
  std::thread wait_thread([&]() {
    // stop() takes the write side of the lifecycle lock and cannot pass the
    // submit read-side guard until task reservation returns.
    worker.stop();
    stop_done.store(true);
    EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
    worker.wait();
    wait_done.store(true);
  });
  ob_usleep(50 * 1000);
  EXPECT_FALSE(stop_done.load());
  EXPECT_FALSE(wait_done.load());

  ctx.set_hold_task_reserve(false);
  submit_thread.join();
  EXPECT_EQ(OB_SUCCESS, submit_ret);
  for (int64_t i = 0; i < 2000 && !wait_done.load(); i++) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(wait_done.load());
  wait_thread.join();
  EXPECT_TRUE(stop_done.load());
  EXPECT_EQ(1, ctx.get_enqueued_tasks());
  EXPECT_FALSE(task.is_freed());
  worker.destroy();
}

TEST(WorkerTest, stop_rejects_new_submit_while_draining_old_tasks)
{
  LogAsyncIOWorker worker;
  IPalfEnvImpl *fake_env = reinterpret_cast<IPalfEnvImpl *>(0xDEAD);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, fake_env, 64));
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  ctx.set_hold_pending(true);
  ASSERT_EQ(OB_SUCCESS, worker.start());

  TrackableLogIOTask accepted_task(1);
  EXPECT_EQ(OB_SUCCESS, worker.submit_io_task(&accepted_task));
  for (int64_t i = 0; i < 2000 && ctx.get_enqueued_tasks() == 0; i++) {
    ob_usleep(1000);
  }
  ASSERT_EQ(1, ctx.get_enqueued_tasks());

  worker.stop();
  ASSERT_FALSE(worker.is_running_);

  TrackableLogIOTask rejected_task(1);
  EXPECT_EQ(OB_NOT_RUNNING, worker.submit_io_task(&rejected_task));
  EXPECT_FALSE(rejected_task.is_freed());

  std::atomic<bool> wait_done{false};
  std::thread wait_thread([&]() {
    EXPECT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
    worker.wait();
    wait_done.store(true);
  });
  ob_usleep(50 * 1000);
  EXPECT_FALSE(wait_done.load());
  ctx.set_hold_pending(false);
  for (int64_t i = 0; i < 2000 && !wait_done.load(); i++) {
    ob_usleep(1000);
  }
  EXPECT_TRUE(wait_done.load());
  wait_thread.join();
  EXPECT_FALSE(accepted_task.is_freed());
  worker.destroy();
}

TEST(WorkerTest, submit_io_task_enqueues_and_worker_dispatches)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_TRUE(worker.is_valid());
  FakeAsyncPalfIOCtx ctx(1);
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(worker, 1, ctx));
  ASSERT_EQ(OB_SUCCESS, worker.start());

  FakeLogIOTask *t = new FakeLogIOTask(1);
  EXPECT_EQ(OB_SUCCESS, worker.submit_io_task(t));
  for (int i = 0; i < 2000 && ctx.get_enqueued_tasks() == 0; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, ctx.get_enqueued_tasks());
  EXPECT_EQ(1, worker.diagnostics_.get_submitted_task_count());

  worker.stop();
  ASSERT_EQ(OB_SUCCESS, worker.unregister_palf_ctx_and_wait(1));
  worker.wait();
  delete t;
  worker.destroy();
}

TEST(WorkerTest, submit_io_task_no_ctx_rejected_before_queue)
{
  LogAsyncIOWorker worker;
  IPalfEnvImpl *fake_env = reinterpret_cast<IPalfEnvImpl *>(0xDEAD);
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, fake_env, 64));
  ASSERT_EQ(OB_SUCCESS, worker.start());

  TrackableLogIOTask task(99);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, worker.submit_io_task(&task));
  EXPECT_FALSE(task.is_freed());
  EXPECT_EQ(0, worker.diagnostics_.get_submitted_task_count());
  EXPECT_EQ(0, worker.diagnostics_.get_handled_task_count());
  EXPECT_EQ(1, worker.diagnostics_.get_dropped_submit_count());

  worker.stop();
  worker.wait();
  worker.destroy();
}

TEST(WorkerTest, submit_io_task_before_start_rejected)
{
  LogAsyncIOWorker worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  FakeLogIOTask *t = new FakeLogIOTask(1);
  EXPECT_EQ(OB_NOT_RUNNING, worker.submit_io_task(t));

  delete t;
  worker.destroy();
}

TEST(ConfigTest, palf_env_default_disables_async_io)
{
  EXPECT_FALSE(PalfEnvImpl::is_async_io_enabled_by_default_());
}

TEST(ConfigTest, async_worker_reuses_legacy_queue_capacity_config)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerConfig config;
  const int64_t queue_capacity = 17;
  config.io_worker_num_ = 2;
  config.io_queue_capcity_ = queue_capacity;
  config.batch_width_ = 1;
  config.batch_depth_ = 1;
  wrapper.enable_async_io_ = true;

  ASSERT_EQ(OB_SUCCESS,
            wrapper.create_and_init_async_workers_(config, OB_SERVER_TENANT_ID, NULL));
  ASSERT_EQ(config.io_worker_num_, wrapper.worker_count_);
  ASSERT_TRUE(OB_ISNULL(wrapper.log_io_workers_));
  ASSERT_TRUE(OB_NOT_NULL(wrapper.async_workers_));
  for (int64_t i = 0; i < wrapper.worker_count_; i++) {
    EXPECT_EQ(queue_capacity, wrapper.async_workers_[i].input_queue_.capacity());
  }

  wrapper.destroy_and_free_async_pool_();
}

TEST(ConfigTest, async_worker_init_failure_frees_constructed_pool)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerConfig config;
  config.io_worker_num_ = 2;
  config.io_queue_capcity_ = 0;
  config.batch_width_ = 1;
  config.batch_depth_ = 1;

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            wrapper.create_and_init_async_workers_(config, OB_SERVER_TENANT_ID, NULL));
  EXPECT_TRUE(OB_ISNULL(wrapper.async_workers_));
  EXPECT_EQ(0, wrapper.worker_count_);
}

TEST(ConfigTest, async_wrapper_destroy_releases_async_pool_by_mode)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerConfig config;
  config.io_worker_num_ = 2;
  config.io_queue_capcity_ = 17;
  config.batch_width_ = 1;
  config.batch_depth_ = 1;
  wrapper.enable_async_io_ = true;

  ASSERT_EQ(OB_SUCCESS,
            wrapper.create_and_init_async_workers_(config, OB_SERVER_TENANT_ID, NULL));
  ASSERT_TRUE(OB_NOT_NULL(wrapper.async_workers_));
  ASSERT_TRUE(OB_ISNULL(wrapper.log_io_workers_));
  EXPECT_EQ(config.io_worker_num_, wrapper.worker_count_);

  wrapper.destroy();
  EXPECT_TRUE(OB_ISNULL(wrapper.async_workers_));
  EXPECT_TRUE(OB_ISNULL(wrapper.log_io_workers_));
  EXPECT_EQ(-1, wrapper.worker_count_);
}

TEST(PalfEnvCleanupTest, inserted_handle_cleanup_releases_by_map_only)
{
  PalfEnvImpl env;
  PalfHandleImpl *handle = NULL;
  const LSKey key(1001);
  ASSERT_EQ(OB_SUCCESS,
            env.palf_handle_impl_map_.init("PalfEnvMapUT", OB_SERVER_TENANT_ID));
  handle = PalfHandleImplFactory::alloc();
  ASSERT_TRUE(NULL != handle);
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.insert_and_get(key, handle));
  EXPECT_EQ(1, env.palf_handle_impl_map_.count());

  env.cleanup_failed_inserted_palf_handle_impl_(key,
                                                 true /* need_revert */,
                                                 handle);
  EXPECT_TRUE(NULL == handle);
  EXPECT_EQ(0, env.palf_handle_impl_map_.count());
  env.palf_handle_impl_map_.destroy();
}

TEST(PalfEnvCleanupTest, reloaded_handle_cleanup_uses_map_owned_reference)
{
  PalfEnvImpl env;
  PalfHandleImpl *handle = NULL;
  const LSKey key(1002);
  ASSERT_EQ(OB_SUCCESS,
            env.palf_handle_impl_map_.init("PalfEnvMapUT", OB_SERVER_TENANT_ID));
  handle = PalfHandleImplFactory::alloc();
  ASSERT_TRUE(NULL != handle);
  ASSERT_EQ(OB_SUCCESS, env.palf_handle_impl_map_.insert_and_get(key, handle));
  env.palf_handle_impl_map_.revert(handle);

  env.cleanup_failed_inserted_palf_handle_impl_(key,
                                                 false /* need_revert */,
                                                 handle);
  EXPECT_TRUE(NULL == handle);
  EXPECT_EQ(0, env.palf_handle_impl_map_.count());
  env.palf_handle_impl_map_.destroy();
}

TEST(PalfEnvCleanupTest, stop_waits_for_palf_registration_barrier)
{
  PalfEnvImpl env;
  std::atomic<bool> stop_entered{false};
  std::atomic<bool> stop_finished{false};
  std::thread stop_thread;
  {
    // PALF create holds this lock until async ctx registration is complete.
    PalfEnvImpl::WLockGuard guard(env.palf_meta_lock_);
    stop_thread = std::thread([&]() {
      stop_entered.store(true);
      env.stop();
      stop_finished.store(true);
    });
    for (int64_t i = 0; i < 2000 && !stop_entered.load(); ++i) {
      ob_usleep(1000);
    }
    EXPECT_TRUE(stop_entered.load());
    ob_usleep(20 * 1000);
    EXPECT_FALSE(stop_finished.load());
  }
  stop_thread.join();
  EXPECT_TRUE(stop_finished.load());
}

TEST(PalfEnvCleanupTest, wait_unregisters_async_ctx_before_worker_join)
{
  const int64_t palf_id = 1003;
  PalfEnvImpl env;
  LogIOWorkerWrapper &wrapper = env.log_io_worker_wrapper_;
  LogIOWorkerConfig config;
  PalfHandleImpl *handle = NULL;
  FakeAsyncPalfIOCtx ctx(palf_id);
  config.io_worker_num_ = 1;
  config.io_queue_capcity_ = 17;
  config.batch_width_ = 1;
  config.batch_depth_ = 1;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;

  ASSERT_EQ(OB_SUCCESS,
            env.palf_handle_impl_map_.init("PalfEnvMapUT", OB_SERVER_TENANT_ID));
  ASSERT_TRUE(NULL != (handle = PalfHandleImplFactory::alloc()));
  ASSERT_EQ(OB_SUCCESS,
            env.palf_handle_impl_map_.insert_and_get(LSKey(palf_id), handle));
  env.palf_handle_impl_map_.revert(handle);
  ASSERT_EQ(OB_SUCCESS,
            wrapper.create_and_init_async_workers_(config, OB_SERVER_TENANT_ID, &env));
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(wrapper.async_workers_[0], palf_id, ctx));
  ASSERT_EQ(OB_SUCCESS, wrapper.register_palf_async_index_(palf_id, 0));

  env.is_inited_ = true;
  env.wait();

  EXPECT_EQ(1, ctx.get_free_this_count());
  EXPECT_EQ(0, wrapper.async_workers_[0].get_ctx_count_());
  env.destroy();

  EXPECT_TRUE(NULL == wrapper.async_workers_);
}

// TODO(shouju.zyp): Add PalfEnv lifecycle coverage that forces async
// registration failure during create/reload and verifies cleanup and reference
// ownership. Also verify remove unregisters the ctx before deleting its
// PalfHandleImpl, and tenant wait drains callbacks and removes every
// worker-owned ctx before destroy releases the worker pool.

TEST(RegistrationTest, control_task_with_async_submitter_dispatches_to_async_worker)
{
  const int64_t palf_id = 1003;
  LogIOWorkerWrapper wrapper;
  LogAsyncIOWorker async_worker;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.log_io_workers_ = NULL;
  wrapper.worker_count_ = 1;
  wrapper.async_workers_ = &async_worker;
  wrapper.round_robin_idx_ = 0;
  wrapper.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.set_refactored(palf_id, 0));
  ASSERT_EQ(OB_SUCCESS, async_worker.init(OB_SERVER_TENANT_ID, NULL, 64));

  TypedFakeLogIOTask *task = new TypedFakeLogIOTask(palf_id, LogIOTaskType::FLUSH_META_TYPE);
  ASSERT_TRUE(NULL != wrapper.get_async_io_worker_(palf_id));
  EXPECT_EQ(OB_NOT_RUNNING, wrapper.get_async_io_worker_(palf_id)->submit_io_task(task));
  delete task;

  async_worker.destroy();
  wrapper.palf_async_index_map_.destroy();
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.is_inited_ = false;
}

TEST(RegistrationTest, unregister_failure_keeps_async_index_for_retry)
{
  const int64_t palf_id = 1005;
  LogIOWorkerWrapper wrapper;
  LogAsyncIOWorker async_worker;
  FakeAsyncPalfIOCtx ctx(palf_id);
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.log_io_workers_ = NULL;
  wrapper.worker_count_ = 1;
  wrapper.async_workers_ = &async_worker;
  wrapper.round_robin_idx_ = 0;
  wrapper.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));
  ASSERT_EQ(OB_SUCCESS, async_worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  ASSERT_EQ(OB_SUCCESS, register_worker_fake_ctx(async_worker, palf_id, ctx));
  ASSERT_EQ(OB_SUCCESS, wrapper.register_palf_async_index_(palf_id, 0));

  async_worker.is_destroying_ = true;
  EXPECT_EQ(OB_STATE_NOT_MATCH, wrapper.unregister_async_palf_ctx(palf_id));
  EXPECT_TRUE(NULL != wrapper.get_async_io_worker_(palf_id));
  async_worker.is_destroying_ = false;
  EXPECT_EQ(OB_SUCCESS, wrapper.unregister_async_palf_ctx(palf_id));
  EXPECT_TRUE(NULL == wrapper.get_async_io_worker_(palf_id));

  async_worker.destroy();
  wrapper.palf_async_index_map_.destroy();
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.is_inited_ = false;
}

TEST(RegistrationTest, wrapper_registration_publishes_ctx_and_index)
{
  const int64_t palf_id = 1006;
  share::ObTenantBase tenant_base(OB_SERVER_TENANT_ID);
  share::ObTenantSwitchGuard tenant_guard(&tenant_base);
  LogIOWorkerWrapper wrapper;
  LogAsyncIOWorker async_worker;
  FakePalfEnvImpl fake_env;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 1;
  wrapper.async_workers_ = &async_worker;
  wrapper.palf_env_impl_ = &fake_env;
  wrapper.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));
  ASSERT_EQ(OB_SUCCESS, async_worker.init(OB_SERVER_TENANT_ID, &fake_env, 64));

  ASSERT_EQ(OB_SUCCESS,
            wrapper.register_async_palf_ctx_if_needed(
                palf_id, 1 /* cb_thread_pool_tg_id */,
                &async_worker));
  int64_t worker_index = -1;
  EXPECT_EQ(OB_SUCCESS,
            wrapper.palf_async_index_map_.get_refactored(palf_id, worker_index));
  EXPECT_EQ(0, worker_index);
  EXPECT_EQ(1, async_worker.get_ctx_count_());
  EXPECT_EQ(OB_SUCCESS,
            wrapper.unregister_async_palf_ctx(palf_id));
  EXPECT_EQ(0, async_worker.get_ctx_count_());

  async_worker.destroy();
  wrapper.palf_async_index_map_.destroy();
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.palf_env_impl_ = NULL;
  wrapper.is_inited_ = false;
}

TEST(RegistrationTest, async_sys_ctx_ignores_writing_throttle)
{
  const int64_t data_palf_id = 1008;
  share::ObTenantBase tenant_base(OB_SERVER_TENANT_ID);
  share::ObTenantSwitchGuard tenant_guard(&tenant_base);
  LogIOWorkerWrapper wrapper;
  LogAsyncIOWorker async_worker;
  FakePalfEnvImpl fake_env;
  AsyncPalfIOCtxEntryGuard sys_entry_guard;
  AsyncPalfIOCtxEntryGuard data_entry_guard;
  AsyncPalfIOCtx *sys_ctx = NULL;
  AsyncPalfIOCtx *data_ctx = NULL;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 1;
  wrapper.async_workers_ = &async_worker;
  wrapper.palf_env_impl_ = &fake_env;
  wrapper.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));
  ASSERT_EQ(OB_SUCCESS, async_worker.init(OB_SERVER_TENANT_ID, &fake_env, 64));

  ASSERT_EQ(OB_SUCCESS,
            wrapper.register_async_palf_ctx_if_needed(
                SYS_PALF_ID, 1 /* cb_thread_pool_tg_id */, &async_worker));
  ASSERT_EQ(OB_SUCCESS,
            wrapper.register_async_palf_ctx_if_needed(
                data_palf_id, 1 /* cb_thread_pool_tg_id */, &async_worker));
  ASSERT_EQ(OB_SUCCESS,
            async_worker.get_entry_guard_(SYS_PALF_ID,
                                          false /* allow_unregistering */,
                                          sys_entry_guard));
  ASSERT_EQ(OB_SUCCESS,
            async_worker.get_entry_guard_(data_palf_id,
                                          false /* allow_unregistering */,
                                          data_entry_guard));
  sys_ctx = static_cast<AsyncPalfIOCtx *>(sys_entry_guard.get_ctx());
  data_ctx = static_cast<AsyncPalfIOCtx *>(data_entry_guard.get_ctx());
  ASSERT_TRUE(OB_NOT_NULL(sys_ctx));
  ASSERT_TRUE(OB_NOT_NULL(data_ctx));
  EXPECT_FALSE(sys_ctx->has_async_throttle_());
  EXPECT_TRUE(data_ctx->has_async_throttle_());

  sys_entry_guard.reset();
  data_entry_guard.reset();
  EXPECT_EQ(OB_SUCCESS, wrapper.unregister_async_palf_ctx(SYS_PALF_ID));
  EXPECT_EQ(OB_SUCCESS, wrapper.unregister_async_palf_ctx(data_palf_id));
  async_worker.destroy();
  wrapper.palf_async_index_map_.destroy();
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.palf_env_impl_ = NULL;
  wrapper.is_inited_ = false;
}

TEST(RegistrationTest, index_publish_failure_rolls_back_created_ctx)
{
  const int64_t palf_id = 1007;
  share::ObTenantBase tenant_base(OB_SERVER_TENANT_ID);
  share::ObTenantSwitchGuard tenant_guard(&tenant_base);
  LogIOWorkerWrapper wrapper;
  LogAsyncIOWorker async_worker;
  FakePalfEnvImpl fake_env;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 1;
  wrapper.async_workers_ = &async_worker;
  wrapper.palf_env_impl_ = &fake_env;
  wrapper.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, async_worker.init(OB_SERVER_TENANT_ID, &fake_env, 64));

  EXPECT_EQ(OB_NOT_INIT,
            wrapper.register_async_palf_ctx_if_needed(
                palf_id, 1 /* cb_thread_pool_tg_id */,
                &async_worker));
  EXPECT_EQ(0, async_worker.get_ctx_count_());

  async_worker.destroy();
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.palf_env_impl_ = NULL;
  wrapper.is_inited_ = false;
}

TEST(SubmitterTest, async_user_tenant_selects_sys_and_data_palf_async_submitter)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerBase *sys_submitter = NULL;
  LogIOWorkerBase *data_submitter = NULL;
  int64_t async_index = -1;
  LogAsyncIOWorker async_workers[3];
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 3;
  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = async_workers;
  wrapper.round_robin_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));

  ASSERT_EQ(OB_SUCCESS, wrapper.build_palf_io_submitter_(SYS_PALF_ID, sys_submitter));
  EXPECT_EQ(static_cast<LogIOWorkerBase *>(async_workers + 0), sys_submitter);
  ASSERT_EQ(OB_SUCCESS, wrapper.get_async_worker_index_by_submitter_(sys_submitter, async_index));
  EXPECT_EQ(0, async_index);

  async_index = -1;
  ASSERT_EQ(OB_SUCCESS, wrapper.build_palf_io_submitter_(1001, data_submitter));
  EXPECT_EQ(static_cast<LogIOWorkerBase *>(async_workers + 1), data_submitter);
  ASSERT_EQ(OB_SUCCESS, wrapper.get_async_worker_index_by_submitter_(data_submitter, async_index));
  EXPECT_EQ(1, async_index);

  wrapper.palf_async_index_map_.destroy();
  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
}

TEST(SubmitterTest, async_single_worker_uses_same_worker_for_data_palf)
{
  LogIOWorkerWrapper wrapper;
  LogAsyncIOWorker async_worker;
  LogIOWorkerBase *submitter = NULL;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 1;
  wrapper.async_workers_ = &async_worker;
  wrapper.round_robin_idx_ = 0;

  ASSERT_EQ(OB_SUCCESS, wrapper.build_palf_io_submitter_(1001, submitter));
  EXPECT_EQ(static_cast<LogIOWorkerBase *>(&async_worker), submitter);

  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
}

TEST(SubmitterTest, public_selection_validates_wrapper_and_palf_id)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerBase *submitter = reinterpret_cast<LogIOWorkerBase *>(0x1);
  EXPECT_EQ(OB_NOT_INIT,
            wrapper.select_palf_io_submitter(1001, submitter));
  EXPECT_TRUE(OB_ISNULL(submitter));

  wrapper.is_inited_ = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            wrapper.select_palf_io_submitter(INVALID_PALF_ID, submitter));
  EXPECT_TRUE(OB_ISNULL(submitter));
  wrapper.is_inited_ = false;
}

// TODO(shouju.zyp): IMPORTANT CLOSE-MODULE COVERAGE.
// PalfEnvLite used to construct its legacy worker with throttling disabled.
// Add an arbitration lifecycle test proving the LogIOWorkerWrapper replacement
// preserves that contract before changing close-module production code.

TEST(SubmitterTest, async_submitter_missing_ctx_rejects_without_legacy_fallback)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerBase *submitter = NULL;
  int64_t async_index = -1;
  const int64_t palf_id = 1001;
  LogAsyncIOWorker async_worker;
  ASSERT_EQ(OB_SUCCESS, async_worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 1;
  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = &async_worker;
  wrapper.round_robin_idx_ = 0;
  wrapper.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));

  ASSERT_EQ(OB_SUCCESS, wrapper.build_palf_io_submitter_(palf_id, submitter));
  ASSERT_EQ(OB_SUCCESS, wrapper.get_async_worker_index_by_submitter_(submitter, async_index));
  ASSERT_EQ(OB_SUCCESS, wrapper.register_palf_async_index_(palf_id, async_index));
  ASSERT_EQ(OB_SUCCESS, async_worker.start());

  TrackableLogIOTask task(palf_id);
  ASSERT_TRUE(NULL != submitter);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, submitter->submit_io_task(&task));
  EXPECT_FALSE(task.is_freed());

  async_worker.stop();
  async_worker.wait();
  async_worker.destroy();
  wrapper.palf_async_index_map_.destroy();
  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.is_inited_ = false;
}

TEST(SubmitterTest, selected_async_submitter_missing_ctx_rejects_without_legacy_fallback)
{
  LogIOWorkerWrapper wrapper;
  const int64_t palf_id = 1002;
  LogAsyncIOWorker async_worker;
  ASSERT_EQ(OB_SUCCESS, async_worker.init(OB_SERVER_TENANT_ID, NULL, 64));
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 1;
  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = &async_worker;
  wrapper.round_robin_idx_ = 0;
  wrapper.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, wrapper.palf_async_index_map_.create(64, "PalfAsyncIdx"));

  TrackableLogIOTask task(palf_id);
  LogIOWorkerBase *submitter = NULL;
  ASSERT_EQ(OB_SUCCESS, wrapper.build_palf_io_submitter_(palf_id, submitter));
  ASSERT_TRUE(NULL != submitter);
  ASSERT_EQ(OB_SUCCESS, async_worker.start());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, submitter->submit_io_task(&task));
  EXPECT_FALSE(task.is_freed());

  async_worker.stop();
  async_worker.wait();
  async_worker.destroy();
  wrapper.palf_async_index_map_.destroy();
  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.is_inited_ = false;
}

TEST(RegistrationTest, sync_wrapper_skips_async_ctx_registration)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorker legacy_worker;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = false;
  wrapper.worker_count_ = 1;
  wrapper.log_io_workers_ = &legacy_worker;
  wrapper.async_workers_ = NULL;

  EXPECT_EQ(OB_SUCCESS,
            wrapper.register_async_palf_ctx_if_needed(
                1003, 1, &legacy_worker));

  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
}

TEST(RegistrationTest, async_wrapper_rejects_non_async_submitter)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorker legacy_worker;
  LogAsyncIOWorker async_worker;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.worker_count_ = 1;
  wrapper.log_io_workers_ = NULL;
  wrapper.async_workers_ = &async_worker;

  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            wrapper.register_async_palf_ctx_if_needed(
                1003, 1, &legacy_worker));

  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
}

TEST(LogIOWorkerTest, rejects_null_task_and_null_throttle)
{
  LogIOWorker worker;
  FakeLogIOTask task(1);
  worker.is_inited_ = true;
  worker.need_ignoring_throttling_ = false;
  worker.throttle_ = NULL;

  EXPECT_EQ(OB_INVALID_ARGUMENT, worker.submit_io_task(NULL));
  EXPECT_EQ(OB_INVALID_ARGUMENT, worker.handle_io_task_(NULL));
  EXPECT_EQ(OB_INVALID_ARGUMENT, worker.handle_io_task_with_throttling_(NULL));
  EXPECT_EQ(OB_ERR_UNEXPECTED, worker.notify_need_writing_throttling(true));
  EXPECT_EQ(OB_ERR_UNEXPECTED, worker.handle_io_task_with_throttling_(&task));

  worker.is_inited_ = false;
}

TEST(RegistrationTest, async_submitter_missing_worker_reports_error)
{
  LogIOWorkerWrapper wrapper;
  LogAsyncIOWorker async_worker;
  IPalfEnvImpl *fake_env = reinterpret_cast<IPalfEnvImpl *>(static_cast<void *>(&async_worker));
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = true;
  wrapper.log_io_workers_ = NULL;
  wrapper.worker_count_ = 1;
  wrapper.async_workers_ = &async_worker;
  wrapper.palf_env_impl_ = fake_env;

  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            wrapper.register_async_palf_ctx_if_needed(
                1004, 1, &async_worker));

  wrapper.async_workers_ = NULL;
  wrapper.worker_count_ = 0;
  wrapper.palf_env_impl_ = NULL;
}

TEST(SubmitterTest, async_disabled_user_tenant_selects_data_palf_legacy_submitter)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerBase *submitter = NULL;
  int64_t async_index = -1;
  LogIOWorker legacy_workers[3];
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = false;
  wrapper.worker_count_ = 3;
  wrapper.log_io_workers_ = legacy_workers;
  wrapper.async_workers_ = NULL;
  wrapper.round_robin_idx_ = 0;

  ASSERT_EQ(OB_SUCCESS, wrapper.build_palf_io_submitter_(1001, submitter));
  EXPECT_EQ(static_cast<LogIOWorkerBase *>(legacy_workers + 1), submitter);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, wrapper.get_async_worker_index_by_submitter_(submitter, async_index));
  EXPECT_EQ(-1, async_index);
  wrapper.log_io_workers_ = NULL;
  wrapper.worker_count_ = 0;
}

TEST(SubmitterTest, sync_user_tenant_rejects_data_palf_without_data_worker)
{
  LogIOWorkerWrapper wrapper;
  LogIOWorkerBase *submitter = NULL;
  LogIOWorker legacy_worker;
  wrapper.is_user_tenant_ = true;
  wrapper.enable_async_io_ = false;
  wrapper.worker_count_ = 1;
  wrapper.log_io_workers_ = &legacy_worker;
  wrapper.async_workers_ = NULL;
  wrapper.round_robin_idx_ = 0;

  EXPECT_EQ(OB_ERR_UNEXPECTED, wrapper.build_palf_io_submitter_(1001, submitter));
  EXPECT_EQ(NULL, submitter);

  wrapper.log_io_workers_ = NULL;
  wrapper.worker_count_ = 0;
}

} // namespace palf
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_palf_async_io_worker.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_palf_async_io_worker");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

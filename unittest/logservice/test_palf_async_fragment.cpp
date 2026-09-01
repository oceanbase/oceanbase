/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_block_alloc_mgr.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "logservice/palf/log_io_context.h"
#include "logservice/palf/log_io_task.h"
#include "logservice/palf/log_io_task_cb_thread_pool.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/ob_local_device.h"
#include "unittest/logservice/mock_palf_handle_impl_for_async.h"

#define private public
#define protected public

#include "logservice/palf/log_async_fragment.h"
#include "logservice/palf/log_async_palf_ctx.h"
#include "logservice/palf/log_writer_utils.h"
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/palf_handle_impl_guard.h"
#include "logservice/palf/lsn.h"
#include "logservice/palf/log_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

static const uint64_t TEST_TENANT_ID = OB_SYS_TENANT_ID;

LSN make_lsn(const offset_t off)
{
  return LSN(off);
}

class TestErrsimGuard
{
public:
  explicit TestErrsimGuard(const char *name) : name_(name) {}
  ~TestErrsimGuard()
  {
    common::EventItem item;
    (void) common::EventTable::set_event(name_, item);
  }

  int set_value(const int64_t value)
  {
    common::EventItem item;
    item.error_code_ = value;
    // Keep the value available across multiple one-second refresh rounds.
    item.occur_ = 0 == value ? 0 : 1000;
    return common::EventTable::set_event(name_, item);
  }

private:
  const char *name_;
};

TEST(TestPalfAsyncFragment, FailedEnqueueDoesNotReleaseTaskSlot)
{
  LogIOFlushMetaTask task(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncPalfIOCtx ctx;

  EXPECT_EQ(0, ATOMIC_LOAD(&ctx.available_flush_task_slot_count_));
  EXPECT_EQ(0, ATOMIC_LOAD(&ctx.available_barrier_task_slot_count_));
  EXPECT_EQ(OB_NOT_INIT, ctx.enqueue_task(&task));
  EXPECT_EQ(0, ATOMIC_LOAD(&ctx.available_flush_task_slot_count_));
  EXPECT_EQ(0, ATOMIC_LOAD(&ctx.available_barrier_task_slot_count_));
}

class MockLogAllocator : public common::ObILogAllocator
{
public:
  MockLogAllocator()
    : block_alloc_(),
      free_flush_task_cnt_(0),
      free_truncate_prefix_task_cnt_(0),
      free_purge_task_cnt_(0)
  {}
  ~MockLogAllocator() {}

  void *alloc(const int64_t size) override
  { return common::ob_malloc(size, common::ObMemAttr(TEST_TENANT_ID, "PalfTest")); }
  void *alloc(const int64_t size, const common::ObMemAttr &attr) override
  { return common::ob_malloc(size, attr); }
  void free(void *ptr) override { common::ob_free(ptr); }
  void *ge_alloc(const int64_t size) override
  { return common::ob_malloc(size, common::ObMemAttr(TEST_TENANT_ID, "PalfTest")); }
  void ge_free(void *ptr) override { common::ob_free(ptr); }
  const common::ObBlockAllocMgr &get_clog_blk_alloc_mgr() const override { return block_alloc_; }
  LogHandleSubmitTask *alloc_log_handle_submit_task(const int64_t palf_id, const int64_t palf_epoch) override
  { UNUSED(palf_id); UNUSED(palf_epoch); return NULL; }
  void free_log_handle_submit_task(LogHandleSubmitTask *ptr) override { UNUSED(ptr); }
  LogIOFlushLogTask *alloc_log_io_flush_log_task(const int64_t palf_id, const int64_t palf_epoch) override
  { return new LogIOFlushLogTask(palf_id, palf_epoch); }
  void free_log_io_flush_log_task(LogIOFlushLogTask *ptr) override
  {
    if (OB_NOT_NULL(ptr)) {
      ATOMIC_INC(&free_flush_task_cnt_);
    }
  }
  LogIOTruncateLogTask *alloc_log_io_truncate_log_task(const int64_t palf_id, const int64_t palf_epoch) override
  { UNUSED(palf_id); UNUSED(palf_epoch); return NULL; }
  void free_log_io_truncate_log_task(LogIOTruncateLogTask *ptr) override { UNUSED(ptr); }
  LogIOFlushMetaTask *alloc_log_io_flush_meta_task(const int64_t palf_id, const int64_t palf_epoch) override
  { UNUSED(palf_id); UNUSED(palf_epoch); return NULL; }
  void free_log_io_flush_meta_task(LogIOFlushMetaTask *ptr) override { UNUSED(ptr); }
  LogIOTruncatePrefixBlocksTask *alloc_log_io_truncate_prefix_blocks_task(
      const int64_t palf_id, const int64_t palf_epoch) override
  { UNUSED(palf_id); UNUSED(palf_epoch); return NULL; }
  void free_log_io_truncate_prefix_blocks_task(LogIOTruncatePrefixBlocksTask *ptr) override
  {
    if (OB_NOT_NULL(ptr)) {
      ATOMIC_INC(&free_truncate_prefix_task_cnt_);
    }
  }
  FetchLogTask *alloc_palf_fetch_log_task() override { return NULL; }
  void free_palf_fetch_log_task(FetchLogTask *ptr) override { UNUSED(ptr); }
  void *alloc_replay_task(const int64_t size) override
  { return common::ob_malloc(size, common::ObMemAttr(TEST_TENANT_ID, "PalfTest")); }
  void *alloc_replay_log_buf(const int64_t size) override
  { return common::ob_malloc(size, common::ObMemAttr(TEST_TENANT_ID, "PalfTest")); }
  void free_replay_task(logservice::ObLogReplayTask *ptr) override { common::ob_free(ptr); }
  void free_replay_log_buf(void *ptr) override { common::ob_free(ptr); }
  LogIOFlashbackTask *alloc_log_io_flashback_task(const int64_t palf_id, const int64_t palf_epoch) override
  { UNUSED(palf_id); UNUSED(palf_epoch); return NULL; }
  void free_log_io_flashback_task(LogIOFlashbackTask *ptr) override { UNUSED(ptr); }
  LogIOPurgeThrottlingTask *alloc_log_io_purge_throttling_task(
      const int64_t palf_id, const int64_t palf_epoch) override
  { UNUSED(palf_id); UNUSED(palf_epoch); return NULL; }
  void free_log_io_purge_throttling_task(LogIOPurgeThrottlingTask *ptr) override
  {
    if (OB_NOT_NULL(ptr)) {
      ATOMIC_INC(&free_purge_task_cnt_);
    }
  }
  LogFillCacheTask *alloc_log_fill_cache_task(const int64_t palf_id, const int64_t palf_epoch) override
  { UNUSED(palf_id); UNUSED(palf_epoch); return NULL; }
  void free_log_fill_cache_task(LogFillCacheTask *ptr) override { UNUSED(ptr); }
  void *alloc_append_compression_buf(const int64_t size) override
  { return common::ob_malloc(size, common::ObMemAttr(TEST_TENANT_ID, "PalfTest")); }
  void free_append_compression_buf(void *ptr) override { common::ob_free(ptr); }
  void *alloc_replay_decompression_buf(const int64_t size) override
  { return common::ob_malloc(size, common::ObMemAttr(TEST_TENANT_ID, "PalfTest")); }
  void free_replay_decompression_buf(void *ptr) override { common::ob_free(ptr); }
  common::ObIAllocator *get_replay_decompression_allocator() override { return this; }
  int64_t get_free_flush_task_cnt() const { return ATOMIC_LOAD(&free_flush_task_cnt_); }
  int64_t get_free_truncate_prefix_task_cnt() const
  { return ATOMIC_LOAD(&free_truncate_prefix_task_cnt_); }
  int64_t get_free_purge_task_cnt() const { return ATOMIC_LOAD(&free_purge_task_cnt_); }

private:
  common::ObBlockAllocMgr block_alloc_;
  int64_t free_flush_task_cnt_;
  int64_t free_truncate_prefix_task_cnt_;
  int64_t free_purge_task_cnt_;
};

class MockPalfEnvImpl : public IPalfEnvImpl
{
public:
  explicit MockPalfEnvImpl(MockAsyncPalfHandleImpl &handle)
    : handle_(handle), allocator_(), guard_get_count_(0), revert_count_(0)
  {}
  ~MockPalfEnvImpl() {}

  int get_palf_handle_impl(const int64_t palf_id, IPalfHandleImplGuard &guard) override
  {
    ++guard_get_count_;
    guard.palf_id_ = palf_id;
    guard.palf_handle_impl_ = &handle_;
    guard.palf_env_impl_ = this;
    return OB_SUCCESS;
  }
  int get_palf_handle_impl(const int64_t palf_id, IPalfHandleImpl *&handle) override
  { UNUSED(palf_id); handle = &handle_; return OB_SUCCESS; }
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
  void revert_palf_handle_impl(IPalfHandleImpl *handle) override
  { UNUSED(handle); ++revert_count_; }
  common::ObILogAllocator *get_log_allocator() override { return &allocator_; }
  int for_each(const common::ObFunction<int(IPalfHandleImpl *)> &func) override
  { return func(&handle_); }
  int create_directory(const char *base_dir) override { UNUSED(base_dir); return OB_NOT_SUPPORTED; }
  int remove_directory(const char *base_dir) override { UNUSED(base_dir); return OB_NOT_SUPPORTED; }
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
  int64_t get_tenant_id() override { return TEST_TENANT_ID; }
  int update_replayable_point(const SCN &replayable_scn) override
  { UNUSED(replayable_scn); return OB_SUCCESS; }
  int get_throttling_options(PalfThrottleOptions &option) override
  { option.reset(); return OB_SUCCESS; }
  void period_calc_disk_usage() override {}
  LogSharedQueueTh *get_log_shared_queue_thread() override { return NULL; }
  int get_options(PalfOptions &options) override
  { options.reset(); return OB_SUCCESS; }
  int64_t get_free_flush_task_cnt() const { return allocator_.get_free_flush_task_cnt(); }
  int64_t get_free_truncate_prefix_task_cnt() const
  { return allocator_.get_free_truncate_prefix_task_cnt(); }
  int64_t get_free_purge_task_cnt() const { return allocator_.get_free_purge_task_cnt(); }
  int64_t get_guard_get_count() const { return guard_get_count_; }
  int64_t get_revert_count() const { return revert_count_; }

private:
  MockAsyncPalfHandleImpl &handle_;
  MockLogAllocator allocator_;
  int64_t guard_get_count_;
  int64_t revert_count_;
};

class MockAsyncDriveWaker : public IAsyncDriveWaker
{
public:
  MockAsyncDriveWaker() : wake_cnt_(0) {}
  ~MockAsyncDriveWaker() {}
  int wake_up_for_drive() override
  {
    ++wake_cnt_;
    return OB_SUCCESS;
  }
  int64_t get_wake_cnt() const { return wake_cnt_; }

private:
  int64_t wake_cnt_;
};

class MutationThenFailControlTask : public LogIOTask
{
public:
  MutationThenFailControlTask(const int64_t palf_id, const int64_t palf_epoch)
    : LogIOTask(palf_id, palf_epoch), execute_count_(0), callback_count_(0), free_count_(0)
  {}
  ~MutationThenFailControlTask() override {}
  int64_t get_execute_count() const { return ATOMIC_LOAD(&execute_count_); }
  int64_t get_callback_count() const { return ATOMIC_LOAD(&callback_count_); }
  int64_t get_free_count() const { return ATOMIC_LOAD(&free_count_); }

protected:
  int do_task_(const int tg_id, IPalfHandleImplGuard &guard) override
  {
    UNUSED(guard);
    UNUSED(tg_id);
    ATOMIC_INC(&execute_count_);
    return OB_EAGAIN;
  }
  int after_consume_(IPalfHandleImplGuard &guard) override
  {
    UNUSED(guard);
    ATOMIC_INC(&callback_count_);
    return OB_SUCCESS;
  }
  LogIOTaskType get_io_task_type_() const override { return LogIOTaskType::FLUSH_META_TYPE; }
  void free_this_(IPalfEnvImpl *palf_env_impl) override
  {
    UNUSED(palf_env_impl);
    ATOMIC_INC(&free_count_);
  }
  int64_t get_io_size_() const override { return 0; }
  bool need_purge_throttling_() const override { return false; }

private:
  int64_t execute_count_;
  int64_t callback_count_;
  int64_t free_count_;
};

struct UnitTaskArena
{
  UnitTaskArena() : task_cnt(0), buf_cnt(0), tasks(), bufs() {}
  ~UnitTaskArena() { reset(); }

  void reset()
  {
    for (int64_t i = 0; i < task_cnt; ++i) {
      if (OB_NOT_NULL(tasks[i])) {
        tasks[i]->destroy();
        delete tasks[i];
        tasks[i] = NULL;
      }
    }
    for (int64_t i = 0; i < buf_cnt; ++i) {
      if (OB_NOT_NULL(bufs[i])) {
        ::free(bufs[i]);
        bufs[i] = NULL;
      }
    }
    task_cnt = 0;
    buf_cnt = 0;
  }

  char *alloc_aligned_buf(const LSN &begin_lsn, const int64_t len)
  {
    char *buf = NULL;
    void *base = NULL;
    const int64_t phase = begin_lsn.val_ % LOG_DIO_ALIGN_SIZE;
    if (len > 0
        && buf_cnt < MAX_UNIT_BUF_CNT
        && 0 == ::posix_memalign(&base, LOG_DIO_ALIGN_SIZE, len + LOG_DIO_ALIGN_SIZE)) {
      bufs[buf_cnt++] = static_cast<char *>(base);
      buf = static_cast<char *>(base) + phase;
      MEMSET(buf, 0xa5, len);
    }
    return buf;
  }

  LogIOFlushLogTask *make_task(const int64_t log_id,
                               const LSN &begin_lsn,
                               const int64_t len,
                               const char *buf0,
                               const int64_t len0,
                               const char *buf1,
                               const int64_t len1)
  {
    LogIOFlushLogTask *task = NULL;
    LogWriteBuf write_buf;
    share::SCN scn;
    scn.convert_for_logservice(1000 + log_id);
    if (task_cnt < MAX_UNIT_TASK_CNT
        && begin_lsn.is_valid()
        && len > 0
        && OB_NOT_NULL(buf0)
        && len0 > 0
        && len0 + len1 == len) {
      task = new LogIOFlushLogTask(1 /* palf_id */, 0 /* palf_epoch */);
      if (OB_NOT_NULL(task)) {
        int ret = write_buf.push_back(buf0, len0);
        if (OB_SUCCESS == ret && len1 > 0) {
          ret = write_buf.push_back(buf1, len1);
        }
        FlushLogCbCtx cb_ctx(log_id, scn, begin_lsn, 1 /* log_proposal_id */,
                             len, 1 /* curr_log_proposal_id */, 1 /* begin_ts */);
        if (OB_SUCCESS != ret || OB_SUCCESS != task->init(cb_ctx, write_buf)) {
          delete task;
          task = NULL;
        } else {
          tasks[task_cnt++] = task;
        }
      }
    }
    return task;
  }

  LogIOFlushLogTask *make_one_buf_task(const int64_t log_id,
                                       const LSN &begin_lsn,
                                       const int64_t len,
                                       char *buf)
  {
    return make_task(log_id, begin_lsn, len, buf, len, NULL, 0);
  }

  static const int64_t MAX_UNIT_TASK_CNT = 128;
  static const int64_t MAX_UNIT_BUF_CNT = 128;
  int64_t task_cnt;
  int64_t buf_cnt;
  LogIOFlushLogTask *tasks[MAX_UNIT_TASK_CNT];
  char *bufs[MAX_UNIT_BUF_CNT];
};

struct AsyncCtxGuard
{
  AsyncCtxGuard()
    : ctx(OB_NEW(AsyncPalfIOCtx, common::ObMemAttr(TEST_TENANT_ID, "PalfAsyncCtx")))
  {}
  ~AsyncCtxGuard()
  {
    if (OB_NOT_NULL(ctx)) {
      ctx->free_this();
      ctx = NULL;
    }
  }
  AsyncPalfIOCtx *operator->() { return ctx; }
  AsyncPalfIOCtx *get() const { return ctx; }
  AsyncPalfIOCtx *ctx;
};

struct CbPoolGuard
{
  CbPoolGuard() : cb_pool() {}
  ~CbPoolGuard() { cb_pool.destroy(); }
  int init(IPalfEnvImpl *env)
  {
    int ret = cb_pool.init(1 /* log_io_cb_num */, env);
    if (OB_SUCC(ret)) {
      ret = cb_pool.start();
    }
    return ret;
  }
  int get_tg_id() const { return cb_pool.get_tg_id(); }
  LogIOTaskCbThreadPool cb_pool;
};

void init_planner(LogAsyncWritePlanner &planner, const LSN &tail)
{
  ASSERT_EQ(OB_SUCCESS, planner.init());
  planner.init_plan_state(tail);
}

TEST(TestPalfAsyncFragment, CtxInitRejectsInvalidDependencies)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ctx->init(1 /* palf_id */, 0 /* cb_tg_id */,
                      &env, &waker, AsyncThrottleContext()));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                      NULL, &waker, AsyncThrottleContext()));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                      &env, NULL, AsyncThrottleContext()));
  EXPECT_EQ(OB_SUCCESS,
            ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                      &env, &waker, AsyncThrottleContext()));
}

TEST(TestPalfAsyncFragment, CtxHoldsPalfHandleUntilDestroy)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker, AsyncThrottleContext()));
  EXPECT_EQ(1, env.get_guard_get_count());
  EXPECT_EQ(0, env.get_revert_count());
  EXPECT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(1, env.get_guard_get_count());
  EXPECT_EQ(0, env.get_revert_count());

  ctx->destroy();
  EXPECT_EQ(1, env.get_guard_get_count());
  EXPECT_EQ(1, env.get_revert_count());
}

TEST(TestPalfAsyncFragment, CtxRefreshesAsyncErrsimOptions)
{
  static const char *const FRAGMENT_SIZE_ERRSIM =
      "ERRSIM_PALF_ASYNC_FRAGMENT_MAX_SIZE_IN_4K";
  static const char *const WAIT_PARENT_SIZE_ERRSIM =
      "ERRSIM_PALF_ASYNC_WAIT_PARENT_MAX_SIZE_IN_4K";
  static const char *const AIO_DELAY_ERRSIM =
      "ERRSIM_PALF_ASYNC_AIO_DELAY_US";
  TestErrsimGuard fragment_size_guard(FRAGMENT_SIZE_ERRSIM);
  TestErrsimGuard wait_parent_size_guard(WAIT_PARENT_SIZE_ERRSIM);
  TestErrsimGuard aio_delay_guard(AIO_DELAY_ERRSIM);
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker, AsyncThrottleContext()));

  ASSERT_EQ(OB_SUCCESS, fragment_size_guard.set_value(256 /* 1MB */));
  ASSERT_EQ(OB_SUCCESS, wait_parent_size_guard.set_value(128 /* 512KB */));
  ASSERT_EQ(OB_SUCCESS, aio_delay_guard.set_value(-5000 /* 5ms */));
  ctx->last_errsim_options_refresh_ts_ = OB_INVALID_TIMESTAMP;
  ctx->refresh_errsim_options_();
  EXPECT_EQ(256 * LOG_DIO_ALIGN_SIZE, ctx->planner_.get_fragment_max_size());
  EXPECT_EQ(128 * LOG_DIO_ALIGN_SIZE, ctx->planner_.get_wait_parent_max_size());
  EXPECT_EQ(5000, ctx->aio_delay_us_);
  EXPECT_EQ(5000, ctx->planner_.get_aio_delay());

  ASSERT_EQ(OB_SUCCESS, fragment_size_guard.set_value(-512 /* 2MB */));
  ASSERT_EQ(OB_SUCCESS, wait_parent_size_guard.set_value(-256 /* 1MB */));
  ASSERT_EQ(OB_SUCCESS, aio_delay_guard.set_value(7000));
  ctx->last_errsim_options_refresh_ts_ = common::ObClockGenerator::getClock() + 1_s;
  ctx->refresh_errsim_options_();
  EXPECT_EQ(256 * LOG_DIO_ALIGN_SIZE, ctx->planner_.get_fragment_max_size());
  EXPECT_EQ(128 * LOG_DIO_ALIGN_SIZE, ctx->planner_.get_wait_parent_max_size());
  EXPECT_EQ(5000, ctx->aio_delay_us_);
  EXPECT_EQ(5000, ctx->planner_.get_aio_delay());
  ctx->last_errsim_options_refresh_ts_ = OB_INVALID_TIMESTAMP;
  ctx->refresh_errsim_options_();
  EXPECT_EQ(512 * LOG_DIO_ALIGN_SIZE, ctx->planner_.get_fragment_max_size());
  EXPECT_EQ(256 * LOG_DIO_ALIGN_SIZE, ctx->planner_.get_wait_parent_max_size());
  EXPECT_EQ(7000, ctx->aio_delay_us_);
  EXPECT_EQ(7000, ctx->planner_.get_aio_delay());

  ASSERT_EQ(OB_SUCCESS, fragment_size_guard.set_value(0));
  ASSERT_EQ(OB_SUCCESS, wait_parent_size_guard.set_value(0));
  ASSERT_EQ(OB_SUCCESS, aio_delay_guard.set_value(0));
  ctx->last_errsim_options_refresh_ts_ = OB_INVALID_TIMESTAMP;
  ctx->refresh_errsim_options_();
  EXPECT_EQ(NORMAL_FRAGMENT_MAX_SIZE, ctx->planner_.get_fragment_max_size());
  EXPECT_EQ(WAIT_PARENT_FRAGMENT_MAX_SIZE, ctx->planner_.get_wait_parent_max_size());
  EXPECT_EQ(0, ctx->aio_delay_us_);
  EXPECT_EQ(0, ctx->planner_.get_aio_delay());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxReportsAioDelayAsNextDriveInterval)
{
  static const char *const AIO_DELAY_ERRSIM = "ERRSIM_PALF_ASYNC_AIO_DELAY_US";
  static const int64_t AIO_DELAY_US = 60_s;
  TestErrsimGuard aio_delay_guard(AIO_DELAY_ERRSIM);
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  CbPoolGuard cb_pool;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, aio_delay_guard.set_value(AIO_DELAY_US));
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, cb_pool.get_tg_id(),
                                  &env, &waker, AsyncThrottleContext()));
  ctx->planner_.init_plan_state(make_lsn(0));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, ctx->planner_.admit_task(task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, ctx->planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.collect_ready_fragments(
      ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *fragment = ready_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  const FragmentRef fragment_ref = fragment->get_fragment_ref();
  const int64_t now_us = common::ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, fragment->mark_submitted(fragment_ref, now_us));
  bool completed_by_me = false;
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, fragment->mark_io_completed(
      fragment_ref, OB_SUCCESS, 0 /* next_retry_ts */, now_us /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_GT(next_drive_interval_us, 0);
  EXPECT_LE(next_drive_interval_us, AIO_DELAY_US);
  EXPECT_EQ(AIO_DELAY_US, ctx->aio_delay_us_);
  EXPECT_EQ(AIO_DELAY_US, ctx->planner_.get_aio_delay());
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.get_fragment(fragment_ref, fragment));

  ASSERT_EQ(OB_SUCCESS, aio_delay_guard.set_value(0));
  ctx->last_errsim_options_refresh_ts_ = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(INT64_MAX, next_drive_interval_us);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ctx->pool_.get_fragment(fragment_ref, fragment));
  ctx->destroy();
}

void prepare_async_throttle(LogWritingThrottle &throttle, const int64_t unrecyclable_size)
{
  throttle.notify_need_writing_throttling(true);
  // Keep the cached options valid during the case so no real disk-space query
  // or sleep is needed to exercise the ctx deadline state machine.
  throttle.last_update_ts_ = common::ObClockGenerator::getClock() + 60 * 1000 * 1000L;
  // Keep the test deadline well above assertion overhead so it cannot expire
  // between drive_write() and the deadline checks below.
  throttle.decay_factor_ = 1000.0;
  throttle.throttling_options_.total_disk_space_ = 100 * 1024 * 1024L;
  throttle.throttling_options_.stopping_writing_percentage_ = 100;
  throttle.throttling_options_.trigger_percentage_ = 50;
  throttle.throttling_options_.maximum_duration_ = 7200 * 1000 * 1000L;
  throttle.throttling_options_.unrecyclable_disk_space_ = unrecyclable_size;
}

void release_and_advance_finished_prefix(LogAsyncWritePlanner &planner,
                                         PhysicalWriteFragmentPool &pool,
                                         LSN &persisted_lsn)
{
  bool stop = false;
  LSN old_persisted_lsn;
  PlannerStatus status;
  while (!stop) {
    old_persisted_lsn = persisted_lsn;
    ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
    ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
    planner.get_status(status);
    persisted_lsn = status.persisted_lsn_;
    stop = (old_persisted_lsn == persisted_lsn);
  }
}

int64_t find_handle_call_index(const std::vector<RecordedHandleCall> &calls,
                               const RecordedHandleCall::Kind kind,
                               const int64_t begin_idx)
{
  int64_t found_idx = -1;
  for (int64_t i = begin_idx; i < static_cast<int64_t>(calls.size()) && found_idx < 0; ++i) {
    if (kind == calls.at(i).kind) {
      found_idx = i;
    }
  }
  return found_idx;
}

void init_async_completion_event(const RecordedHandleCall &submit_call,
                                 const int ret_code,
                                 const int64_t finish_ts,
                                 AsyncIOCompletionEvent &event)
{
  event.ctx.palf_id = 1;
  event.ctx.fragment_ref = submit_call.fragment_ref;
  event.ctx.begin_lsn = submit_call.submitted_begin_lsn;
  event.ctx.end_lsn = submit_call.submitted_begin_lsn
      + static_cast<offset_t>(submit_call.submitted_write_len);
  event.ret_code = ret_code;
  event.finish_ts = finish_ts;
}

void complete_async_submit(AsyncPalfIOCtx &ctx, const RecordedHandleCall &submit_call)
{
  AsyncIOCompletionEvent event;
  bool need_wake_worker = false;
  init_async_completion_event(submit_call, OB_SUCCESS,
                              common::ObTimeUtility::current_time(), event);
  ASSERT_EQ(OB_SUCCESS, ctx.on_aio_complete(event, need_wake_worker));
  ASSERT_TRUE(need_wake_worker);
}

TEST(TestPalfAsyncFragment, DeletedHandleDiscardsQueuedFlushAndBarrierTasks)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  MutationThenFailControlTask barrier_task(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker, AsyncThrottleContext()));
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *flush_task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(flush_task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(flush_task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_META_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&barrier_task));

  handle.mark_deleted_atomic_only();
  EXPECT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, ctx->task_queue_.get_total());
  EXPECT_EQ(0, ctx->get_inflight_count());
  EXPECT_EQ(1, env.get_free_flush_task_cnt());
  EXPECT_EQ(0, barrier_task.get_execute_count());
  EXPECT_EQ(0, barrier_task.get_callback_count());
  EXPECT_EQ(1, barrier_task.get_free_count());
  EXPECT_EQ(AsyncPalfIOCtx::FLUSH_LOG_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_flush_task_slot_count_));
  EXPECT_EQ(AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, DeletedHandleDrainsCompletedAioWithoutPublishing)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker, AsyncThrottleContext()));
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *flush_task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(flush_task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(flush_task));
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  const int64_t submit_idx = find_handle_call_index(
      handle.calls(), RecordedHandleCall::SUBMIT, 0);
  ASSERT_GE(submit_idx, 0);
  EXPECT_EQ(1, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
  EXPECT_EQ(0, env.get_free_flush_task_cnt());

  handle.mark_deleted_atomic_only();
  complete_async_submit(*ctx.get(), handle.calls().at(submit_idx));
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_LT(find_handle_call_index(handle.calls(), RecordedHandleCall::COMMIT, 0), 0);
  EXPECT_LT(find_handle_call_index(handle.calls(), RecordedHandleCall::ADVANCE_REUSE, 0), 0);
  EXPECT_EQ(0, ctx->pool_.get_used_slot_count());
  EXPECT_EQ(0, ctx->get_inflight_count());
  EXPECT_EQ(1, env.get_free_flush_task_cnt());
  EXPECT_EQ(AsyncPalfIOCtx::FLUSH_LOG_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_flush_task_slot_count_));
  EXPECT_EQ(1, env.get_guard_get_count());
  EXPECT_EQ(0, env.get_revert_count());

  ctx->destroy();
  EXPECT_EQ(1, env.get_revert_count());
}

TEST(TestPalfAsyncFragment, BarrierTaskSlotTracksInflightUntilTaskFinished)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  LogIOFlushMetaTask barrier(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  AsyncPalfIOCtx::AsyncQueueItem *popped = NULL;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker, AsyncThrottleContext()));
  ASSERT_EQ(AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));

  for (int64_t i = 0; i < AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_META_TYPE));
  }
  EXPECT_EQ(0, ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));
  EXPECT_EQ(OB_SIZE_OVERFLOW,
            ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_META_TYPE));
  for (int64_t i = 0; i < AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY; ++i) {
    ctx->release_task_slot(LogIOTaskType::FLUSH_META_TYPE);
  }

  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_META_TYPE));
  EXPECT_EQ(AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY - 1,
            ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));
  EXPECT_EQ(1, ctx->get_inflight_count());
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&barrier));
  EXPECT_EQ(AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY - 1,
            ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));
  EXPECT_EQ(1, ctx->get_inflight_count());
  ASSERT_EQ(OB_SUCCESS, ctx->pop_task_queue_item_(popped));
  ASSERT_TRUE(OB_NOT_NULL(popped));
  EXPECT_EQ(AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY - 1,
            ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));
  EXPECT_EQ(1, ctx->get_inflight_count());
  ctx->free_task_queue_item_(popped);
  EXPECT_TRUE(OB_ISNULL(popped));
  ctx->release_task_slot(LogIOTaskType::FLUSH_META_TYPE);
  EXPECT_EQ(AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));
  EXPECT_EQ(0, ctx->get_inflight_count());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, FlushTaskSlotTracksInflightUntilTaskFinished)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  AsyncPalfIOCtx::AsyncQueueItem *popped = NULL;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker, AsyncThrottleContext()));
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));

  EXPECT_EQ(AsyncPalfIOCtx::FLUSH_LOG_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_flush_task_slot_count_));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  EXPECT_EQ(1, ctx->get_inflight_count());
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));
  EXPECT_EQ(1, ctx->get_inflight_count());
  ASSERT_EQ(OB_SUCCESS, ctx->pop_task_queue_item_(popped));
  ASSERT_TRUE(OB_NOT_NULL(popped));
  EXPECT_EQ(AsyncPalfIOCtx::FLUSH_LOG_TASK_QUEUE_CAPACITY - 1,
            ATOMIC_LOAD(&ctx->available_flush_task_slot_count_));
  EXPECT_EQ(1, ctx->get_inflight_count());
  ctx->free_task_queue_item_(popped);
  ctx->release_task_slot(LogIOTaskType::FLUSH_LOG_TYPE);
  EXPECT_EQ(AsyncPalfIOCtx::FLUSH_LOG_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_flush_task_slot_count_));
  EXPECT_EQ(0, ctx->get_inflight_count());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxDoesNotRetryFailedControlBarrier)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  MutationThenFailControlTask task(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */, &env, &waker,
                                  AsyncThrottleContext()));
  ctx->planner_.init_plan_state(make_lsn(0));
  ASSERT_EQ(OB_SUCCESS,
      ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_META_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&task));

  EXPECT_EQ(OB_EAGAIN, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);
  EXPECT_EQ(1, task.get_execute_count());
  EXPECT_TRUE(OB_ISNULL(ctx->control_barrier_task_));
  EXPECT_EQ(AsyncPalfIOCtx::BARRIER_TASK_QUEUE_CAPACITY,
            ATOMIC_LOAD(&ctx->available_barrier_task_slot_count_));
  EXPECT_EQ(0, ctx->get_inflight_count());
  EXPECT_EQ(0, task.get_callback_count());
  EXPECT_EQ(1, task.get_free_count());

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(1, task.get_execute_count());
  EXPECT_EQ(0, task.get_callback_count());
  EXPECT_EQ(1, task.get_free_count());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxThrottleDeadlineCanShrinkAndBypassesOnce)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  LogWritingThrottle throttle;
  NeedPurgingThrottlingFunc purge_func = [](){ return false; };
  int64_t purge_task_count = 0;
  bool can_admit = true;
  int64_t current_next_admit_ts = 0;
  int64_t next_admit_ts = 0;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  prepare_async_throttle(throttle, 80 * 1024 * 1024L);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker,
                                  AsyncThrottleContext(&throttle, &purge_func, &purge_task_count)));

  ASSERT_EQ(OB_SUCCESS, ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                                  can_admit, next_admit_ts));
  EXPECT_FALSE(can_admit);
  EXPECT_GT(next_admit_ts, common::ObClockGenerator::getClock());
  EXPECT_TRUE(ctx->ignore_throttle_once_);
  EXPECT_EQ(1, throttle.stat_.total_skipped_task_cnt_);

  const int64_t first_next_admit_ts = next_admit_ts;
  current_next_admit_ts = next_admit_ts;
  throttle.last_update_ts_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                      can_admit, next_admit_ts));
  EXPECT_FALSE(can_admit);
  EXPECT_EQ(first_next_admit_ts, next_admit_ts);
  EXPECT_TRUE(ctx->ignore_throttle_once_);

  throttle.last_update_ts_ = common::ObClockGenerator::getClock() + 60 * 1000 * 1000L;
  ASSERT_EQ(OB_SUCCESS, ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                                  can_admit, next_admit_ts));
  EXPECT_FALSE(can_admit);
  EXPECT_EQ(first_next_admit_ts, next_admit_ts);
  EXPECT_EQ(1, throttle.stat_.total_skipped_task_cnt_);

  throttle.throttling_options_.unrecyclable_disk_space_ = 60 * 1024 * 1024L;
  ASSERT_EQ(OB_SUCCESS, ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                                  can_admit, next_admit_ts));
  EXPECT_FALSE(can_admit);
  EXPECT_LT(next_admit_ts, first_next_admit_ts);
  EXPECT_EQ(1, throttle.stat_.total_skipped_task_cnt_);

  current_next_admit_ts = next_admit_ts;
  throttle.throttling_options_.unrecyclable_disk_space_ = 10 * 1024 * 1024L;
  ASSERT_EQ(OB_SUCCESS, ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                                  can_admit, next_admit_ts));
  EXPECT_TRUE(can_admit);
  EXPECT_EQ(0, next_admit_ts);
  EXPECT_FALSE(ctx->ignore_throttle_once_);

  current_next_admit_ts = next_admit_ts;
  throttle.throttling_options_.unrecyclable_disk_space_ = 80 * 1024 * 1024L;
  ASSERT_EQ(OB_SUCCESS, ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                                  can_admit, next_admit_ts));
  EXPECT_FALSE(can_admit);
  EXPECT_TRUE(ctx->ignore_throttle_once_);
  current_next_admit_ts = common::ObClockGenerator::getClock() - 1;
  ASSERT_EQ(OB_SUCCESS, ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                                  can_admit, next_admit_ts));
  EXPECT_TRUE(can_admit);
  EXPECT_EQ(0, next_admit_ts);
  EXPECT_FALSE(ctx->ignore_throttle_once_);

  current_next_admit_ts = next_admit_ts;
  ASSERT_EQ(OB_SUCCESS, ctx->can_admit_new_entry_(1 << 20, current_next_admit_ts,
                                                  can_admit, next_admit_ts));
  EXPECT_FALSE(can_admit);
  EXPECT_GT(next_admit_ts, common::ObClockGenerator::getClock());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxThrottleDeadlineDrainsQueuedTasks)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  CbPoolGuard cb_pool;
  LogWritingThrottle throttle;
  NeedPurgingThrottlingFunc purge_func = [](){ return false; };
  int64_t purge_task_count = 0;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  prepare_async_throttle(throttle, 80 * 1024 * 1024L);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, cb_pool.get_tg_id(),
                                  &env, &waker,
                                  AsyncThrottleContext(&throttle, &purge_func, &purge_task_count)));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), 2048, buf);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(2048), 2048, buf + 2048);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task0));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task1));

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_GT(next_drive_interval_us, 0);
  EXPECT_LT(next_drive_interval_us, INT64_MAX);
  EXPECT_EQ(2, ctx->task_queue_.get_total());
  EXPECT_EQ(0, static_cast<int64_t>(handle.calls().size()));
  EXPECT_GT(ctx->throttle_next_admit_ts_, common::ObClockGenerator::getClock());

  ctx->throttle_next_admit_ts_ = common::ObClockGenerator::getClock() - 1;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, ctx->task_queue_.get_total());
  EXPECT_FALSE(ctx->ignore_throttle_once_);
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  int64_t submit_idx = -1;
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    if (RecordedHandleCall::SUBMIT == calls.at(i).kind) {
      submit_idx = i;
    }
  }
  ASSERT_GE(submit_idx, 0);
  const RecordedHandleCall &submit_call = calls.at(submit_idx);

  AsyncIOCompletionEvent event;
  event.ctx.palf_id = 1;
  event.ctx.fragment_ref = submit_call.fragment_ref;
  event.ctx.begin_lsn = submit_call.submitted_begin_lsn;
  event.ctx.end_lsn = submit_call.submitted_begin_lsn
      + static_cast<offset_t>(submit_call.submitted_write_len);
  event.ret_code = OB_SUCCESS;
  event.finish_ts = common::ObTimeUtility::current_time();
  bool need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  ASSERT_TRUE(need_wake_worker);
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  for (int64_t i = 0; i < 100 && env.get_free_flush_task_cnt() < 2; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(2, env.get_free_flush_task_cnt());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxThrottleErrorSchedulesRetry)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  LogWritingThrottle throttle;
  NeedPurgingThrottlingFunc purge_func = [](){ return false; };
  int64_t purge_task_count = 0;
  bool can_admit = true;
  int64_t next_drive_interval_us = 0;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  throttle.notify_need_writing_throttling(true);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker,
                                  AsyncThrottleContext(&throttle, &purge_func, &purge_task_count)));
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  const int64_t before_check_ts = common::ObClockGenerator::getClock();
  ctx->check_throttle_admission_(can_admit);
  EXPECT_FALSE(can_admit);
  EXPECT_EQ(1, ctx->throttle_error_count_);
  EXPECT_GT(ctx->throttle_next_admit_ts_, before_check_ts);
  EXPECT_FALSE(ctx->ignore_throttle_once_);
  const int64_t first_retry_ts = ctx->throttle_next_admit_ts_;
  ctx->check_throttle_admission_(can_admit);
  EXPECT_FALSE(can_admit);
  EXPECT_EQ(2, ctx->throttle_error_count_);
  EXPECT_EQ(first_retry_ts, ctx->throttle_next_admit_ts_);
  ASSERT_EQ(OB_SUCCESS, ctx->get_next_drive_interval_(next_drive_interval_us));
  EXPECT_GT(next_drive_interval_us, 0);
  EXPECT_LT(next_drive_interval_us, INT64_MAX);

  AsyncPalfIOCtx::AsyncQueueItem *item = NULL;
  ASSERT_EQ(OB_SUCCESS, ctx->pop_task_queue_item_(item));
  ASSERT_TRUE(OB_NOT_NULL(item));
  item->payload_ = NULL;
  ctx->free_task_queue_item_(item);
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxThrottleDoesNotBlockPendingPlannerWork)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  CbPoolGuard cb_pool;
  LogWritingThrottle throttle;
  NeedPurgingThrottlingFunc purge_func = [](){ return false; };
  int64_t purge_task_count = 0;
  bool consumed = false;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  prepare_async_throttle(throttle, 80 * 1024 * 1024L);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, cb_pool.get_tg_id(),
                                  &env, &waker,
                                  AsyncThrottleContext(&throttle, &purge_func, &purge_task_count)));
  ctx->planner_.init_plan_state(make_lsn(0));
  ctx->current_write_block_id_ = 0;

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), 2048, buf);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(2048), 2048, buf + 2048);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  ASSERT_EQ(OB_SUCCESS, ctx->planner_.admit_task(task0, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task1));

  ASSERT_EQ(OB_SUCCESS, ctx->drive_phase_task_(&handle));
  PlannerStatus status;
  ctx->planner_.get_status(status);
  EXPECT_EQ(1, ctx->task_queue_.get_total());
  EXPECT_EQ(1, status.get_pending_task_count());
  EXPECT_EQ(1, status.get_active_fragment_count());
  EXPECT_FALSE(status.has_pending_source());
  EXPECT_GT(ctx->throttle_next_admit_ts_, common::ObClockGenerator::getClock());

  ASSERT_EQ(OB_SUCCESS, ctx->drive_phase_fragment_(&handle));
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  int64_t submit_idx = -1;
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    if (RecordedHandleCall::SUBMIT == calls.at(i).kind) {
      submit_idx = i;
    }
  }
  ASSERT_GE(submit_idx, 0);
  const RecordedHandleCall &submit_call = calls.at(submit_idx);
  AsyncIOCompletionEvent event;
  event.ctx.palf_id = 1;
  event.ctx.fragment_ref = submit_call.fragment_ref;
  event.ctx.begin_lsn = submit_call.submitted_begin_lsn;
  event.ctx.end_lsn = submit_call.submitted_begin_lsn
      + static_cast<offset_t>(submit_call.submitted_write_len);
  event.ret_code = OB_SUCCESS;
  event.finish_ts = common::ObTimeUtility::current_time();
  bool need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  ASSERT_TRUE(need_wake_worker);
  ASSERT_EQ(OB_SUCCESS, ctx->drive_phase_completion_(&handle));

  AsyncPalfIOCtx::AsyncQueueItem *item = NULL;
  ASSERT_EQ(OB_SUCCESS, ctx->pop_task_queue_item_(item));
  ASSERT_TRUE(OB_NOT_NULL(item));
  item->payload_ = NULL;
  ctx->free_task_queue_item_(item);
  for (int64_t i = 0; i < 100 && env.get_free_flush_task_cnt() < 1; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, env.get_free_flush_task_cnt());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxPurgeBarrierBypassesThrottleAndReleasesCount)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  LogWritingThrottle throttle;
  int64_t purge_task_count = 0;
  NeedPurgingThrottlingFunc purge_func = [&purge_task_count]() {
    return purge_task_count > 0;
  };
  LogIOPurgeThrottlingTask purge_task(1 /* palf_id */, 0 /* palf_epoch */);
  PurgeThrottlingCbCtx purge_ctx(PURGE_BY_RECONFIRM);
  AsyncCtxGuard ctx;
  CbPoolGuard cb_pool;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  prepare_async_throttle(throttle, 80 * 1024 * 1024L);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  cb_pool.get_tg_id(), &env, &waker,
                                  AsyncThrottleContext(
                                      &throttle, &purge_func, &purge_task_count)));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *flush_task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(flush_task));
  ASSERT_EQ(OB_SUCCESS, purge_task.init(purge_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(flush_task));
  ASSERT_EQ(OB_SUCCESS,
      ctx->try_reserve_task_slot(LogIOTaskType::PURGE_THROTTLING_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&purge_task));
  EXPECT_EQ(1, purge_task_count);

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(1, ctx->task_queue_.get_total());
  EXPECT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  const int64_t submit_idx = find_handle_call_index(
      handle.calls(), RecordedHandleCall::SUBMIT, 0);
  ASSERT_GE(submit_idx, 0);
  complete_async_submit(*ctx.get(), handle.calls().at(submit_idx));

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);
  EXPECT_EQ(1, purge_task_count);
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, throttle.appended_log_size_cur_round_);
  EXPECT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, purge_task_count);
  EXPECT_TRUE(OB_ISNULL(ctx->control_barrier_task_));

  for (int64_t i = 0; i < 100 && (env.get_free_flush_task_cnt() < 1
      || env.get_free_purge_task_cnt() < 1); ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, env.get_free_flush_task_cnt());
  EXPECT_EQ(1, env.get_free_purge_task_cnt());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxSubmitsCompletesAndPublishesTasks)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  CbPoolGuard cb_pool;
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                cb_pool.get_tg_id(), &env, &waker,
                                AsyncThrottleContext()));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), 2048, buf);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(2048), 2048, buf + 2048);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task0));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task1));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  const std::vector<RecordedHandleCall> &submit_calls = handle.calls();
  int64_t submit_idx = -1;
  for (int64_t i = 0; i < static_cast<int64_t>(submit_calls.size()); ++i) {
    if (RecordedHandleCall::SUBMIT == submit_calls.at(i).kind) {
      submit_idx = i;
    }
  }
  ASSERT_GE(submit_idx, 0);
  const RecordedHandleCall &submit_call = submit_calls.at(submit_idx);
  EXPECT_EQ(make_lsn(0), submit_call.submitted_begin_lsn);
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, submit_call.submitted_write_len);
  ASSERT_TRUE(submit_call.fragment_ref.is_valid());
  EXPECT_GT(ctx->get_inflight_count(), 0);

  AsyncIOCompletionEvent event;
  event.ctx.palf_id = 1;
  event.ctx.fragment_ref = submit_call.fragment_ref;
  event.ctx.begin_lsn = submit_call.submitted_begin_lsn;
  event.ctx.end_lsn = submit_call.submitted_begin_lsn
      + static_cast<offset_t>(submit_call.submitted_write_len);
  event.ret_code = OB_SUCCESS;
  event.finish_ts = common::ObTimeUtility::current_time();
  bool need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  EXPECT_TRUE(need_wake_worker);
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));

  int64_t commit_cnt = 0;
  int64_t reuse_cnt = 0;
  LSN last_commit_end;
  LSN last_reuse_lsn;
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    if (RecordedHandleCall::COMMIT == calls.at(i).kind) {
      ++commit_cnt;
      last_commit_end = calls.at(i).end_lsn;
    } else if (RecordedHandleCall::ADVANCE_REUSE == calls.at(i).kind) {
      ++reuse_cnt;
      last_reuse_lsn = calls.at(i).end_lsn;
    }
  }
  EXPECT_EQ(2, commit_cnt);
  EXPECT_EQ(2, reuse_cnt);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), last_commit_end);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), last_reuse_lsn);
  for (int64_t i = 0; i < 100 && env.get_free_flush_task_cnt() < 2; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(2, env.get_free_flush_task_cnt());
  EXPECT_EQ(0, ctx->get_inflight_count());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxFinishesPublishAfterCallbackPoolStops)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  CbPoolGuard cb_pool;
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  cb_pool.get_tg_id(), &env, &waker,
                                  AsyncThrottleContext()));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  int64_t submit_idx = -1;
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    if (RecordedHandleCall::SUBMIT == calls.at(i).kind) {
      submit_idx = i;
    }
  }
  ASSERT_GE(submit_idx, 0);
  const RecordedHandleCall &submit_call = calls.at(submit_idx);
  ASSERT_TRUE(submit_call.fragment_ref.is_valid());

  ASSERT_EQ(OB_SUCCESS, cb_pool.cb_pool.stop());
  AsyncIOCompletionEvent event;
  event.ctx.palf_id = 1;
  event.ctx.fragment_ref = submit_call.fragment_ref;
  event.ctx.begin_lsn = submit_call.submitted_begin_lsn;
  event.ctx.end_lsn = submit_call.submitted_begin_lsn
      + static_cast<offset_t>(submit_call.submitted_write_len);
  event.ret_code = OB_SUCCESS;
  event.finish_ts = common::ObTimeUtility::current_time();
  bool need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  ASSERT_TRUE(need_wake_worker);
  EXPECT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, ctx->get_inflight_count());
  EXPECT_EQ(1, env.get_free_flush_task_cnt());
  EXPECT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(1, env.get_free_flush_task_cnt());
  ASSERT_EQ(OB_SUCCESS, cb_pool.cb_pool.wait());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, BlockSwitchPendingAloneDoesNotPinCtx)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  1 /* cb_tg_id */, &env, &waker,
                                  AsyncThrottleContext()));

  ctx->block_switch_pending_ = true;
  ctx->block_switch_pending_since_ts_ = common::ObTimeUtility::current_time();
  EXPECT_EQ(0, ctx->get_inflight_count());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxPreparesFirstBlockBeforeFirstSubmit)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  CbPoolGuard cb_pool;
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  handle.set_planned_tail(make_lsn(0));
  handle.set_planned_writable_size(0);
  handle.set_need_block_header(true);
  handle.set_model_real_submit_gates(true);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  cb_pool.get_tg_id(), &env, &waker,
                                  AsyncThrottleContext()));
  EXPECT_FALSE(ctx->block_switch_pending_);

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, make_lsn(0),
                                                    159 /* data_len */, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));

  const std::vector<RecordedHandleCall> &calls = handle.calls();
  int64_t switch_idx = -1;
  int64_t header_idx = -1;
  int64_t submit_idx = -1;
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    if (RecordedHandleCall::SWITCH_BLOCK == calls.at(i).kind && switch_idx < 0) {
      switch_idx = i;
    } else if (RecordedHandleCall::BLOCK_HEADER == calls.at(i).kind && header_idx < 0) {
      header_idx = i;
    } else if (RecordedHandleCall::SUBMIT == calls.at(i).kind && submit_idx < 0) {
      submit_idx = i;
    }
  }
  EXPECT_GE(switch_idx, 0);
  EXPECT_GE(header_idx, 0);
  EXPECT_GE(submit_idx, 0);
  EXPECT_LT(switch_idx, submit_idx);
  EXPECT_LT(header_idx, submit_idx);
  EXPECT_FALSE(ctx->block_switch_pending_);
  EXPECT_GT(ctx->get_inflight_count(), 0);

  const RecordedHandleCall &submit_call = calls.at(submit_idx);
  AsyncIOCompletionEvent event;
  event.ctx.palf_id = 1;
  event.ctx.fragment_ref = submit_call.fragment_ref;
  event.ctx.begin_lsn = submit_call.submitted_begin_lsn;
  event.ctx.end_lsn = submit_call.submitted_begin_lsn
      + static_cast<offset_t>(submit_call.submitted_write_len);
  event.ret_code = OB_SUCCESS;
  event.finish_ts = common::ObTimeUtility::current_time();
  bool need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, ctx->get_inflight_count());
  for (int64_t i = 0; i < 100 && env.get_free_flush_task_cnt() < 1; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, env.get_free_flush_task_cnt());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxRefreshesTailPrefixOnFirstDrive)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  const offset_t prefix_len = 123;
  const LSN tail_lsn = make_lsn(LOG_DIO_ALIGN_SIZE + prefix_len);
  PlannerStatus status;
  handle.set_planned_tail(tail_lsn);
  handle.set_tail_page_fill(0x5a);
  handle.set_tail_page_read_size(LOG_DIO_ALIGN_SIZE);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                1 /* cb_tg_id */, &env, &waker,
                                AsyncThrottleContext()));
  ctx->planner_.get_status(status);
  EXPECT_FALSE(status.planned_end_lsn_.is_valid());

  char *buf = arena.alloc_aligned_buf(tail_lsn, LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, tail_lsn, LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  ctx->planner_.get_status(status);
  EXPECT_EQ(tail_lsn + static_cast<offset_t>(LOG_DIO_ALIGN_SIZE), status.planned_end_lsn_);
  EXPECT_EQ(1, handle.get_tail_page_read_cnt());
  EXPECT_EQ(1, handle.get_tail_prefix_fill_cnt());
  EXPECT_EQ(prefix_len, handle.get_tail_prefix_fill_size());
  EXPECT_EQ(static_cast<unsigned char>(0x5a), handle.get_tail_prefix_fill_first_byte());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxDoesNotRefreshPlannerBeforeFrontBarrier)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  LogIOFlushMetaTask barrier(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  LogWritingThrottle throttle;
  NeedPurgingThrottlingFunc purge_func = [](){ return false; };
  int64_t purge_task_count = 0;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  PlannerStatus status;
  handle.set_planned_tail(make_lsn(0));
  prepare_async_throttle(throttle, 80 * 1024 * 1024L);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                1 /* cb_tg_id */, &env, &waker,
                                AsyncThrottleContext(&throttle, &purge_func, &purge_task_count)));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_META_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&barrier));

  int64_t next_drive_interval_us = INT64_MAX;
  // The front barrier is staged and executed before planner refresh. This stack
  // flush-meta task is intentionally not initialized, so the drive returns the
  // barrier error, but planner refresh must still be skipped.
  ASSERT_EQ(OB_NOT_INIT, ctx->drive_write(next_drive_interval_us));
  ctx->planner_.get_status(status);
  EXPECT_FALSE(status.planned_end_lsn_.is_valid());
  EXPECT_EQ(0, handle.get_tail_page_read_cnt());
  EXPECT_EQ(0, handle.get_tail_prefix_fill_cnt());
  EXPECT_EQ(0, ctx->throttle_error_count_);
  EXPECT_EQ(0, ctx->throttle_next_admit_ts_);
  EXPECT_EQ(0, throttle.stat_.total_skipped_task_cnt_);

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxDoesNotRefreshPlannerBetweenTailBarriers)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  const LSN old_tail = make_lsn(LOG_DIO_ALIGN_SIZE);
  const LSN first_tail = make_lsn(PALF_BLOCK_SIZE);
  const LSN second_tail = make_lsn(2 * PALF_BLOCK_SIZE);
  LogIOTruncatePrefixBlocksTask first_barrier(1 /* palf_id */, 0 /* palf_epoch */);
  LogIOTruncatePrefixBlocksTask second_barrier(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  TruncatePrefixBlocksCbCtx first_cb_ctx(first_tail);
  TruncatePrefixBlocksCbCtx second_cb_ctx(second_tail);
  CbPoolGuard cb_pool;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  handle.set_planned_tail(old_tail);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  cb_pool.get_tg_id(), &env, &waker,
                                  AsyncThrottleContext()));
  ctx->planner_.init_plan_state(old_tail);

  ASSERT_EQ(OB_SUCCESS, first_barrier.init(first_cb_ctx));
  ASSERT_EQ(OB_SUCCESS, second_barrier.init(second_cb_ctx));
  ASSERT_EQ(OB_SUCCESS,
      ctx->try_reserve_task_slot(LogIOTaskType::TRUNCATE_PREFIX_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&first_barrier));
  ASSERT_EQ(OB_SUCCESS,
      ctx->try_reserve_task_slot(LogIOTaskType::TRUNCATE_PREFIX_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&second_barrier));

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);
  EXPECT_EQ(1, handle.get_truncate_prefix_cnt());
  EXPECT_EQ(0, handle.get_async_storage_snapshot_cnt());
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(2, handle.get_truncate_prefix_cnt());
  EXPECT_EQ(second_tail, handle.get_truncate_prefix_lsn());
  EXPECT_EQ(0, handle.get_async_storage_snapshot_cnt());
  for (int64_t i = 0; i < 100 && handle.get_after_truncate_prefix_cnt() < 2; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(2, handle.get_after_truncate_prefix_cnt());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxRefreshesPlannerWhenFlushFollowsTailBarrier)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  const LSN old_tail = make_lsn(LOG_DIO_ALIGN_SIZE);
  const LSN new_tail = make_lsn(PALF_BLOCK_SIZE);
  LogIOTruncatePrefixBlocksTask barrier(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  TruncatePrefixBlocksCbCtx cb_ctx(new_tail);
  CbPoolGuard cb_pool;
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  handle.set_planned_tail(old_tail);
  handle.set_model_real_submit_gates(true);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  cb_pool.get_tg_id(), &env, &waker,
                                  AsyncThrottleContext()));
  ctx->planner_.init_plan_state(old_tail);

  ASSERT_EQ(OB_SUCCESS, barrier.init(cb_ctx));
  ASSERT_EQ(OB_SUCCESS,
      ctx->try_reserve_task_slot(LogIOTaskType::TRUNCATE_PREFIX_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(&barrier));
  char *buf = arena.alloc_aligned_buf(new_tail, LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *flush_task = arena.make_one_buf_task(
      1 /* log_id */, new_tail, LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(flush_task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(flush_task));

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);
  EXPECT_EQ(0, handle.get_async_storage_snapshot_cnt());
  EXPECT_LT(find_handle_call_index(handle.calls(), RecordedHandleCall::SUBMIT, 0), 0);

  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(1, handle.get_async_storage_snapshot_cnt());
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  const int64_t switch_idx = find_handle_call_index(calls, RecordedHandleCall::SWITCH_BLOCK, 0);
  const int64_t header_idx = find_handle_call_index(calls, RecordedHandleCall::BLOCK_HEADER, 0);
  const int64_t submit_idx = find_handle_call_index(calls, RecordedHandleCall::SUBMIT, 0);
  ASSERT_GE(switch_idx, 0);
  ASSERT_GE(header_idx, 0);
  ASSERT_GE(submit_idx, 0);
  EXPECT_LT(switch_idx, submit_idx);
  EXPECT_LT(header_idx, submit_idx);
  complete_async_submit(*ctx.get(), calls.at(submit_idx));
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, ctx->get_inflight_count());
  for (int64_t i = 0; i < 100 && (handle.get_after_truncate_prefix_cnt() < 1
      || env.get_free_flush_task_cnt() < 1); ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, handle.get_after_truncate_prefix_cnt());
  EXPECT_EQ(1, env.get_free_flush_task_cnt());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxSkipsPlannerRefreshWhenQueueEmpty)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  const offset_t prefix_len = 123;
  const LSN tail_lsn = make_lsn(LOG_DIO_ALIGN_SIZE + prefix_len);
  PlannerStatus status;
  handle.set_planned_tail(tail_lsn);
  handle.set_tail_page_fill(0x5a);
  handle.set_tail_page_read_size(LOG_DIO_ALIGN_SIZE);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                1 /* cb_tg_id */, &env, &waker,
                                AsyncThrottleContext()));
  ctx->block_switch_pending_ = true;

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  ctx->planner_.get_status(status);
  EXPECT_FALSE(status.planned_end_lsn_.is_valid());
  EXPECT_EQ(0, handle.get_tail_page_read_cnt());
  EXPECT_EQ(0, handle.get_tail_prefix_fill_cnt());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxSwitchesBlockBeforePlanningNextBlockFragment)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  CbPoolGuard cb_pool;
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  const LSN begin_lsn = make_lsn(PALF_BLOCK_SIZE - LOG_DIO_ALIGN_SIZE);
  handle.set_planned_tail(begin_lsn);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  cb_pool.get_tg_id(), &env, &waker,
                                  AsyncThrottleContext()));

  char *buf0 = arena.alloc_aligned_buf(begin_lsn, LOG_DIO_ALIGN_SIZE);
  char *buf1 = arena.alloc_aligned_buf(make_lsn(PALF_BLOCK_SIZE), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, begin_lsn,
                                                     LOG_DIO_ALIGN_SIZE,
                                                     buf0);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(PALF_BLOCK_SIZE),
                                                     LOG_DIO_ALIGN_SIZE,
                                                     buf1);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task0));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task1));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  const std::vector<RecordedHandleCall> &first_calls = handle.calls();
  int64_t submit_cnt = 0;
  int64_t switch_cnt = 0;
  FragmentRef first_ref;
  for (int64_t i = 0; i < static_cast<int64_t>(first_calls.size()); ++i) {
    if (RecordedHandleCall::SUBMIT == first_calls.at(i).kind) {
      ++submit_cnt;
      first_ref = first_calls.at(i).fragment_ref;
      EXPECT_EQ(begin_lsn, first_calls.at(i).submitted_begin_lsn);
      EXPECT_EQ(LOG_DIO_ALIGN_SIZE, first_calls.at(i).submitted_write_len);
    } else if (RecordedHandleCall::SWITCH_BLOCK == first_calls.at(i).kind) {
      ++switch_cnt;
    }
  }
  EXPECT_EQ(1, submit_cnt);
  EXPECT_EQ(0, switch_cnt);
  ASSERT_TRUE(first_ref.is_valid());

  AsyncIOCompletionEvent event;
  event.ctx.palf_id = 1;
  event.ctx.fragment_ref = first_ref;
  event.ctx.begin_lsn = begin_lsn;
  event.ctx.end_lsn = make_lsn(PALF_BLOCK_SIZE);
  event.ret_code = OB_SUCCESS;
  event.finish_ts = common::ObTimeUtility::current_time();
  bool need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  EXPECT_TRUE(need_wake_worker);
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));

  const std::vector<RecordedHandleCall> &calls = handle.calls();
  submit_cnt = 0;
  switch_cnt = 0;
  bool has_next_block_submit = false;
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    if (RecordedHandleCall::SUBMIT == calls.at(i).kind) {
      ++submit_cnt;
      if (make_lsn(PALF_BLOCK_SIZE) == calls.at(i).submitted_begin_lsn) {
        has_next_block_submit = true;
      }
    } else if (RecordedHandleCall::SWITCH_BLOCK == calls.at(i).kind) {
      ++switch_cnt;
    }
  }
  EXPECT_EQ(2, submit_cnt);
  EXPECT_EQ(1, switch_cnt);
  EXPECT_TRUE(has_next_block_submit);

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxDoesNotPrepareNextBlockBeforePersistedBoundary)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  const LSN persisted_lsn = make_lsn(PALF_BLOCK_SIZE - LOG_DIO_ALIGN_SIZE);
  const LSN block_end_lsn = make_lsn(PALF_BLOCK_SIZE);
  handle.set_planned_tail(persisted_lsn);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                1 /* cb_tg_id */, &env, &waker,
                                AsyncThrottleContext()));

  ctx->planner_.persisted_lsn_ = persisted_lsn;
  ctx->planner_.queue_end_lsn_ = block_end_lsn;

  char *buf = arena.alloc_aligned_buf(block_end_lsn, LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, block_end_lsn,
                                                    LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    EXPECT_NE(RecordedHandleCall::SWITCH_BLOCK, calls.at(i).kind);
    EXPECT_NE(RecordedHandleCall::BLOCK_HEADER, calls.at(i).kind);
    EXPECT_NE(RecordedHandleCall::SUBMIT, calls.at(i).kind);
  }

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxPropagatesStateNotMatchWhenSubmitSeesMissingBlockHeader)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  const LSN begin_lsn = make_lsn(0);
  handle.set_planned_tail(begin_lsn);
  handle.set_model_real_submit_gates(true);
  handle.set_need_block_header(true);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                1 /* cb_tg_id */, &env, &waker,
                                AsyncThrottleContext()));

  char *buf = arena.alloc_aligned_buf(begin_lsn, LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, begin_lsn,
                                                    LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  int64_t next_drive_interval_us = INT64_MAX;
  EXPECT_EQ(OB_STATE_NOT_MATCH, ctx->drive_write(next_drive_interval_us));
  EXPECT_FALSE(ctx->block_switch_pending_);
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    EXPECT_NE(RecordedHandleCall::SWITCH_BLOCK, calls.at(i).kind);
    EXPECT_NE(RecordedHandleCall::BLOCK_HEADER, calls.at(i).kind);
  }

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxRejectsSubmitFromUnexpectedBlock)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  PhysicalWriteFragment fragment;
  FragmentRef ref;
  const LSN begin_lsn = make_lsn(PALF_BLOCK_SIZE);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                1 /* cb_tg_id */, &env, &waker,
                                AsyncThrottleContext()));
  char *buf = arena.alloc_aligned_buf(begin_lsn, LOG_DIO_ALIGN_SIZE);
  int64_t planned_len = 0;
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, fragment.alloc_from_free(0 /* slot_id */, begin_lsn,
                                                buf, LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE,
                                                FragmentRef(), ref, planned_len));
  ASSERT_EQ(LOG_DIO_ALIGN_SIZE, planned_len);
  ctx->current_write_block_id_ = 0;

  EXPECT_EQ(OB_ERR_UNEXPECTED, ctx->submit_fragment_(&handle, fragment));
  EXPECT_EQ(0, static_cast<int64_t>(handle.calls().size()));
  EXPECT_TRUE(fragment.is_ready());
  EXPECT_EQ(0, ctx->get_inflight_count());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxRetriesSubmittedFragmentAfterIOHandleCheckFails)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  PhysicalWriteFragment *fragment = NULL;
  FragmentRef ref;
  int64_t planned_len = 0;
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  1 /* cb_tg_id */, &env, &waker,
                                  AsyncThrottleContext()));
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.alloc_slot(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE,
                                              LOG_DIO_ALIGN_SIZE, FragmentRef(), ref, planned_len));
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.get_fragment(ref, fragment));
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  ctx->current_write_block_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, ctx->submit_fragment_(&handle, *fragment));
  ASSERT_TRUE(fragment->is_submitted());
  ASSERT_EQ(2, ctx->get_inflight_count());
  ASSERT_EQ(1, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));

  const int64_t now = common::ObTimeUtility::current_time();
  EXPECT_EQ(OB_NOT_INIT, ctx->poll_submitted_fragment_(*fragment, now));
  EXPECT_TRUE(fragment->is_failed());
  EXPECT_EQ(OB_NOT_INIT, fragment->get_ret_code());
  EXPECT_GT(fragment->get_next_retry_ts(), now);
  EXPECT_EQ(1, ctx->get_inflight_count());
  EXPECT_EQ(0, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
  EXPECT_EQ(OB_SUCCESS, ctx->poll_submitted_fragment_(*fragment, now));
  EXPECT_EQ(1, ctx->get_inflight_count());

  ASSERT_EQ(OB_SUCCESS, ctx->pool_.free_slot(ref));
  EXPECT_EQ(0, ctx->get_inflight_count());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxPollsFinishedIoHandleAndRetriesDeviceFailure)
{
  const int io_results[] = {OB_SUCCESS, OB_IO_ERROR};
  const int64_t io_result_count = static_cast<int64_t>(
      sizeof(io_results) / sizeof(io_results[0]));
  for (int64_t i = 0; i < io_result_count; ++i) {
    share::ObLocalDevice local_device;
    common::ObIOResult io_result;
    UnitTaskArena arena;
    MockAsyncPalfHandleImpl handle;
    MockPalfEnvImpl env(handle);
    MockAsyncDriveWaker waker;
    AsyncCtxGuard ctx;
    PhysicalWriteFragment *fragment = NULL;
    FragmentRef ref;
    int64_t planned_len = 0;
    ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
    ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                    1 /* cb_tg_id */, &env, &waker,
                                    AsyncThrottleContext()));
    char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(buf));
    ASSERT_EQ(OB_SUCCESS, ctx->pool_.alloc_slot(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE,
                                                LOG_DIO_ALIGN_SIZE, FragmentRef(), ref, planned_len));
    ASSERT_EQ(OB_SUCCESS, ctx->pool_.get_fragment(ref, fragment));
    ASSERT_TRUE(OB_NOT_NULL(fragment));
    ctx->current_write_block_id_ = 0;
    ASSERT_EQ(OB_SUCCESS, ctx->submit_fragment_(&handle, *fragment));
    const int64_t first_submit_idx = find_handle_call_index(
        handle.calls(), RecordedHandleCall::SUBMIT, 0 /* start_idx */);
    ASSERT_GE(first_submit_idx, 0);
    ASSERT_EQ(2, ctx->get_inflight_count());
    ASSERT_EQ(1, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));

    common::ObIOInfo io_info;
    io_info.tenant_id_ = OB_SERVER_TENANT_ID;
    io_info.fd_.first_id_ = 0;
    io_info.fd_.second_id_ = 0;
    io_info.fd_.device_handle_ = &local_device;
    io_info.flag_.set_mode(common::ObIOMode::WRITE);
    io_info.flag_.set_wait_event(1);
    io_info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
    io_info.offset_ = 0;
    io_info.size_ = LOG_DIO_ALIGN_SIZE;
    io_info.buf_ = buf;
    ASSERT_TRUE(io_info.is_valid());
    ASSERT_EQ(OB_SUCCESS, io_result.basic_init());
    ASSERT_EQ(OB_SUCCESS, io_result.init(io_info));
    ASSERT_EQ(OB_SUCCESS, fragment->get_io_handle().set_result(io_result));
    io_result.finish_without_accumulate(common::ObIORetCode(io_results[i]));
    ASSERT_EQ(OB_SUCCESS, ctx->poll_submitted_fragment_(
        *fragment, common::ObTimeUtility::current_time()));
    EXPECT_EQ(1, ctx->get_inflight_count());
    EXPECT_EQ(0, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
    if (OB_SUCCESS == io_results[i]) {
      EXPECT_TRUE(fragment->is_finished());
    } else {
      EXPECT_TRUE(fragment->is_failed());
      EXPECT_EQ(io_results[i], fragment->get_ret_code());
      fragment->next_retry_ts_ = common::ObTimeUtility::current_time() - 1;
      ASSERT_EQ(OB_SUCCESS, ctx->drive_phase_fragment_(&handle));
      EXPECT_TRUE(fragment->is_submitted());
      EXPECT_EQ(2, ctx->get_inflight_count());
      EXPECT_EQ(1, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
      const int64_t retry_submit_idx = find_handle_call_index(
          handle.calls(), RecordedHandleCall::SUBMIT, first_submit_idx + 1);
      ASSERT_GE(retry_submit_idx, 0);
      complete_async_submit(*ctx.get(), handle.calls().at(retry_submit_idx));
      EXPECT_TRUE(fragment->is_finished());
      EXPECT_EQ(1, ctx->get_inflight_count());
      EXPECT_EQ(0, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
    }
    ASSERT_EQ(OB_SUCCESS, ctx->pool_.free_slot(ref));
    EXPECT_EQ(0, ctx->get_inflight_count());
    ctx->destroy();
  }
}

TEST(TestPalfAsyncFragment, CtxRollsBackInflightAfterSynchronousSubmitFailure)
{
  const int submit_errors[] = {OB_EAGAIN, OB_IO_ERROR};
  const int64_t submit_error_count = static_cast<int64_t>(
      sizeof(submit_errors) / sizeof(submit_errors[0]));
  for (int64_t i = 0; i < submit_error_count; ++i) {
    UnitTaskArena arena;
    MockAsyncPalfHandleImpl handle;
    MockPalfEnvImpl env(handle);
    MockAsyncDriveWaker waker;
    AsyncCtxGuard ctx;
    PhysicalWriteFragment *fragment = NULL;
    FragmentRef ref;
    int64_t planned_len = 0;
    ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
    ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                    1 /* cb_tg_id */, &env, &waker,
                                    AsyncThrottleContext()));
    char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(buf));
    ASSERT_EQ(OB_SUCCESS, ctx->pool_.alloc_slot(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE,
                                                LOG_DIO_ALIGN_SIZE, FragmentRef(), ref, planned_len));
    ASSERT_EQ(OB_SUCCESS, ctx->pool_.get_fragment(ref, fragment));
    ASSERT_TRUE(OB_NOT_NULL(fragment));
    ctx->current_write_block_id_ = 0;
    handle.set_submit_ret(submit_errors[i]);
    const int64_t before_submit_ts = common::ObTimeUtility::current_time();

    EXPECT_EQ(submit_errors[i], ctx->submit_fragment_(&handle, *fragment));
    EXPECT_TRUE(fragment->is_failed());
    EXPECT_EQ(submit_errors[i], fragment->get_ret_code());
    EXPECT_EQ(1, ctx->get_inflight_count());
    EXPECT_EQ(0, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
    if (OB_EAGAIN == submit_errors[i]) {
      EXPECT_LE(fragment->get_next_retry_ts(), common::ObTimeUtility::current_time());
    } else {
      EXPECT_GT(fragment->get_next_retry_ts(), before_submit_ts);
    }

    fragment->next_retry_ts_ = common::ObTimeUtility::current_time() - 1;
    handle.set_submit_ret(OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, ctx->drive_phase_fragment_(&handle));
    EXPECT_TRUE(fragment->is_submitted());
    EXPECT_EQ(2, ctx->get_inflight_count());
    EXPECT_EQ(1, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
    const int64_t submit_idx = find_handle_call_index(
        handle.calls(), RecordedHandleCall::SUBMIT, 0);
    ASSERT_GE(submit_idx, 0);
    complete_async_submit(*ctx.get(), handle.calls().at(submit_idx));
    EXPECT_EQ(1, ctx->get_inflight_count());
    EXPECT_EQ(0, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
    ASSERT_EQ(OB_SUCCESS, ctx->pool_.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
    EXPECT_EQ(0, ctx->get_inflight_count());
    ctx->destroy();
  }
}

TEST(TestPalfAsyncFragment, CtxHandlesAioCompletionExactlyOnce)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  PhysicalWriteFragment *fragment = NULL;
  FragmentRef ref;
  int64_t planned_len = 0;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  1 /* cb_tg_id */, &env, &waker,
                                  AsyncThrottleContext()));
  char *buf = arena.alloc_aligned_buf(make_lsn(0), 2 * LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.alloc_slot(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE,
                                              LOG_DIO_ALIGN_SIZE, FragmentRef(), ref, planned_len));
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.get_fragment(ref, fragment));
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  ctx->current_write_block_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, ctx->submit_fragment_(&handle, *fragment));
  ASSERT_EQ(2, ctx->get_inflight_count());
  ASSERT_EQ(1, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
  const int64_t submit_idx = find_handle_call_index(handle.calls(), RecordedHandleCall::SUBMIT, 0);
  ASSERT_GE(submit_idx, 0);

  AsyncIOCompletionEvent event;
  bool need_wake_worker = false;
  init_async_completion_event(handle.calls().at(submit_idx), OB_IO_ERROR,
                              common::ObTimeUtility::current_time(), event);
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  EXPECT_TRUE(need_wake_worker);
  EXPECT_TRUE(fragment->is_failed());
  EXPECT_EQ(1, ctx->get_inflight_count());
  EXPECT_EQ(0, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
  EXPECT_EQ(1, ATOMIC_LOAD(&ctx->complete_fail_cnt_));

  need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  EXPECT_FALSE(need_wake_worker);
  EXPECT_EQ(1, ctx->get_inflight_count());
  EXPECT_EQ(1, ATOMIC_LOAD(&ctx->complete_fail_cnt_));

  fragment->next_retry_ts_ = common::ObTimeUtility::current_time() - 1;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_phase_fragment_(&handle));
  EXPECT_TRUE(fragment->is_submitted());
  EXPECT_EQ(2, ctx->get_inflight_count());
  EXPECT_EQ(1, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
  const int64_t retry_submit_idx = find_handle_call_index(
      handle.calls(), RecordedHandleCall::SUBMIT, submit_idx + 1);
  ASSERT_GE(retry_submit_idx, 0);
  complete_async_submit(*ctx.get(), handle.calls().at(retry_submit_idx));
  EXPECT_EQ(1, ctx->get_inflight_count());
  EXPECT_EQ(0, ATOMIC_LOAD(&ctx->inflight_aio_cnt_));
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  EXPECT_EQ(0, ctx->get_inflight_count());

  need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  EXPECT_FALSE(need_wake_worker);
  EXPECT_EQ(0, ctx->get_inflight_count());
  const int64_t stale_completion_cnt = ATOMIC_LOAD(&ctx->stale_completion_cnt_);
  EXPECT_GT(stale_completion_cnt, 0);

  FragmentRef new_ref;
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.alloc_slot(make_lsn(LOG_DIO_ALIGN_SIZE),
                                              buf + LOG_DIO_ALIGN_SIZE,
                                              LOG_DIO_ALIGN_SIZE,
                                              LOG_DIO_ALIGN_SIZE,
                                              FragmentRef(), new_ref, planned_len));
  ASSERT_FALSE(new_ref.is_equal(ref));
  ASSERT_EQ(OB_SUCCESS, ctx->pool_.get_fragment(new_ref, fragment));
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  ASSERT_TRUE(fragment->is_ready());

  event.ctx.fragment_ref = ref;
  need_wake_worker = false;
  ASSERT_EQ(OB_SUCCESS, ctx->on_aio_complete(event, need_wake_worker));
  EXPECT_FALSE(need_wake_worker);
  EXPECT_TRUE(fragment->is_ready());
  EXPECT_GT(ATOMIC_LOAD(&ctx->stale_completion_cnt_), stale_completion_cnt);

  ASSERT_EQ(OB_SUCCESS, ctx->pool_.free_slot(new_ref));
  EXPECT_EQ(0, ctx->get_inflight_count());
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxSwitchesBlockBeforeAdmittingBoundaryTask)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  const LSN block_end_lsn = make_lsn(PALF_BLOCK_SIZE);
  handle.set_planned_tail(block_end_lsn);
  handle.set_planned_writable_size(0);

  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                1 /* cb_tg_id */, &env, &waker,
                                AsyncThrottleContext()));
  EXPECT_FALSE(ctx->block_switch_pending_);

  char *buf = arena.alloc_aligned_buf(block_end_lsn, LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, block_end_lsn,
                                                    LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  int64_t submit_cnt = 0;
  int64_t switch_cnt = 0;
  int64_t first_next_block_submit_idx = -1;
  int64_t first_switch_idx = -1;
  int64_t first_header_idx = -1;
  bool has_next_block_submit = false;
  for (int64_t i = 0; i < static_cast<int64_t>(calls.size()); ++i) {
    if (RecordedHandleCall::SUBMIT == calls.at(i).kind) {
      ++submit_cnt;
      if (block_end_lsn == calls.at(i).submitted_begin_lsn) {
        has_next_block_submit = true;
        if (first_next_block_submit_idx < 0) {
          first_next_block_submit_idx = i;
        }
      }
    } else if (RecordedHandleCall::SWITCH_BLOCK == calls.at(i).kind) {
      ++switch_cnt;
      if (first_switch_idx < 0) {
        first_switch_idx = i;
      }
    } else if (RecordedHandleCall::BLOCK_HEADER == calls.at(i).kind
               && first_header_idx < 0) {
      first_header_idx = i;
    }
  }
  EXPECT_EQ(1, switch_cnt);
  EXPECT_EQ(1, submit_cnt);
  EXPECT_TRUE(has_next_block_submit);
  EXPECT_GE(first_switch_idx, 0);
  EXPECT_GE(first_header_idx, 0);
  EXPECT_GE(first_next_block_submit_idx, 0);
  EXPECT_LT(first_switch_idx, first_next_block_submit_idx);
  EXPECT_LT(first_header_idx, first_next_block_submit_idx);
  EXPECT_FALSE(ctx->block_switch_pending_);

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxRetriesBlockSwitchAfterEagain)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  AsyncCtxGuard ctx;
  CbPoolGuard cb_pool;
  const LSN block_end_lsn = make_lsn(PALF_BLOCK_SIZE);
  int64_t next_drive_interval_us = INT64_MAX;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  ASSERT_EQ(OB_SUCCESS, cb_pool.init(&env));
  handle.set_planned_tail(block_end_lsn);
  handle.set_planned_writable_size(0);
  handle.set_need_block_header(true);
  handle.set_model_real_submit_gates(true);
  handle.set_switch_block_ret(OB_EAGAIN);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                  cb_pool.get_tg_id(), &env, &waker,
                                  AsyncThrottleContext()));

  char *buf = arena.alloc_aligned_buf(block_end_lsn, LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(
      1 /* log_id */, block_end_lsn, LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::FLUSH_LOG_TYPE));
  ASSERT_EQ(OB_SUCCESS, ctx->enqueue_task(task));

  EXPECT_EQ(OB_EAGAIN, ctx->drive_write(next_drive_interval_us));
  EXPECT_TRUE(ctx->block_switch_pending_);
  EXPECT_EQ(1, ctx->task_queue_.get_total());
  EXPECT_GE(find_handle_call_index(handle.calls(), RecordedHandleCall::SWITCH_BLOCK, 0), 0);
  EXPECT_LT(find_handle_call_index(handle.calls(), RecordedHandleCall::BLOCK_HEADER, 0), 0);
  EXPECT_LT(find_handle_call_index(handle.calls(), RecordedHandleCall::SUBMIT, 0), 0);

  handle.set_switch_block_ret(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_FALSE(ctx->block_switch_pending_);
  EXPECT_EQ(0, ctx->task_queue_.get_total());
  const std::vector<RecordedHandleCall> &calls = handle.calls();
  const int64_t header_idx = find_handle_call_index(calls, RecordedHandleCall::BLOCK_HEADER, 0);
  const int64_t submit_idx = find_handle_call_index(calls, RecordedHandleCall::SUBMIT, 0);
  ASSERT_GE(header_idx, 0);
  ASSERT_GE(submit_idx, 0);
  EXPECT_LT(header_idx, submit_idx);
  complete_async_submit(*ctx.get(), calls.at(submit_idx));
  ASSERT_EQ(OB_SUCCESS, ctx->drive_write(next_drive_interval_us));
  EXPECT_EQ(0, ctx->get_inflight_count());
  for (int64_t i = 0; i < 100 && env.get_free_flush_task_cnt() < 1; ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, env.get_free_flush_task_cnt());

  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxInvalidatesPlannerAfterTruncatePrefixBarrier)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  const LSN old_tail = make_lsn(LOG_DIO_ALIGN_SIZE);
  const LSN new_tail = make_lsn(PALF_BLOCK_SIZE);
  LogIOTruncatePrefixBlocksTask task(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  PlannerStatus status;
  TruncatePrefixBlocksCbCtx cb_ctx(new_tail);
  LogIOTaskCbThreadPool cb_pool;
  bool consumed = false;

  handle.set_planned_tail(old_tail);
  handle.set_planned_writable_size(PALF_BLOCK_SIZE - old_tail.val_);
  handle.set_need_block_header(false);

  ASSERT_EQ(OB_SUCCESS, cb_pool.init(1 /* log_io_cb_num */, &env));
  ASSERT_EQ(OB_SUCCESS, cb_pool.start());
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */,
                                cb_pool.get_tg_id(), &env, &waker,
                                AsyncThrottleContext()));
  ctx->planner_.init_plan_state(old_tail);
  ctx->planner_.get_status(status);
  ASSERT_EQ(old_tail, status.planned_end_lsn_);
  ASSERT_EQ(old_tail, status.persisted_lsn_);

  ASSERT_EQ(OB_SUCCESS, task.init(cb_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::TRUNCATE_PREFIX_TYPE));
  ctx->control_barrier_task_ = &task;

  ASSERT_EQ(OB_SUCCESS, ctx->execute_control_barrier_task_(consumed));
  EXPECT_TRUE(consumed);
  EXPECT_EQ(1, handle.get_truncate_prefix_cnt());
  EXPECT_EQ(new_tail, handle.get_truncate_prefix_lsn());
  ctx->planner_.get_status(status);
  EXPECT_FALSE(status.planned_end_lsn_.is_valid());
  EXPECT_FALSE(status.persisted_lsn_.is_valid());
  EXPECT_FALSE(ctx->block_switch_pending_);
  EXPECT_TRUE(OB_ISNULL(ctx->control_barrier_task_));

  for (int64_t i = 0; i < 100 && 0 == handle.get_after_truncate_prefix_cnt(); ++i) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, handle.get_after_truncate_prefix_cnt());

  EXPECT_EQ(OB_SUCCESS, cb_pool.stop());
  EXPECT_EQ(OB_SUCCESS, cb_pool.wait());
  cb_pool.destroy();
  ctx->destroy();
}

TEST(TestPalfAsyncFragment, CtxDropsEpochMismatchControlBarrierLikeSyncWorker)
{
  MockAsyncPalfHandleImpl handle;
  MockPalfEnvImpl env(handle);
  MockAsyncDriveWaker waker;
  LogIOTruncatePrefixBlocksTask task(1 /* palf_id */, 0 /* palf_epoch */);
  AsyncCtxGuard ctx;
  ASSERT_TRUE(OB_NOT_NULL(ctx.get()));
  bool consumed = false;

  handle.set_palf_epoch(1);
  ASSERT_EQ(OB_SUCCESS, ctx->init(1 /* palf_id */, 1 /* cb_tg_id */,
                                  &env, &waker, AsyncThrottleContext()));
  TruncatePrefixBlocksCbCtx cb_ctx(make_lsn(LOG_DIO_ALIGN_SIZE));
  ASSERT_EQ(OB_SUCCESS, task.init(cb_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->try_reserve_task_slot(LogIOTaskType::TRUNCATE_PREFIX_TYPE));
  ctx->control_barrier_task_ = &task;

  EXPECT_EQ(OB_STATE_NOT_MATCH, ctx->execute_control_barrier_task_(consumed));
  EXPECT_TRUE(consumed);
  EXPECT_TRUE(OB_ISNULL(ctx->control_barrier_task_));
  EXPECT_EQ(1, env.get_free_truncate_prefix_task_cnt());
  EXPECT_EQ(0, ctx->get_inflight_count());
  ctx->destroy();
  EXPECT_EQ(1, env.get_free_truncate_prefix_task_cnt());
}

TEST(TestPalfAsyncFragment, PlannerPlansOneRoundIntoOneFragment)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  const int64_t first_task_len = 2 * 1024 * 1024;
  const int64_t round_len = 5 * 1024 * 1024;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), round_len);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), first_task_len, buf);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(
      2, make_lsn(first_task_len), round_len - first_task_len, buf + first_task_len);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  EXPECT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  EXPECT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(2, status.pending_task_count_);
  EXPECT_EQ(1, status.active_fragment_count_);
  EXPECT_EQ(make_lsn(round_len), status.planned_end_lsn_);

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(0)));
  EXPECT_EQ(make_lsn(0), ready_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(make_lsn(round_len), ready_fragments.at(0)->get_end_lsn());
  EXPECT_EQ(NORMAL_FRAGMENT_MAX_SIZE, ready_fragments.at(0)->get_fragment_max_size());
}

TEST(TestPalfAsyncFragment, PlannerKeepsDerivedEndInvalidAfterTailResetFailure)
{
  MockAsyncPalfHandleImpl handle;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));
  const LSN tail_lsn = make_lsn(LOG_DIO_ALIGN_SIZE + 123);
  PlannerStatus status;

  handle.set_carry_read_ret(OB_IO_ERROR);
  EXPECT_EQ(OB_IO_ERROR, planner.reset_after_tail_changed(&handle, tail_lsn));
  planner.get_status(status);
  EXPECT_FALSE(status.get_planned_end_lsn().is_valid());
  EXPECT_FALSE(status.get_persisted_lsn().is_valid());

  handle.set_carry_read_ret(OB_SUCCESS);
  handle.set_tail_page_read_size(LOG_DIO_ALIGN_SIZE);
  ASSERT_EQ(OB_SUCCESS, planner.reset_after_tail_changed(&handle, tail_lsn));
  planner.get_status(status);
  EXPECT_EQ(tail_lsn, status.get_planned_end_lsn());
  EXPECT_EQ(tail_lsn, status.get_persisted_lsn());
}

TEST(TestPalfAsyncFragment, PlannerRejectsShortReadAndFillFailureDuringTailReset)
{
  MockAsyncPalfHandleImpl handle;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));
  const offset_t prefix_len = 123;
  const LSN tail_lsn = make_lsn(LOG_DIO_ALIGN_SIZE + prefix_len);
  PlannerStatus status;

  handle.set_tail_page_read_size(prefix_len - 1);
  EXPECT_EQ(OB_ERR_UNEXPECTED, planner.reset_after_tail_changed(&handle, tail_lsn));
  planner.get_status(status);
  EXPECT_FALSE(status.get_planned_end_lsn().is_valid());
  EXPECT_FALSE(status.get_persisted_lsn().is_valid());
  EXPECT_EQ(0, handle.get_tail_prefix_fill_cnt());

  handle.set_tail_page_read_size(LOG_DIO_ALIGN_SIZE);
  handle.set_tail_prefix_fill_ret(OB_IO_ERROR);
  EXPECT_EQ(OB_IO_ERROR, planner.reset_after_tail_changed(&handle, tail_lsn));
  planner.get_status(status);
  EXPECT_FALSE(status.get_planned_end_lsn().is_valid());
  EXPECT_FALSE(status.get_persisted_lsn().is_valid());
  EXPECT_EQ(1, handle.get_tail_prefix_fill_cnt());

  handle.set_tail_prefix_fill_ret(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, planner.reset_after_tail_changed(&handle, tail_lsn));
  planner.get_status(status);
  EXPECT_EQ(tail_lsn, status.get_planned_end_lsn());
  EXPECT_EQ(tail_lsn, status.get_persisted_lsn());
  EXPECT_EQ(2, handle.get_tail_prefix_fill_cnt());
}

TEST(TestPalfAsyncFragment, PlannerRejectsTailResetWhileWorkRemains)
{
  UnitTaskArena arena;
  MockAsyncPalfHandleImpl handle;
  const LSN new_tail = make_lsn(LOG_DIO_ALIGN_SIZE + 123);
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));

  {
    PhysicalWriteFragmentPool pool;
    ASSERT_EQ(OB_SUCCESS, pool.init());
    LogAsyncWritePlanner planner(pool);
    init_planner(planner, make_lsn(0));
    LogIOFlushLogTask *task = arena.make_one_buf_task(
        1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
    ASSERT_TRUE(OB_NOT_NULL(task));
    bool consumed = false;
    ASSERT_EQ(OB_SUCCESS, planner.admit_task(task, consumed));
    ASSERT_TRUE(consumed);
    EXPECT_EQ(OB_ERR_UNEXPECTED, planner.reset_after_tail_changed(&handle, new_tail));
    EXPECT_EQ(1, planner.pending_task_queue_.get_total());
    EXPECT_EQ(0, handle.get_tail_page_read_cnt());
  }

  {
    PhysicalWriteFragmentPool pool;
    ASSERT_EQ(OB_SUCCESS, pool.init());
    LogAsyncWritePlanner planner(pool);
    init_planner(planner, make_lsn(0));
    FragmentRef ref;
    int64_t planned_len = 0;
    ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE,
                                          NORMAL_FRAGMENT_MAX_SIZE, FragmentRef(), ref, planned_len));
    ASSERT_EQ(OB_SUCCESS,
              planner.push_fragment_ref_(ref, make_lsn(0), make_lsn(LOG_DIO_ALIGN_SIZE)));
    EXPECT_EQ(OB_ERR_UNEXPECTED, planner.reset_after_tail_changed(&handle, new_tail));
    EXPECT_EQ(1, planner.fragment_ref_queue_.get_total());
    EXPECT_EQ(0, handle.get_tail_page_read_cnt());
  }

  {
    PhysicalWriteFragmentPool pool;
    ASSERT_EQ(OB_SUCCESS, pool.init());
    LogAsyncWritePlanner planner(pool);
    init_planner(planner, make_lsn(0));
    ASSERT_EQ(OB_SUCCESS,
              planner.pending_sources_[0].init(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE));
    EXPECT_EQ(OB_ERR_UNEXPECTED, planner.reset_after_tail_changed(&handle, new_tail));
    EXPECT_FALSE(planner.pending_sources_[0].is_empty());
    EXPECT_EQ(0, handle.get_tail_page_read_cnt());
  }
}

TEST(TestPalfAsyncFragment, PlannerRejectsThirdNonContiguousSourceAndRollsBack)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *buf0 = arena.alloc_aligned_buf(make_lsn(0), 2048);
  char *buf1 = arena.alloc_aligned_buf(make_lsn(2048), 2048);
  char *buf2 = arena.alloc_aligned_buf(make_lsn(LOG_DIO_ALIGN_SIZE), 1024);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  ASSERT_TRUE(OB_NOT_NULL(buf2));
  ASSERT_NE(buf0 + 2048, buf1);
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), 2048, buf0);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(2048), 2048, buf1);
  LogIOFlushLogTask *task2 = arena.make_one_buf_task(3, make_lsn(LOG_DIO_ALIGN_SIZE), 1024, buf2);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  ASSERT_TRUE(OB_NOT_NULL(task2));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  ASSERT_TRUE(consumed);
  EXPECT_EQ(make_lsn(0), planner.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf0, planner.pending_sources_[0].get_buf());
  EXPECT_EQ(2048, planner.pending_sources_[0].get_len());
  EXPECT_EQ(make_lsn(2048), planner.pending_sources_[1].get_begin_lsn());
  EXPECT_EQ(buf1, planner.pending_sources_[1].get_buf());
  EXPECT_EQ(2048, planner.pending_sources_[1].get_len());

  consumed = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, planner.admit_task(task2, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), planner.queue_end_lsn_);
  EXPECT_EQ(2048, planner.pending_sources_[0].get_len());
  EXPECT_EQ(2048, planner.pending_sources_[1].get_len());

  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), planner.get_planned_end_lsn_());
  EXPECT_TRUE(planner.pending_sources_[0].is_empty());
  EXPECT_TRUE(planner.pending_sources_[1].is_empty());
}

TEST(TestPalfAsyncFragment, PlannerRollsBackRejectedTaskAdmission)
{
  UnitTaskArena arena;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), 2 * LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));

  {
    PhysicalWriteFragmentPool pool;
    ASSERT_EQ(OB_SUCCESS, pool.init());
    LogAsyncWritePlanner planner(pool);
    init_planner(planner, make_lsn(0));
    LogIOFlushLogTask *gap_task = arena.make_one_buf_task(
        1 /* log_id */, make_lsn(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE,
        buf + LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(gap_task));
    bool consumed = false;
    EXPECT_EQ(OB_ERR_UNEXPECTED, planner.admit_task(gap_task, consumed));
    EXPECT_FALSE(consumed);
    EXPECT_EQ(make_lsn(0), planner.queue_end_lsn_);
    EXPECT_TRUE(planner.pending_sources_[0].is_empty());
    EXPECT_TRUE(planner.pending_sources_[1].is_empty());
  }

  {
    PhysicalWriteFragmentPool pool;
    ASSERT_EQ(OB_SUCCESS, pool.init());
    LogAsyncWritePlanner planner(pool);
    init_planner(planner, make_lsn(0));
    LogWriteBuf write_buf;
    ASSERT_EQ(OB_SUCCESS, write_buf.push_back(buf, 1024));
    share::SCN scn;
    scn.convert_for_logservice(1000);
    FlushLogCbCtx cb_ctx(2 /* log_id */, scn, make_lsn(0), 1 /* log_proposal_id */,
                         2048 /* total_len */, 1 /* curr_log_proposal_id */,
                         1 /* begin_ts */);
    LogIOFlushLogTask mismatched_task(1 /* palf_id */, 0 /* palf_epoch */);
    ASSERT_EQ(OB_SUCCESS, mismatched_task.init(cb_ctx, write_buf));
    bool consumed = false;
    EXPECT_EQ(OB_ERR_UNEXPECTED, planner.admit_task(&mismatched_task, consumed));
    EXPECT_FALSE(consumed);
    EXPECT_EQ(make_lsn(0), planner.queue_end_lsn_);
    EXPECT_TRUE(planner.pending_sources_[0].is_empty());
    EXPECT_TRUE(planner.pending_sources_[1].is_empty());
  }

  {
    PhysicalWriteFragmentPool pool;
    ASSERT_EQ(OB_SUCCESS, pool.init());
    LogAsyncWritePlanner planner(pool);
    init_planner(planner, make_lsn(0));
    LogIOFlushLogTask *task = arena.make_one_buf_task(
        3 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
    ASSERT_TRUE(OB_NOT_NULL(task));
    for (int64_t i = 0; i < LogAsyncWritePlanner::TASK_QUEUE_CAPACITY; ++i) {
      LogIOFlushLogTask *queued_task = task;
      ASSERT_EQ(OB_SUCCESS, planner.pending_task_queue_.push(queued_task));
    }
    bool consumed = false;
    EXPECT_EQ(OB_SIZE_OVERFLOW, planner.admit_task(task, consumed));
    EXPECT_FALSE(consumed);
    EXPECT_EQ(make_lsn(0), planner.queue_end_lsn_);
    EXPECT_TRUE(planner.pending_sources_[0].is_empty());
    EXPECT_TRUE(planner.pending_sources_[1].is_empty());
  }
}

TEST(TestPalfAsyncFragment, PlannerStartsNewFragmentAtWrappedDioBoundary)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *buf0 = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  char *buf1 = arena.alloc_aligned_buf(make_lsn(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf0);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(LOG_DIO_ALIGN_SIZE),
                                                     LOG_DIO_ALIGN_SIZE, buf1);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  EXPECT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  EXPECT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(2, status.pending_task_count_);
  EXPECT_EQ(2, status.active_fragment_count_);
  EXPECT_EQ(make_lsn(2 * LOG_DIO_ALIGN_SIZE), status.planned_end_lsn_);
}

TEST(TestPalfAsyncFragment, PlannerAppliesConfiguredFragmentLimit)
{
  static const int64_t fragment_max_size = 256 * LOG_DIO_ALIGN_SIZE;
  static const int64_t wait_parent_max_size = 128 * LOG_DIO_ALIGN_SIZE;
  static const int64_t task_len = 2 * fragment_max_size + LOG_DIO_ALIGN_SIZE;
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));
  ASSERT_EQ(OB_SUCCESS,
            planner.update_fragment_size_limits(fragment_max_size,
                                                wait_parent_max_size));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            planner.update_fragment_size_limits(
                LogAsyncWritePlanner::MIN_FRAGMENT_MAX_SIZE - LOG_DIO_ALIGN_SIZE,
                wait_parent_max_size));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            planner.update_fragment_size_limits(fragment_max_size + 1,
                                                wait_parent_max_size));
  EXPECT_EQ(fragment_max_size, planner.get_fragment_max_size());
  EXPECT_EQ(wait_parent_max_size, planner.get_wait_parent_max_size());
  ASSERT_EQ(OB_SUCCESS,
            planner.update_fragment_size_limits(fragment_max_size,
                                                2 * fragment_max_size));
  EXPECT_EQ(fragment_max_size, planner.get_wait_parent_max_size());
  ASSERT_EQ(OB_SUCCESS,
            planner.update_fragment_size_limits(fragment_max_size,
                                                wait_parent_max_size));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), task_len);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, make_lsn(0), task_len, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(3, ready_fragments.count());
  for (int64_t i = 0; i < ready_fragments.count(); ++i) {
    PhysicalWriteFragment *fragment = ready_fragments.at(i);
    int64_t data_len = 0;
    ASSERT_TRUE(OB_NOT_NULL(fragment));
    EXPECT_EQ(fragment_max_size, fragment->get_fragment_max_size());
    ASSERT_EQ(OB_SUCCESS, fragment->get_data_len(data_len));
    EXPECT_EQ(i < 2 ? fragment_max_size : LOG_DIO_ALIGN_SIZE, data_len);
    fragment->set_state_(AsyncFragmentState::FINISHED);
  }
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(make_lsn(task_len), status.get_planned_end_lsn());
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  LogIOFlushLogTask *published_task = NULL;
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(task, published_task);
  planner.pop_published_task();
}

TEST(TestPalfAsyncFragment, PlannerAppliesConfiguredWaitParentLimit)
{
  static const int64_t fragment_max_size = 256 * LOG_DIO_ALIGN_SIZE;
  static const int64_t wait_parent_max_size = 128 * LOG_DIO_ALIGN_SIZE;
  static const int64_t parent_len = LOG_DIO_ALIGN_SIZE / 2;
  static const int64_t child_len = wait_parent_max_size - parent_len;
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));
  ASSERT_EQ(OB_SUCCESS,
            planner.update_fragment_size_limits(fragment_max_size,
                                                wait_parent_max_size));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), wait_parent_max_size);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *parent_task = arena.make_one_buf_task(1, make_lsn(0), parent_len, buf);
  LogIOFlushLogTask *child_task = arena.make_one_buf_task(
      2, make_lsn(parent_len), child_len, buf + parent_len);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(parent_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  PhysicalWriteFragment *parent = NULL;
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(planner.last_fragment_ref_, parent));
  ASSERT_TRUE(OB_NOT_NULL(parent));
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent->get_fragment_ref(), 1 /* submit_ts */));

  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  const PhysicalWriteFragmentStateFilter wait_parent_filter(AsyncFragmentState::WAIT_PARENT);
  ASSERT_EQ(OB_SUCCESS, pool.collect_fragments(wait_fragments, wait_parent_filter));
  ASSERT_EQ(1, wait_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(wait_fragments.at(0)));
  EXPECT_EQ(wait_parent_max_size, wait_fragments.at(0)->get_fragment_max_size());
  EXPECT_EQ(make_lsn(0), wait_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(make_lsn(wait_parent_max_size), wait_fragments.at(0)->get_end_lsn());

  parent->set_state_(AsyncFragmentState::FINISHED);
  wait_fragments.at(0)->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  LogIOFlushLogTask *published_task = NULL;
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(parent_task, published_task);
  planner.pop_published_task();
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(child_task, published_task);
  planner.pop_published_task();
}

TEST(TestPalfAsyncFragment, PlannerDoesNotAdmitNextBlockTaskBeforeBlockSwitch)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  const LSN begin_lsn = make_lsn(PALF_BLOCK_SIZE - LOG_DIO_ALIGN_SIZE);
  init_planner(planner, begin_lsn);

  char *buf0 = arena.alloc_aligned_buf(begin_lsn, LOG_DIO_ALIGN_SIZE);
  char *buf1 = arena.alloc_aligned_buf(make_lsn(PALF_BLOCK_SIZE), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, begin_lsn, LOG_DIO_ALIGN_SIZE, buf0);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(PALF_BLOCK_SIZE), LOG_DIO_ALIGN_SIZE, buf1);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  EXPECT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  EXPECT_FALSE(consumed);

  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(1, status.pending_task_count_);
  EXPECT_FALSE(status.has_pending_source_);
  EXPECT_EQ(make_lsn(PALF_BLOCK_SIZE), status.planned_end_lsn_);
}

TEST(TestPalfAsyncFragment, PlannerQueuesLaterTaskWhenHeadPartiallyPlanned)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  FragmentRef reserved_refs[FRAGMENT_SLOT_CNT_PER_PALF - 1];
  int64_t reserved_len = 0;
  for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF - 1; ++i) {
    const LSN reserved_lsn = make_lsn(PALF_BLOCK_SIZE + i * LOG_DIO_ALIGN_SIZE);
    char *reserved_buf = arena.alloc_aligned_buf(reserved_lsn, LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(reserved_buf));
    ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(reserved_lsn, reserved_buf, LOG_DIO_ALIGN_SIZE,
                                          NORMAL_FRAGMENT_MAX_SIZE, FragmentRef(), reserved_refs[i], reserved_len));
    ASSERT_EQ(LOG_DIO_ALIGN_SIZE, reserved_len);
  }

  const int64_t task0_len = 2 * LOG_DIO_ALIGN_SIZE;
  char *buf0 = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  char *buf0_tail = arena.alloc_aligned_buf(make_lsn(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE);
  char *buf1 = arena.alloc_aligned_buf(make_lsn(task0_len), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf0_tail));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  LogIOFlushLogTask *task0 = arena.make_task(1, make_lsn(0), task0_len,
                                            buf0, LOG_DIO_ALIGN_SIZE,
                                            buf0_tail, LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(task0_len),
                                                     LOG_DIO_ALIGN_SIZE, buf1);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  EXPECT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(1, status.pending_task_count_);
  EXPECT_EQ(FRAGMENT_SLOT_CNT_PER_PALF, status.active_fragment_count_);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), status.planned_end_lsn_);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), planner.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf0_tail, planner.pending_sources_[0].get_buf());
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, planner.pending_sources_[0].get_len());

  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  EXPECT_TRUE(consumed);
  planner.get_status(status);
  EXPECT_EQ(2, status.pending_task_count_);

  for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, pool.free_slot(reserved_refs[i]));
  }

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  for (int64_t i = 0; i < ready_fragments.count(); ++i) {
    ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(i)));
    ready_fragments.at(i)->set_state_(AsyncFragmentState::FINISHED);
  }
  LSN persisted_lsn(make_lsn(0));
  release_and_advance_finished_prefix(planner, pool, persisted_lsn);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), persisted_lsn);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  planner.get_status(status);
  EXPECT_EQ(make_lsn(task0_len + LOG_DIO_ALIGN_SIZE), status.planned_end_lsn_);
}

TEST(TestPalfAsyncFragment, PlannerKeepsPartialWaitParentSourceForRetry)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  static const int64_t reserved_count = FRAGMENT_SLOT_CNT_PER_PALF - 2;
  FragmentRef reserved_refs[reserved_count];
  int64_t planned_len = 0;
  for (int64_t i = 0; i < reserved_count; ++i) {
    const LSN reserved_lsn = make_lsn(PALF_BLOCK_SIZE + i * LOG_DIO_ALIGN_SIZE);
    char *reserved_buf = arena.alloc_aligned_buf(reserved_lsn, LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(reserved_buf));
    ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(reserved_lsn, reserved_buf, LOG_DIO_ALIGN_SIZE,
                                          NORMAL_FRAGMENT_MAX_SIZE, FragmentRef(),
                                          reserved_refs[i], planned_len));
  }

  const int64_t parent_len = LOG_DIO_ALIGN_SIZE / 2;
  const int64_t child_len = WAIT_PARENT_FRAGMENT_MAX_SIZE;
  const int64_t next_task_len = 2 * 1024;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), parent_len + child_len + next_task_len);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *parent_task = arena.make_one_buf_task(1, make_lsn(0), parent_len, buf);
  LogIOFlushLogTask *child_task = arena.make_one_buf_task(
      2, make_lsn(parent_len), child_len, buf + parent_len);
  LogIOFlushLogTask *next_task = arena.make_one_buf_task(
      3, make_lsn(parent_len + child_len), next_task_len, buf + parent_len + child_len);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));
  ASSERT_TRUE(OB_NOT_NULL(next_task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(parent_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  PhysicalWriteFragment *parent = NULL;
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(planner.last_fragment_ref_, parent));
  ASSERT_TRUE(OB_NOT_NULL(parent));
  EXPECT_EQ(NORMAL_FRAGMENT_MAX_SIZE, parent->get_fragment_max_size());
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent->get_fragment_ref(), 1 /* submit_ts */));

  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(FRAGMENT_SLOT_CNT_PER_PALF, status.get_active_fragment_count());
  EXPECT_EQ(make_lsn(WAIT_PARENT_FRAGMENT_MAX_SIZE), status.get_planned_end_lsn());
  ASSERT_FALSE(planner.pending_sources_[0].is_empty());
  EXPECT_EQ(make_lsn(WAIT_PARENT_FRAGMENT_MAX_SIZE), planner.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf + WAIT_PARENT_FRAGMENT_MAX_SIZE, planner.pending_sources_[0].get_buf());
  EXPECT_EQ(parent_len, planner.pending_sources_[0].get_len());
  EXPECT_TRUE(planner.pending_sources_[1].is_empty());

  PhysicalWriteFragment *wait_parent = NULL;
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(planner.last_fragment_ref_, wait_parent));
  ASSERT_TRUE(OB_NOT_NULL(wait_parent));
  EXPECT_TRUE(wait_parent->is_wait_parent());
  EXPECT_EQ(WAIT_PARENT_FRAGMENT_MAX_SIZE, wait_parent->get_fragment_max_size());
  EXPECT_EQ(make_lsn(WAIT_PARENT_FRAGMENT_MAX_SIZE), wait_parent->get_end_lsn());

  ASSERT_EQ(OB_SUCCESS, planner.admit_task(next_task, consumed));
  ASSERT_TRUE(consumed);
  EXPECT_EQ(2 * parent_len, planner.pending_sources_[0].get_len());
  ASSERT_EQ(OB_SUCCESS, pool.free_slot(reserved_refs[0]));
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  EXPECT_TRUE(planner.pending_sources_[0].is_empty());
  EXPECT_TRUE(planner.pending_sources_[1].is_empty());
  EXPECT_EQ(make_lsn(WAIT_PARENT_FRAGMENT_MAX_SIZE + 2 * parent_len),
            planner.get_planned_end_lsn_());

  PhysicalWriteFragment *ready_fragment = NULL;
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(planner.last_fragment_ref_, ready_fragment));
  ASSERT_TRUE(OB_NOT_NULL(ready_fragment));
  EXPECT_TRUE(ready_fragment->is_ready());
  EXPECT_EQ(make_lsn(WAIT_PARENT_FRAGMENT_MAX_SIZE), ready_fragment->get_begin_lsn());
  EXPECT_EQ(make_lsn(WAIT_PARENT_FRAGMENT_MAX_SIZE + 2 * parent_len),
            ready_fragment->get_end_lsn());
}

TEST(TestPalfAsyncFragment, PendingSourceRangeAdvancesLsnBufferAndLengthTogether)
{
  UnitTaskArena arena;
  LogAsyncWritePlanner::PendingSourceRange source;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, source.init(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE));
  ASSERT_EQ(OB_SUCCESS, source.consume(2048));
  EXPECT_EQ(make_lsn(2048), source.get_begin_lsn());
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), source.get_end_lsn());
  EXPECT_EQ(buf + 2048, source.get_buf());
  EXPECT_EQ(2048, source.get_len());

  ASSERT_EQ(OB_SUCCESS, source.consume(2048));
  EXPECT_TRUE(source.is_empty());
}

TEST(TestPalfAsyncFragment, PlannerAdvancesReleasedFragmentRef)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(0)));
  const FragmentRef old_ref = ready_fragments.at(0)->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, pool.free_slot(old_ref));

  FragmentRef new_ref;
  int64_t planned_len = 0;
  char *new_buf = arena.alloc_aligned_buf(make_lsn(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(new_buf));
  ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(make_lsn(LOG_DIO_ALIGN_SIZE), new_buf,
                                        LOG_DIO_ALIGN_SIZE, NORMAL_FRAGMENT_MAX_SIZE,
                                        FragmentRef(), new_ref, planned_len));
  ASSERT_EQ(LOG_DIO_ALIGN_SIZE, planned_len);
  ASSERT_EQ(old_ref.slot_id, new_ref.slot_id);
  ASSERT_NE(old_ref.generation, new_ref.generation);

  LSN persisted_lsn(make_lsn(0));
  PlannerStatus status;
  EXPECT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  planner.get_status(status);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), status.persisted_lsn_);
}

TEST(TestPalfAsyncFragment, PlannerAdvancesOnlyFinishedFragment)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(1, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(0)));

  PlannerStatus status;
  EXPECT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  planner.get_status(status);
  EXPECT_EQ(make_lsn(0), status.persisted_lsn_);

  ready_fragments.at(0)->set_state_(AsyncFragmentState::FINISHED);
  EXPECT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  planner.get_status(status);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), status.persisted_lsn_);
}

TEST(TestPalfAsyncFragment, PlannerAdvancesOutOfOrderCompletionOnlyAfterPrefix)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *source_buf = arena.alloc_aligned_buf(make_lsn(0), 3 * LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(source_buf));
  char *buf0 = source_buf;
  char *buf1 = source_buf + 2 * LOG_DIO_ALIGN_SIZE;
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf0);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(
      2 /* log_id */, make_lsn(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE, buf1);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(2, ready_fragments.count());
  PhysicalWriteFragment *first = NULL;
  PhysicalWriteFragment *second = NULL;
  for (int64_t i = 0; i < ready_fragments.count(); ++i) {
    PhysicalWriteFragment *fragment = ready_fragments.at(i);
    ASSERT_TRUE(OB_NOT_NULL(fragment));
    if (make_lsn(0) == fragment->get_begin_lsn()) {
      first = fragment;
    } else if (make_lsn(LOG_DIO_ALIGN_SIZE) == fragment->get_begin_lsn()) {
      second = fragment;
    }
  }
  ASSERT_TRUE(OB_NOT_NULL(first));
  ASSERT_TRUE(OB_NOT_NULL(second));

  second->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(make_lsn(0), status.get_persisted_lsn());

  first->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  planner.get_status(status);
  EXPECT_EQ(make_lsn(2 * LOG_DIO_ALIGN_SIZE), status.get_persisted_lsn());
  LogIOFlushLogTask *published_task = NULL;
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(task0, published_task);
  planner.pop_published_task();
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(task1, published_task);
}

TEST(TestPalfAsyncFragment, PlannerPublishesTasksByPersistedLsn)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), 2048, buf);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(2048), 2048, buf + 2048);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(0)));
  ready_fragments.at(0)->set_state_(AsyncFragmentState::FINISHED);

  LSN persisted_lsn(make_lsn(0));
  PlannerStatus status;
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  planner.get_status(status);
  EXPECT_EQ(0, status.active_fragment_count_);
  EXPECT_EQ(make_lsn(0), persisted_lsn);
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  planner.get_status(status);
  persisted_lsn = status.persisted_lsn_;
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), persisted_lsn);

  LogIOFlushLogTask *published_task = NULL;
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(task0, published_task);
  EXPECT_EQ(make_lsn(0), published_task->get_flush_begin_lsn());
  EXPECT_EQ(make_lsn(2048), published_task->get_flush_end_lsn());
  planner.pop_published_task();
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(task1, published_task);
  planner.pop_published_task();
  EXPECT_EQ(OB_ITER_END, planner.peek_publishable_task(published_task));
}

TEST(TestPalfAsyncFragment, PlannerDelaysFinishedFragmentVisibilityAndRecycle)
{
  static const int64_t AIO_DELAY_US = 60_s;
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  PalfPerfItem fragment_recycle_delay_us(false /* is_counter */);
  init_planner(planner, make_lsn(0));
  ASSERT_EQ(OB_SUCCESS, planner.update_aio_delay(AIO_DELAY_US));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(
      ready_fragments, AIO_DELAY_US, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *fragment = ready_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  const FragmentRef fragment_ref = fragment->get_fragment_ref();
  const int64_t now_us = common::ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, fragment->mark_submitted(fragment_ref, now_us));
  bool completed_by_me = false;
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  EXPECT_EQ(OB_INVALID_ARGUMENT, fragment->mark_io_completed(
      fragment_ref, OB_SUCCESS, 0 /* next_retry_ts */, 0 /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  EXPECT_TRUE(fragment->is_submitted());
  ASSERT_EQ(OB_SUCCESS, fragment->mark_io_completed(
      fragment_ref, OB_SUCCESS, 0 /* next_retry_ts */, now_us /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  ASSERT_TRUE(completed_by_me);

  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(
      AIO_DELAY_US, &fragment_recycle_delay_us));
  EXPECT_EQ(0, fragment_recycle_delay_us.value_hist_.count_);
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(1, status.get_active_fragment_count());
  EXPECT_EQ(make_lsn(0), status.get_persisted_lsn());

  // Move the physical completion into the previous window to cover natural
  // expiry while keeping the configured delay positive and avoiding a sleep.
  fragment->finish_ts_ = common::ObTimeUtility::current_time() - AIO_DELAY_US;
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(
      AIO_DELAY_US, &fragment_recycle_delay_us));
  EXPECT_EQ(1, fragment_recycle_delay_us.value_hist_.count_);
  EXPECT_GE(fragment_recycle_delay_us.value_hist_.sum_, AIO_DELAY_US);
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  planner.get_status(status);
  EXPECT_EQ(0, status.get_active_fragment_count());
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), status.get_persisted_lsn());
  LogIOFlushLogTask *published_task = NULL;
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(task, published_task);
  planner.pop_published_task();
}

TEST(TestPalfAsyncFragment, PlannerWaitsForSubmittedParentOnSameDioPage)
{
  static const int64_t AIO_DELAY_US = 60_s;
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *task0 = arena.make_one_buf_task(1, make_lsn(0), 2048, buf);
  LogIOFlushLogTask *task1 = arena.make_one_buf_task(2, make_lsn(2048), 1024, buf + 2048);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task0, consumed));
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *parent = ready_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(parent));
  const FragmentRef parent_ref = parent->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent_ref, 1 /* submit_ts */));

  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task1, consumed));
  EXPECT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  const PhysicalWriteFragmentStateFilter wait_parent_filter(AsyncFragmentState::WAIT_PARENT);
  ASSERT_EQ(OB_SUCCESS, pool.collect_fragments(wait_fragments, wait_parent_filter));
  ASSERT_EQ(1, wait_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(wait_fragments.at(0)));
  EXPECT_TRUE(wait_fragments.at(0)->get_parent_ref().is_equal(parent_ref));
  EXPECT_EQ(make_lsn(0), wait_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(make_lsn(3072), wait_fragments.at(0)->get_end_lsn());

  bool completed_by_me = false;
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, parent->mark_io_completed(
      parent_ref, OB_IO_ERROR, 2 /* next_retry_ts */,
      common::ObTimeUtility::current_time() /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  ASSERT_TRUE(completed_by_me);
  ASSERT_TRUE(parent->is_failed());
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  EXPECT_EQ(0, ready_fragments.count());
  EXPECT_TRUE(wait_fragments.at(0)->is_wait_parent());

  ASSERT_EQ(OB_SUCCESS, planner.update_aio_delay(AIO_DELAY_US));
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent_ref, 2 /* submit_ts */));
  ASSERT_EQ(OB_SUCCESS, parent->mark_io_completed(
      parent_ref, OB_SUCCESS, 0 /* next_retry_ts */,
      common::ObTimeUtility::current_time() /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  ASSERT_TRUE(completed_by_me);
  ASSERT_TRUE(parent->is_finished());
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, AIO_DELAY_US,
                                                      NULL, NULL, NULL));
  EXPECT_EQ(0, ready_fragments.count());
  EXPECT_TRUE(wait_fragments.at(0)->is_wait_parent());

  parent->finish_ts_ = common::ObTimeUtility::current_time() - AIO_DELAY_US;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, AIO_DELAY_US,
                                                      NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(wait_fragments.at(0), ready_fragments.at(0));
}

TEST(TestPalfAsyncFragment, PlannerKeepsDependencyUntilParentDelayExpires)
{
  static const int64_t AIO_DELAY_US = 60_s;
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));
  ASSERT_EQ(OB_SUCCESS, planner.update_aio_delay(AIO_DELAY_US));

  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *parent_task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), 2048, buf);
  LogIOFlushLogTask *child_task = arena.make_one_buf_task(
      2 /* log_id */, make_lsn(2048), 1024, buf + 2048);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(parent_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(
      ready_fragments, AIO_DELAY_US, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *parent = ready_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(parent));
  const FragmentRef parent_ref = parent->get_fragment_ref();
  const int64_t now_us = common::ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent_ref, now_us));
  bool completed_by_me = false;
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, parent->mark_io_completed(
      parent_ref, OB_SUCCESS, 0 /* next_retry_ts */, now_us /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  ASSERT_TRUE(completed_by_me);

  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  const PhysicalWriteFragmentStateFilter wait_filter(AsyncFragmentState::WAIT_PARENT);
  ASSERT_EQ(OB_SUCCESS, pool.collect_fragments(wait_fragments, wait_filter));
  ASSERT_EQ(1, wait_fragments.count());
  PhysicalWriteFragment *child = wait_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(child));
  EXPECT_TRUE(child->get_parent_ref().is_equal(parent_ref));

  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(
      ready_fragments, AIO_DELAY_US, NULL, NULL, NULL));
  EXPECT_EQ(0, ready_fragments.count());
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(AIO_DELAY_US, NULL));
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(parent_ref, parent));

  parent->finish_ts_ = common::ObTimeUtility::current_time() - AIO_DELAY_US;
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(AIO_DELAY_US, NULL));
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(
      ready_fragments, AIO_DELAY_US, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(child, ready_fragments.at(0));
  const FragmentRef child_ref = child->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, child->mark_submitted(child_ref, now_us));
  ASSERT_EQ(OB_SUCCESS, child->mark_io_completed(
      child_ref, OB_SUCCESS, 0 /* next_retry_ts */, now_us /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  ASSERT_TRUE(completed_by_me);
  child->finish_ts_ = common::ObTimeUtility::current_time() - AIO_DELAY_US;
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(AIO_DELAY_US, NULL));
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(make_lsn(3072), status.get_persisted_lsn());
  LogIOFlushLogTask *published_task = NULL;
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  planner.pop_published_task();
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  planner.pop_published_task();
}

TEST(TestPalfAsyncFragment, PlannerExtendsWaitParentFragmentAndQueuedRange)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  const int64_t child_task1_len = LOG_DIO_ALIGN_SIZE + 1024;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), 2 * LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *parent_task = arena.make_one_buf_task(
      1 /* log_id */, make_lsn(0), 2048, buf);
  LogIOFlushLogTask *child_task0 = arena.make_one_buf_task(
      2 /* log_id */, make_lsn(2048), 1024, buf + 2048);
  LogIOFlushLogTask *child_task1 = arena.make_one_buf_task(
      3 /* log_id */, make_lsn(3072), child_task1_len, buf + 3072);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task0));
  ASSERT_TRUE(OB_NOT_NULL(child_task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(parent_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *parent = ready_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(parent));
  const FragmentRef parent_ref = parent->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent_ref, 1 /* submit_ts */));

  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task0, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task1, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  const PhysicalWriteFragmentStateFilter wait_filter(AsyncFragmentState::WAIT_PARENT);
  ASSERT_EQ(OB_SUCCESS, pool.collect_fragments(wait_fragments, wait_filter));
  ASSERT_EQ(1, wait_fragments.count());
  PhysicalWriteFragment *child = wait_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(child));
  EXPECT_EQ(make_lsn(2 * LOG_DIO_ALIGN_SIZE), child->get_end_lsn());
  EXPECT_TRUE(child->get_parent_ref().is_equal(parent_ref));
  EXPECT_EQ(WAIT_PARENT_FRAGMENT_MAX_SIZE, child->get_fragment_max_size());
  ASSERT_TRUE(OB_NOT_NULL(planner.last_fragment_ref_item_));
  EXPECT_EQ(make_lsn(2 * LOG_DIO_ALIGN_SIZE), planner.last_fragment_ref_item_->end_lsn_);

  parent->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(child, ready_fragments.at(0));
  child->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(make_lsn(2 * LOG_DIO_ALIGN_SIZE), status.get_persisted_lsn());
}

TEST(TestPalfAsyncFragment, PerfReporterPrintsItemsSinceLastPrint)
{
  const int64_t now = common::ObTimeUtility::current_time();
  PalfPerfItem counter(true /* is_counter */);
  PalfPerfItem value(false /* is_counter */);
  counter.record(now, 1);
  value.record(now, 1024);
  PalfPerfReporter reporter("[TEST][PERF]");
  reporter.add_item("counter_item", &counter);
  reporter.add_item("value_item", &value);
  reporter.print(now + 1000 * 1000, 0);
  char buf[4096];
  MEMSET(buf, 0, sizeof(buf));
  reporter.to_string(buf, sizeof(buf));
  EXPECT_TRUE(OB_NOT_NULL(strstr(buf, "counter_item")));
  EXPECT_TRUE(OB_NOT_NULL(strstr(buf, "value_item")));
}

TEST(TestPalfAsyncFragment, PlannerStartsNewFragmentForNonContiguousPendingBuffer)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *parent_buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  char *child_buf1 = arena.alloc_aligned_buf(make_lsn(3072), 1024);
  ASSERT_TRUE(OB_NOT_NULL(parent_buf));
  ASSERT_TRUE(OB_NOT_NULL(child_buf1));
  char *child_buf0 = parent_buf + 2048;
  ASSERT_NE(parent_buf + 3072, child_buf1);
  LogIOFlushLogTask *parent_task = arena.make_one_buf_task(1, make_lsn(0), 2048, parent_buf);
  LogIOFlushLogTask *child_task0 = arena.make_one_buf_task(2, make_lsn(2048), 1024, child_buf0);
  LogIOFlushLogTask *child_task1 = arena.make_one_buf_task(3, make_lsn(3072), 1024, child_buf1);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task0));
  ASSERT_TRUE(OB_NOT_NULL(child_task1));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(parent_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *parent = ready_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(parent));
  const FragmentRef parent_ref = parent->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent_ref, 1 /* submit_ts */));

  consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task0, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task1, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(3, status.active_fragment_count_);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), status.planned_end_lsn_);

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  const PhysicalWriteFragmentStateFilter wait_parent_filter(AsyncFragmentState::WAIT_PARENT);
  ASSERT_EQ(OB_SUCCESS, pool.collect_fragments(wait_fragments, wait_parent_filter));
  ASSERT_EQ(2, wait_fragments.count());
  PhysicalWriteFragment *first_child = NULL;
  PhysicalWriteFragment *second_child = NULL;
  for (int64_t i = 0; i < wait_fragments.count(); ++i) {
    PhysicalWriteFragment *fragment = wait_fragments.at(i);
    ASSERT_TRUE(OB_NOT_NULL(fragment));
    if (make_lsn(3072) == fragment->get_end_lsn()) {
      first_child = fragment;
    } else if (make_lsn(LOG_DIO_ALIGN_SIZE) == fragment->get_end_lsn()) {
      second_child = fragment;
    }
  }
  ASSERT_TRUE(OB_NOT_NULL(first_child));
  ASSERT_TRUE(OB_NOT_NULL(second_child));
  EXPECT_TRUE(first_child->get_parent_ref().is_equal(parent_ref));
  EXPECT_TRUE(second_child->get_parent_ref().is_equal(first_child->get_fragment_ref()));
  EXPECT_EQ(make_lsn(0), first_child->get_begin_lsn());
  EXPECT_EQ(make_lsn(0), second_child->get_begin_lsn());

  parent->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(first_child, ready_fragments.at(0));

  first_child->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(second_child, ready_fragments.at(0));
}

TEST(TestPalfAsyncFragment, PlannerHandlesTwoSourceRangesFromOneTask)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  init_planner(planner, make_lsn(0));

  char *buf0 = arena.alloc_aligned_buf(make_lsn(0), 2048);
  char *buf1 = arena.alloc_aligned_buf(make_lsn(2048), 2048);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  LogIOFlushLogTask *task = arena.make_task(
      1 /* log_id */, make_lsn(0), LOG_DIO_ALIGN_SIZE,
      buf0, 2048, buf1, 2048);
  ASSERT_TRUE(OB_NOT_NULL(task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(task, consumed));
  ASSERT_TRUE(consumed);
  EXPECT_EQ(2, task->get_write_buf().get_buf_count());
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  const PhysicalWriteFragmentStateFilter wait_filter(AsyncFragmentState::WAIT_PARENT);
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(OB_SUCCESS, pool.collect_fragments(wait_fragments, wait_filter));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_EQ(1, wait_fragments.count());
  PhysicalWriteFragment *parent = ready_fragments.at(0);
  PhysicalWriteFragment *child = wait_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(parent));
  ASSERT_TRUE(OB_NOT_NULL(child));
  EXPECT_TRUE(child->get_parent_ref().is_equal(parent->get_fragment_ref()));

  parent->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(child, ready_fragments.at(0));
  child->set_state_(AsyncFragmentState::FINISHED);
  ASSERT_EQ(OB_SUCCESS, pool.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
  ASSERT_EQ(OB_SUCCESS, planner.advance_finished_fragment_prefix());

  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(make_lsn(LOG_DIO_ALIGN_SIZE), status.get_persisted_lsn());
  LogIOFlushLogTask *published_task = NULL;
  ASSERT_EQ(OB_SUCCESS, planner.peek_publishable_task(published_task));
  EXPECT_EQ(task, published_task);
  planner.pop_published_task();
  EXPECT_EQ(OB_ITER_END, planner.peek_publishable_task(published_task));
}

TEST(TestPalfAsyncFragment, PlannerWaitsForSubmittedParentTailDioPage)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  const int64_t parent_begin_off = 2 * LOG_DIO_ALIGN_SIZE;
  const int64_t parent_end_off = 198 * 1024;
  const int64_t child_buf0_len = WAIT_PARENT_FRAGMENT_MAX_SIZE
      + upper_align(parent_end_off, LOG_DIO_ALIGN_SIZE) - parent_end_off;
  const int64_t child_buf1_len = LOG_DIO_ALIGN_SIZE;
  const int64_t child_len = child_buf0_len + child_buf1_len;
  const int64_t wait_parent_begin_off = lower_align(parent_end_off, LOG_DIO_ALIGN_SIZE);
  const int64_t wait_parent_end_off = wait_parent_begin_off + WAIT_PARENT_FRAGMENT_MAX_SIZE;
  init_planner(planner, make_lsn(parent_begin_off));

  char *parent_buf = arena.alloc_aligned_buf(make_lsn(parent_begin_off),
                                             parent_end_off - parent_begin_off);
  char *child_buf0 = arena.alloc_aligned_buf(make_lsn(parent_end_off), child_buf0_len);
  char *child_buf1 = arena.alloc_aligned_buf(
      make_lsn(parent_end_off + child_buf0_len), child_buf1_len);
  ASSERT_TRUE(OB_NOT_NULL(parent_buf));
  ASSERT_TRUE(OB_NOT_NULL(child_buf0));
  ASSERT_TRUE(OB_NOT_NULL(child_buf1));
  LogIOFlushLogTask *parent_task = arena.make_one_buf_task(1, make_lsn(parent_begin_off),
                                                           parent_end_off - parent_begin_off,
                                                           parent_buf);
  LogIOFlushLogTask *child_task = arena.make_task(
      2, make_lsn(parent_end_off), child_len,
      child_buf0, child_buf0_len, child_buf1, child_buf1_len);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(parent_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *parent = ready_fragments.at(0);
  ASSERT_TRUE(OB_NOT_NULL(parent));
  const FragmentRef parent_ref = parent->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent_ref, 1 /* submit_ts */));

  consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner.admit_task(child_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  const PhysicalWriteFragmentStateFilter wait_parent_filter(AsyncFragmentState::WAIT_PARENT);
  ASSERT_EQ(OB_SUCCESS, pool.collect_fragments(wait_fragments, wait_parent_filter));
  ASSERT_EQ(1, wait_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(wait_fragments.at(0)));
  // parent 逻辑区间是 [8K, 198K), 但 DIO 会写到尾页末端 200K. child 从
  // 198K 开始并重写同一尾页, 因此首个最大 2MB 的物理区间必须等待 parent.
  EXPECT_TRUE(wait_fragments.at(0)->get_parent_ref().is_equal(parent_ref));
  EXPECT_EQ(make_lsn(wait_parent_begin_off), wait_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(make_lsn(wait_parent_end_off), wait_fragments.at(0)->get_end_lsn());
  EXPECT_EQ(WAIT_PARENT_FRAGMENT_MAX_SIZE, wait_fragments.at(0)->get_fragment_max_size());

  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(2, ready_fragments.count());
  ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(0)));
  ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(1)));
  EXPECT_EQ(make_lsn(wait_parent_end_off), ready_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(make_lsn(parent_end_off + child_buf0_len), ready_fragments.at(0)->get_end_lsn());
  EXPECT_EQ(make_lsn(parent_end_off + child_buf0_len), ready_fragments.at(1)->get_begin_lsn());
  EXPECT_EQ(make_lsn(parent_end_off + child_len), ready_fragments.at(1)->get_end_lsn());
  EXPECT_EQ(NORMAL_FRAGMENT_MAX_SIZE, ready_fragments.at(0)->get_fragment_max_size());
  EXPECT_EQ(NORMAL_FRAGMENT_MAX_SIZE, ready_fragments.at(1)->get_fragment_max_size());

  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(make_lsn(parent_end_off + child_len), status.get_planned_end_lsn());
  EXPECT_FALSE(status.has_pending_source());
}

TEST(TestPalfAsyncFragment, AsyncWriteStructValidatesLifetimeFields)
{
  UnitTaskArena arena;
  AsyncPalfIOCtx ctx;
  AsyncPwriteRequest request;
  const LSN begin_lsn = make_lsn(0);
  char *buf = arena.alloc_aligned_buf(begin_lsn, LOG_DIO_ALIGN_SIZE);
  const FragmentRef ref(0 /* slot_id */, 0 /* generation */);
  ASSERT_TRUE(OB_NOT_NULL(buf));

  ASSERT_EQ(OB_SUCCESS, request.init(begin_lsn, buf, LOG_DIO_ALIGN_SIZE,
                                     &ctx, ref, 1 /* submit_ts */));
  EXPECT_TRUE(request.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(LSN(), buf, LOG_DIO_ALIGN_SIZE, &ctx, ref, 1 /* submit_ts */));
  EXPECT_FALSE(request.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(begin_lsn, NULL, LOG_DIO_ALIGN_SIZE, &ctx, ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(begin_lsn, buf, 0 /* aligned_buf_len */, &ctx, ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(begin_lsn, buf, LOG_DIO_ALIGN_SIZE, NULL, ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(begin_lsn, buf, LOG_DIO_ALIGN_SIZE, &ctx, FragmentRef(), 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(begin_lsn, buf, LOG_DIO_ALIGN_SIZE, &ctx, ref, 0 /* submit_ts */));

  AsyncIOCompletionEvent event;
  event.ctx.fragment_ref = ref;
  event.ret_code = OB_IO_ERROR;
  event.finish_ts = 1;
  EXPECT_TRUE(event.is_valid());
  event.reset();
  EXPECT_FALSE(event.is_valid());
}

TEST(TestPalfAsyncFragment, FragmentCompletionAndGenerationAreCloseOnce)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  char *buf = arena.alloc_aligned_buf(make_lsn(0), 2 * LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  FragmentRef old_ref;
  int64_t planned_len = 0;
  ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE,
                                        LOG_DIO_ALIGN_SIZE, FragmentRef(),
                                        old_ref, planned_len));
  PhysicalWriteFragment *fragment = NULL;
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(old_ref, fragment));
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  EXPECT_EQ(OB_INVALID_ARGUMENT, fragment->mark_submitted(old_ref, 0 /* submit_ts */));
  ASSERT_EQ(OB_SUCCESS, fragment->mark_submitted(old_ref, 1 /* submit_ts */));

  bool completed_by_me = false;
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, fragment->mark_io_completed(
      old_ref, OB_SUCCESS, 0 /* next_retry_ts */,
      100 /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  EXPECT_TRUE(completed_by_me);
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, completed_data_len);
  EXPECT_EQ(1, submit_ts);
  completed_by_me = true;
  ASSERT_EQ(OB_SUCCESS, fragment->mark_io_completed(
      old_ref, OB_SUCCESS, 0 /* next_retry_ts */,
      200 /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  EXPECT_FALSE(completed_by_me);
  EXPECT_EQ(0, completed_data_len);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, submit_ts);
  int64_t remaining_delay_us = 0;
  ASSERT_EQ(OB_SUCCESS, fragment->get_remaining_finish_delay(
      150 /* now_us */, 100 /* aio_delay_us */, remaining_delay_us));
  EXPECT_EQ(50, remaining_delay_us);
  ASSERT_EQ(OB_SUCCESS, pool.free_slot(old_ref));

  FragmentRef new_ref;
  ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(make_lsn(LOG_DIO_ALIGN_SIZE),
                                        buf + LOG_DIO_ALIGN_SIZE,
                                        LOG_DIO_ALIGN_SIZE,
                                        LOG_DIO_ALIGN_SIZE,
                                        FragmentRef(), new_ref, planned_len));
  ASSERT_EQ(old_ref.slot_id, new_ref.slot_id);
  ASSERT_NE(old_ref.generation, new_ref.generation);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, pool.free_slot(old_ref));
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(new_ref, fragment));
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  EXPECT_TRUE(fragment->is_ready());
  ASSERT_EQ(OB_SUCCESS, pool.free_slot(new_ref));
}

TEST(TestPalfAsyncFragment, FragmentRejectsNonAlignedFirstSource)
{
  UnitTaskArena arena;
  PhysicalWriteFragment fragment;
  FragmentRef ref;
  const LSN begin_lsn = make_lsn(2048);
  const int64_t len = 1024;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), len);

  ASSERT_TRUE(OB_NOT_NULL(buf));
  int64_t planned_len = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, fragment.alloc_from_free(0 /* slot_id */, begin_lsn,
                                                       buf, len, LOG_DIO_ALIGN_SIZE,
                                                       FragmentRef(), ref, planned_len));
}

TEST(TestPalfAsyncFragment, PlannerRejectsNonAppendableFragment)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  PhysicalWriteFragment fragment;
  LogAsyncWritePlanner planner(pool);
  FragmentRef ref;
  const LSN begin_lsn = make_lsn(0);
  char *buf = arena.alloc_aligned_buf(begin_lsn, LOG_DIO_ALIGN_SIZE);
  int64_t planned_len = -1;

  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, fragment.alloc_from_free(0 /* slot_id */, begin_lsn,
                                                buf, LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE,
                                                FragmentRef(), ref, planned_len));
  ASSERT_EQ(LOG_DIO_ALIGN_SIZE, planned_len);
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            planner.append_source_range_to_fragment_(fragment, ref, begin_lsn, buf, LOG_DIO_ALIGN_SIZE, planned_len));
  EXPECT_EQ(0, planned_len);
  EXPECT_TRUE(fragment.get_begin_lsn().is_valid());
}

TEST(TestPalfAsyncFragment, PoolReportsOldestPendingFragmentTs)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  FragmentRef ref1;
  FragmentRef ref2;
  PhysicalWriteFragment *frag1 = NULL;
  PhysicalWriteFragment *frag2 = NULL;
  char *buf1 = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  char *buf2 = arena.alloc_aligned_buf(make_lsn(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE);
  int64_t planned_len = 0;
  bool completed_by_me = false;

  ASSERT_TRUE(OB_NOT_NULL(buf1));
  ASSERT_TRUE(OB_NOT_NULL(buf2));
  ASSERT_EQ(OB_SUCCESS, pool.init());
  ASSERT_EQ(OB_INVALID_TIMESTAMP, pool.get_oldest_pending_io_start_ts());
  ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(make_lsn(0), buf1, LOG_DIO_ALIGN_SIZE,
                                        NORMAL_FRAGMENT_MAX_SIZE, FragmentRef(), ref1, planned_len));
  ASSERT_EQ(LOG_DIO_ALIGN_SIZE, planned_len);
  ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(make_lsn(LOG_DIO_ALIGN_SIZE), buf2, LOG_DIO_ALIGN_SIZE,
                                        NORMAL_FRAGMENT_MAX_SIZE, FragmentRef(), ref2, planned_len));
  ASSERT_EQ(LOG_DIO_ALIGN_SIZE, planned_len);
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(ref1, frag1));
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(ref2, frag2));
  ASSERT_TRUE(OB_NOT_NULL(frag1));
  ASSERT_TRUE(OB_NOT_NULL(frag2));

  ASSERT_EQ(OB_SUCCESS, frag1->mark_submitted(ref1, 200 /* submit_ts */));
  ASSERT_EQ(200, pool.get_oldest_pending_io_start_ts());
  ASSERT_EQ(OB_SUCCESS, frag2->mark_submitted(ref2, 100 /* submit_ts */));
  ASSERT_EQ(100, pool.get_oldest_pending_io_start_ts());

  ASSERT_EQ(OB_SUCCESS, frag2->mark_failed(ref2, OB_TIMEOUT, 0 /* next_retry_ts */));
  ASSERT_EQ(100, pool.get_oldest_pending_io_start_ts());
  ASSERT_EQ(OB_SUCCESS, frag2->mark_submitted(ref2, 100 /* submit_ts */));
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, frag2->mark_io_completed(
      ref2, OB_SUCCESS, 0 /* next_retry_ts */,
      common::ObTimeUtility::current_time() /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  ASSERT_TRUE(completed_by_me);
  ASSERT_EQ(LOG_DIO_ALIGN_SIZE, completed_data_len);
  ASSERT_EQ(100, submit_ts);
  EXPECT_EQ(200, pool.get_oldest_pending_io_start_ts());
  pool.destroy();
}

TEST(TestPalfAsyncFragment, FragmentAppendsOnlyWhenWaitingParent)
{
  UnitTaskArena arena;
  PhysicalWriteFragment fragment;
  FragmentRef ref;
  int64_t data_len = 0;
  const LSN begin_lsn = make_lsn(0);
  char *buf = arena.alloc_aligned_buf(begin_lsn, LOG_DIO_ALIGN_SIZE);
  int64_t planned_len = 0;

  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, fragment.alloc_from_free(0 /* slot_id */, begin_lsn,
                                                buf, 1024, LOG_DIO_ALIGN_SIZE,
                                                FragmentRef(1, 1), ref, planned_len));
  ASSERT_EQ(1024, planned_len);
  ASSERT_TRUE(fragment.is_wait_parent());
  EXPECT_EQ(OB_SUCCESS, fragment.append_source(make_lsn(1024), 1024, buf + 1024));
  EXPECT_EQ(OB_SUCCESS, fragment.get_data_len(data_len));
  EXPECT_EQ(2048, data_len);
}

TEST(TestPalfAsyncFragment, FragmentRejectsInvalidAppendWithoutChangingRange)
{
  UnitTaskArena arena;
  PhysicalWriteFragment fragment;
  FragmentRef ref;
  int64_t planned_len = 0;
  int64_t data_len = 0;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), 2 * LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, fragment.alloc_from_free(
      0 /* slot_id */, make_lsn(0), buf, 1024,
      LOG_DIO_ALIGN_SIZE, FragmentRef(1, 1), ref, planned_len));
  ASSERT_TRUE(fragment.is_wait_parent());

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            fragment.append_source(make_lsn(2048), 512, buf + 1024));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            fragment.append_source(make_lsn(1024), 512, buf + 2048));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            fragment.append_source(make_lsn(1024), LOG_DIO_ALIGN_SIZE, buf + 1024));
  ASSERT_EQ(OB_SUCCESS, fragment.get_data_len(data_len));
  EXPECT_EQ(1024, data_len);
  EXPECT_EQ(make_lsn(1024), fragment.get_end_lsn());
}

TEST(TestPalfAsyncFragment, PoolReportsRetryAndFinishDelayIntervals)
{
  UnitTaskArena arena;
  PhysicalWriteFragmentPool pool;
  PhysicalWriteFragment *fragment = NULL;
  FragmentRef ref;
  int64_t planned_len = 0;
  const int64_t now = common::ObTimeUtility::current_time();
  int64_t next_drive_interval_us = INT64_MAX;
  char *buf = arena.alloc_aligned_buf(make_lsn(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_EQ(OB_SUCCESS, pool.init());
  ASSERT_EQ(OB_SUCCESS, pool.alloc_slot(make_lsn(0), buf, LOG_DIO_ALIGN_SIZE,
                                        LOG_DIO_ALIGN_SIZE, FragmentRef(), ref, planned_len));
  ASSERT_EQ(OB_SUCCESS, pool.get_fragment(ref, fragment));
  ASSERT_TRUE(OB_NOT_NULL(fragment));
  ASSERT_EQ(OB_SUCCESS, pool.get_next_drive_interval(now, 100 /* aio_delay_us */,
                                                      next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);

  ASSERT_EQ(OB_SUCCESS, fragment->mark_submitted(ref, 1 /* submit_ts */));
  ASSERT_EQ(OB_SUCCESS, pool.get_next_drive_interval(now, 100 /* aio_delay_us */,
                                                      next_drive_interval_us));
  EXPECT_EQ(INT64_MAX, next_drive_interval_us);
  ASSERT_EQ(OB_SUCCESS, fragment->mark_failed(ref, OB_IO_ERROR, now + 100));
  ASSERT_EQ(OB_SUCCESS, pool.get_next_drive_interval(now, 100 /* aio_delay_us */,
                                                      next_drive_interval_us));
  EXPECT_EQ(100, next_drive_interval_us);

  ASSERT_EQ(OB_SUCCESS, fragment->mark_submitted(ref, 2 /* submit_ts */));
  bool completed_by_me = false;
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, fragment->mark_io_completed(
      ref, OB_SUCCESS, 0 /* next_retry_ts */, now /* finish_ts */,
      completed_by_me, completed_data_len, submit_ts));
  EXPECT_TRUE(completed_by_me);
  ASSERT_EQ(OB_SUCCESS, pool.get_next_drive_interval(now, 100 /* aio_delay_us */,
                                                      next_drive_interval_us));
  EXPECT_EQ(100, next_drive_interval_us);
  ASSERT_EQ(OB_SUCCESS, pool.get_next_drive_interval(now + 100, 100 /* aio_delay_us */,
                                                      next_drive_interval_us));
  EXPECT_EQ(0, next_drive_interval_us);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  const uint64_t tenant_id = 1001;
  oceanbase::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id);
  oceanbase::share::ObTenantBase tenant_base(tenant_id);
  oceanbase::share::ObTenantEnv::set_tenant(&tenant_base);
  oceanbase::common::ObLogger::get_logger().set_file_name("test_palf_async_fragment.log", true);
  testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  oceanbase::ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id);
  return ret;
}

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

#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/ob_define.h"

#define private public
#define protected public

#include "logservice/palf/log_async_io_struct.h"
#include "logservice/palf/log_async_palf_ctx_interface.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_io_adapter.h"
#include "logservice/palf/log_io_task.h"
#include "logservice/palf/log_io_worker_base.h"
#include "logservice/palf/log_engine.h"
#include "logservice/palf/log_storage.h"
#include "logservice/palf/log_writer_utils.h"
#include "logservice/palf/lsn.h"

#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;

namespace
{
void init_storage_state(LogStorage &s,
                        const int64_t logical_block_size,
                        const LSN &base_lsn,
                        const int64_t curr_block_writable_size,
                        const bool need_header)
{
  s.is_inited_ = true;
  s.logical_block_size_ = logical_block_size;
  s.palf_id_ = 1;
  s.log_tail_ = base_lsn;
  s.readable_log_tail_ = base_lsn;
  s.curr_block_writable_size_ = curr_block_writable_size;
  s.need_append_block_header_ = need_header;
}

class OwnershipTrackingIOTask : public LogIOTask
{
public:
  OwnershipTrackingIOTask()
    : LogIOTask(1, 1), released_(false), post_release_type_read_count_(0)
  {}
  ~OwnershipTrackingIOTask() override {}
  bool is_released() const { return released_; }
  int64_t get_post_release_type_read_count() const { return post_release_type_read_count_; }

protected:
  int do_task_(int, IPalfHandleImplGuard &) override { return OB_SUCCESS; }
  int after_consume_(IPalfHandleImplGuard &) override { return OB_SUCCESS; }
  LogIOTaskType get_io_task_type_() const override
  {
    if (released_) {
      ++post_release_type_read_count_;
    }
    return LogIOTaskType::FLUSH_LOG_TYPE;
  }
  void free_this_(IPalfEnvImpl *) override { released_ = true; }
  int64_t get_io_size_() const override { return 0; }
  bool need_purge_throttling_() const override { return false; }

private:
  bool released_;
  mutable int64_t post_release_type_read_count_;
};

class ImmediateConsumeIOWorker : public LogIOWorkerBase
{
public:
  ImmediateConsumeIOWorker() {}
  ~ImmediateConsumeIOWorker() override {}
  void destroy() override {}
  int submit_io_task(LogIOTask *task) override
  {
    task->free_this(NULL);
    return OB_SUCCESS;
  }
  int64_t get_oldest_pending_io_start_ts() const override { return OB_INVALID_TIMESTAMP; }
};


} // namespace

TEST(TestPalfAsyncStorage, SubmitDoesNotReadTaskAfterOwnershipTransfer)
{
  ImmediateConsumeIOWorker worker;
  LogEngine engine;
  OwnershipTrackingIOTask task;
  engine.io_task_submitter_ = &worker;

  OB_LOGGER.set_log_level("TRACE");
  const int ret = engine.submit_io_task_(&task);
  OB_LOGGER.set_log_level("INFO");

  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(task.is_released());
  EXPECT_EQ(0, task.get_post_release_type_read_count());
}

TEST(TestPalfAsyncStorage, PublishedTail)
{
  const int64_t lbs = PALF_BLOCK_SIZE;
  LogStorage s;
  LogStorage::AsyncStorageSnapshot snapshot;
  init_storage_state(s, lbs, LSN(0), lbs, false /* need_header */);

  EXPECT_EQ(LSN(0), s.get_end_lsn());
  EXPECT_EQ(OB_SUCCESS, s.commit_async_append(LSN(0), LSN(4096)));
  EXPECT_EQ(LSN(4096), s.log_tail_);
  EXPECT_EQ(LSN(4096), s.readable_log_tail_);
  s.get_async_storage_snapshot(snapshot);
  EXPECT_EQ(LSN(4096), snapshot.log_tail);
  EXPECT_EQ(lbs - 4096, snapshot.curr_block_writable_size);
  EXPECT_FALSE(snapshot.need_append_block_header);

  EXPECT_EQ(OB_ERR_UNEXPECTED, s.commit_async_append(LSN(0), LSN(4097)));
  EXPECT_EQ(LSN(4096), s.log_tail_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, s.commit_async_append(LSN(4096), LSN(4095)));
  EXPECT_EQ(LSN(4096), s.log_tail_);

  s.readable_log_tail_ = LSN(lbs + LOG_DIO_ALIGN_SIZE);
  EXPECT_EQ(OB_SUCCESS, s.commit_async_append(LSN(4096), LSN(lbs)));
  s.get_async_storage_snapshot(snapshot);
  EXPECT_EQ(LSN(lbs), snapshot.log_tail);
  EXPECT_EQ(0, snapshot.curr_block_writable_size);
  EXPECT_EQ(LSN(lbs + LOG_DIO_ALIGN_SIZE), s.readable_log_tail_);

  s.is_inited_ = false;
}

class WakeRecordingCtx : public IAsyncPalfIOCtx
{
public:
  explicit WakeRecordingCtx(const bool need_wake)
    : need_wake_(need_wake),
      request_drives_(0),
      active_ref_(0),
      completions_(0),
      completion_ret_(OB_SUCCESS),
      request_drive_ret_(OB_SUCCESS),
      last_event_(),
      unpin_after_wake_ok_(true)
  {}
  ~WakeRecordingCtx() override {}
  int64_t get_palf_id() const override { return 9; }
  int try_reserve_task_slot(const LogIOTaskType task_type) override
  {
    UNUSED(task_type);
    return OB_SUCCESS;
  }
  void release_task_slot(const LogIOTaskType task_type) override
  {
    UNUSED(task_type);
  }
  int enqueue_task(LogIOTask *) override { return OB_SUCCESS; }
  int drive_write(int64_t &next_drive_interval_us) override
  {
    next_drive_interval_us = INT64_MAX;
    return OB_SUCCESS;
  }
  int on_aio_complete(const AsyncIOCompletionEvent &event,
                      bool &need_wake_worker) override
  {
    ++completions_;
    last_event_ = event;
    need_wake_worker = need_wake_;
    return completion_ret_;
  }
  int request_drive() override
  {
    if (active_ref_ <= 0) {
      unpin_after_wake_ok_ = false;
    }
    ++request_drives_;
    return request_drive_ret_;
  }
  bool is_drained() const override { return true; }
  int64_t get_inflight_count() const override { return 0; }
  int64_t get_oldest_pending_io_start_ts() const override { return 0; }
  int64_t get_throttle_next_admit_ts() const override { return 0; }
  void pin() override { ++active_ref_; }
  void unpin() override { --active_ref_; }
  int64_t get_active_ref() const override { return active_ref_; }
  void free_this() override {}
  void set_completion_ret(const int ret) { completion_ret_ = ret; }
  void set_request_drive_ret(const int ret) { request_drive_ret_ = ret; }
  const AsyncIOCompletionEvent &get_last_event() const { return last_event_; }
  TO_STRING_KV(K_(need_wake), K_(request_drives), K_(active_ref), K_(completions));

  bool need_wake_;
  int64_t request_drives_;
  int64_t active_ref_;
  int64_t completions_;
  int completion_ret_;
  int request_drive_ret_;
  AsyncIOCompletionEvent last_event_;
  bool unpin_after_wake_ok_;
};

TEST(TestPalfAsyncStorage, AsyncIOValueContracts)
{
  char *payload = static_cast<char *>(
      ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "TestAioValue"));
  ASSERT_TRUE(OB_NOT_NULL(payload));
  WakeRecordingCtx ctx(false /* need_wake */);
  AsyncPwriteRequest request;
  FragmentRef fragment_ref(2, 3);
  FragmentRef same_ref(2, 3);
  FragmentRef other_ref(2, 4);
  EXPECT_TRUE(fragment_ref.is_valid());
  EXPECT_TRUE(fragment_ref.is_equal(same_ref));
  EXPECT_FALSE(fragment_ref.is_equal(other_ref));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(LSN(), payload, LOG_DIO_ALIGN_SIZE, &ctx,
                         fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(LSN(0), NULL, LOG_DIO_ALIGN_SIZE, &ctx,
                         fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(LSN(0), payload, 0, &ctx,
                         fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(LSN(0), payload, LOG_DIO_ALIGN_SIZE, NULL,
                         fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(LSN(0), payload, LOG_DIO_ALIGN_SIZE, &ctx,
                         FragmentRef(), 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            request.init(LSN(0), payload, LOG_DIO_ALIGN_SIZE, &ctx,
                         fragment_ref, 0 /* submit_ts */));
  EXPECT_FALSE(request.is_valid());
  ASSERT_EQ(OB_SUCCESS,
            request.init(LSN(0), payload, LOG_DIO_ALIGN_SIZE, &ctx,
                         fragment_ref, 1 /* submit_ts */));
  EXPECT_TRUE(request.is_valid());

  AsyncIOCompletionEvent event;
  event.ctx.fragment_ref = fragment_ref;
  event.ret_code = OB_IO_ERROR;
  event.finish_ts = 1;
  EXPECT_TRUE(event.is_valid());
  event.reset();
  EXPECT_FALSE(event.is_valid());

  fragment_ref.reset();
  EXPECT_FALSE(fragment_ref.is_valid());
  ob_free_align(payload);
}

TEST(TestPalfAsyncStorage, AsyncPwriteDIOValidation)
{
  const int64_t lbs = PALF_BLOCK_SIZE;
  LogStorage s;
  const int64_t payload_size = LOG_DIO_ALIGN_SIZE;
  const int64_t big_payload_size = 2 * LOG_DIO_ALIGN_SIZE;
  char *payload = static_cast<char *>(
      ob_malloc_align(LOG_DIO_ALIGN_SIZE, payload_size, "TestAioBuf"));
  char *big_payload = static_cast<char *>(
      ob_malloc_align(LOG_DIO_ALIGN_SIZE, big_payload_size, "TestAioBuf"));
  common::ObIOHandle io_h;
  init_storage_state(s, lbs, LSN(0), lbs, false /* need_header */);
  ASSERT_TRUE(OB_NOT_NULL(payload));
  ASSERT_TRUE(OB_NOT_NULL(big_payload));

  WakeRecordingCtx ctx(false /* need_wake */);
  const FragmentRef fragment_ref(0, 1);
  AsyncPwriteRequest req;

  ASSERT_EQ(OB_SUCCESS, req.init(LSN(0), payload, payload_size,
                                 &ctx, fragment_ref, 1 /* submit_ts */));
  s.is_inited_ = false;
  EXPECT_EQ(OB_NOT_INIT, s.async_pwrite(req, io_h));

  s.is_inited_ = true;

  ASSERT_EQ(OB_SUCCESS, req.init(LSN(0), payload, payload_size - 1,
                                 &ctx, fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.async_pwrite(req, io_h));

  ASSERT_EQ(OB_SUCCESS, req.init(LSN(1), payload, payload_size,
                                 &ctx, fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.async_pwrite(req, io_h));

  ASSERT_EQ(OB_SUCCESS, req.init(LSN(0), big_payload + 1, payload_size,
                                 &ctx, fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.async_pwrite(req, io_h));

  ASSERT_EQ(OB_SUCCESS, req.init(LSN(lbs - payload_size), big_payload, big_payload_size,
                                 &ctx, fragment_ref, 1 /* submit_ts */));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.async_pwrite(req, io_h));

  req.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, s.async_pwrite(req, io_h));

  s.is_inited_ = false;
  ob_free_align(payload);
  ob_free_align(big_payload);
}

TEST(TestPalfAsyncStorage, AsyncPwriteDelegatesAndPropagatesLayerErrors)
{
  char *payload = static_cast<char *>(
      ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "TestAioLayer"));
  ASSERT_TRUE(OB_NOT_NULL(payload));
  WakeRecordingCtx ctx(false /* need_wake */);
  AsyncPwriteRequest request;
  common::ObIOHandle io_handle;
  ASSERT_EQ(OB_SUCCESS,
            request.init(LSN(0), payload, LOG_DIO_ALIGN_SIZE, &ctx,
                         FragmentRef(0, 1), 1 /* submit_ts */));

  LogStorage storage;
  init_storage_state(storage, PALF_BLOCK_SIZE, LSN(0), PALF_BLOCK_SIZE,
                     false /* need_header */);
  EXPECT_EQ(OB_NOT_INIT, storage.async_pwrite(request, io_handle));
  storage.is_inited_ = false;

  LogBlockMgr block_mgr;
  EXPECT_EQ(OB_NOT_INIT, block_mgr.aio_write(0, 0, request, io_handle));
  block_mgr.is_inited_ = true;
  block_mgr.log_block_size_ = PALF_BLOCK_SIZE;
  block_mgr.curr_writable_block_id_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            block_mgr.aio_write(1, 0, request, io_handle));
  EXPECT_EQ(OB_NOT_INIT,
            block_mgr.aio_write(0, 0, request, io_handle));
  block_mgr.is_inited_ = false;

  LogBlockHandler handler;
  EXPECT_EQ(OB_NOT_INIT, handler.aio_write(0, request, io_handle));
  handler.is_inited_ = true;
  handler.log_block_size_ = PALF_BLOCK_SIZE;
  EXPECT_EQ(OB_ERR_UNEXPECTED, handler.aio_write(0, request, io_handle));
  handler.is_inited_ = false;

  LogIOAdapter adapter;
  ObIOFd io_fd;
  EXPECT_EQ(OB_NOT_INIT, adapter.aio_write(io_fd, 0, request, io_handle));
  adapter.is_inited_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            adapter.aio_write(io_fd, 0, request, io_handle));
  adapter.is_inited_ = false;

  ob_free_align(payload);
}

// TODO(shouju.zyp): IMPORTANT INTERFACE INTEGRATION COVERAGE.
// Add one real aligned temporary-file write that reaches ObIOManager and its
// callback after a reusable PALF storage fixture is available. The test must
// verify persisted bytes, caller-buffer lifetime, callback metadata, and that
// both submit failure and completion release the ctx pin/outstanding count.

TEST(TestPalfAsyncStorage, ReadLogStorageTailPageValidation)
{
  const int64_t lbs = PALF_BLOCK_SIZE;
  LogStorage s;
  int64_t read_size = -1;
  char *payload = static_cast<char *>(
      ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "TestTailPage"));
  ASSERT_TRUE(OB_NOT_NULL(payload));

  EXPECT_EQ(OB_NOT_INIT, s.read_log_storage_tail_page(LSN(0), payload,
                                                     LOG_DIO_ALIGN_SIZE, read_size));
  EXPECT_EQ(0, read_size);

  init_storage_state(s, lbs, LSN(LOG_DIO_ALIGN_SIZE), lbs - LOG_DIO_ALIGN_SIZE,
                     false /* need_header */);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.read_log_storage_tail_page(LSN(1), payload, LOG_DIO_ALIGN_SIZE, read_size));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.read_log_storage_tail_page(LSN(0), NULL, LOG_DIO_ALIGN_SIZE, read_size));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.read_log_storage_tail_page(LSN(0), payload, LOG_DIO_ALIGN_SIZE - 1, read_size));
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND,
            s.read_log_storage_tail_page(LSN(0), payload, LOG_DIO_ALIGN_SIZE, read_size));
  EXPECT_EQ(0, read_size);

  s.is_inited_ = false;
  ob_free_align(payload);
}

// TODO(shouju.zyp): IMPORTANT INTERFACE INTEGRATION COVERAGE.
// Extend ReadLogStorageTailPageValidation with a real 4K page read and an
// injected device read failure once the temporary PALF storage fixture above
// is available.

// TODO(shouju.zyp): IMPORTANT CROSS-BRANCH COVERAGE.
// Dirty-suffix recovery, iterator warning policy, and recovery-manifest refresh
// belong to task/2026061200116711128. Keep those tests on the recovery branch;
// restore them here only in a merged validation worktree with the matching
// recovery implementation.

TEST(TestPalfAsyncStorage, AIOCallbackUsesProcessLifetimeAllocator)
{
  LogAsyncIOCallback callback;
  EXPECT_EQ(lib::ObMallocAllocator::get_instance(), callback.get_allocator());
}

TEST(TestPalfAsyncStorage, AIOCallbackWakesWorkerWhenNeeded)
{
  WakeRecordingCtx ctx_wake(true /* need_wake */);
  {
    LogAsyncIOCallback cb(&ctx_wake, FragmentRef(0, 1),
                          LSN(0), LSN(LOG_DIO_ALIGN_SIZE), 1 /* submit_ts */);

    EXPECT_EQ(OB_SUCCESS, cb.inner_process(NULL, 0));
    EXPECT_EQ(1, ctx_wake.completions_);
    EXPECT_EQ(1, ctx_wake.request_drives_);
    EXPECT_TRUE(ctx_wake.unpin_after_wake_ok_);
    EXPECT_EQ(0, ctx_wake.get_active_ref());
  }

  WakeRecordingCtx ctx_nowake(false /* need_wake */);
  {
    LogAsyncIOCallback cb2(&ctx_nowake, FragmentRef(0, 1),
                           LSN(0), LSN(LOG_DIO_ALIGN_SIZE), 1 /* submit_ts */);

    EXPECT_EQ(OB_SUCCESS, cb2.inner_process(NULL, 0));
    EXPECT_EQ(1, ctx_nowake.completions_);
    EXPECT_EQ(0, ctx_nowake.request_drives_);
    EXPECT_EQ(0, ctx_nowake.get_active_ref());
  }
}

TEST(TestPalfAsyncStorage, AIOCallbackConstructorPinsAndDestructorReleasesCtx)
{
  WakeRecordingCtx ctx(false /* need_wake */);
  {
    LogAsyncIOCallback cb(&ctx, FragmentRef(0, 1),
                          LSN(0), LSN(LOG_DIO_ALIGN_SIZE), 1 /* submit_ts */);
    EXPECT_EQ(1, ctx.get_active_ref());
  }
  EXPECT_EQ(0, ctx.get_active_ref());
}

TEST(TestPalfAsyncStorage, AIOCallbackRejectsNullCtx)
{
  {
    LogAsyncIOCallback cb(NULL /* ctx */, FragmentRef(0, 1),
                          LSN(0), LSN(LOG_DIO_ALIGN_SIZE), 1 /* submit_ts */);
    EXPECT_EQ(OB_ERR_UNEXPECTED, cb.inner_process(NULL, 0));
  }
}

TEST(TestPalfAsyncStorage, AIOCallbackReportsMetadataAndReleasesOnCtxError)
{
  const LSN begin_lsn(LOG_DIO_ALIGN_SIZE);
  const LSN end_lsn(2 * LOG_DIO_ALIGN_SIZE);
  const int64_t submit_ts = 123;
  WakeRecordingCtx ctx(false /* need_wake */);
  ctx.set_completion_ret(OB_EAGAIN);
  {
    LogAsyncIOCallback cb(&ctx, FragmentRef(3, 9), begin_lsn, end_lsn,
                          submit_ts);
    EXPECT_EQ(OB_SUCCESS, cb.inner_process(NULL, 0));
    EXPECT_EQ(1, ctx.completions_);
    EXPECT_EQ(0, ctx.request_drives_);
    EXPECT_EQ(0, ctx.get_active_ref());
    EXPECT_TRUE(ctx.get_last_event().ctx.fragment_ref.is_equal(FragmentRef(3, 9)));
    EXPECT_EQ(begin_lsn, ctx.get_last_event().ctx.begin_lsn);
    EXPECT_EQ(end_lsn, ctx.get_last_event().ctx.end_lsn);
    EXPECT_EQ(submit_ts, ctx.get_last_event().ctx.submit_ts);
  }
}

TEST(TestPalfAsyncStorage, AIOCallbackReleasesPinWhenWakeRequestFails)
{
  WakeRecordingCtx ctx(true /* need_wake */);
  ctx.set_request_drive_ret(OB_EAGAIN);
  {
    LogAsyncIOCallback cb(&ctx, FragmentRef(0, 1),
                          LSN(0), LSN(LOG_DIO_ALIGN_SIZE), 1 /* submit_ts */);
    EXPECT_EQ(OB_SUCCESS, cb.inner_process(NULL, 0));
    EXPECT_EQ(1, ctx.request_drives_);
    EXPECT_EQ(0, ctx.get_active_ref());
  }
}

TEST(TestPalfAsyncStorage, WriteCopyBudgetStat)
{
  LogBlockHandler handler;
  handler.sec_stat_trace_time_ = ObClockGenerator::getClock();

  handler.record_write_copy_budget_stat_(100, 9000);

  EXPECT_EQ(9000, handler.accum_input_bytes_);
  EXPECT_EQ(3996, handler.accum_head_pad_bytes_);
  EXPECT_EQ(908, handler.accum_tail_pad_bytes_);
  EXPECT_EQ(4096, handler.accum_body_aligned_bytes_);
  EXPECT_EQ(1, handler.accum_write_call_count_);
}

// TODO(shouju.zyp): IMPORTANT INTERFACE INTEGRATION COVERAGE.
// These state-only tests verify validation and lower-layer error propagation.
// Add successful first-block header creation, block switch, and idempotent retry
// with the real temporary PALF storage fixture described above. The same
// fixture must also verify that the shared prepare helper keeps sync writev
// behavior unchanged.
TEST(TestPalfAsyncStorage, PrepareFirstBlockPropagatesBlockManagerFailure)
{
  LogStorage s;
  init_storage_state(s, PALF_BLOCK_SIZE, LSN(0), PALF_BLOCK_SIZE, true /* need_header */);

  EXPECT_EQ(OB_NOT_INIT,
            s.prepare_async_block_for_write(share::SCN::min_scn()));
  EXPECT_EQ(OB_NOT_INIT,
            s.prepare_async_block_for_write(share::SCN::min_scn()));

  s.is_inited_ = false;
}

TEST(TestPalfAsyncStorage, PrepareFirstBlockAlreadyDone)
{
  LogStorage s;
  init_storage_state(s, PALF_BLOCK_SIZE, LSN(0), PALF_BLOCK_SIZE, false /* need_header */);

  EXPECT_EQ(OB_SUCCESS,
            s.prepare_async_block_for_write(share::SCN::min_scn()));

  s.is_inited_ = false;
}

TEST(TestPalfAsyncStorage, PrepareNormalBoundaryMisalignedTail)
{
  LogStorage s;
  init_storage_state(s, PALF_BLOCK_SIZE, LSN(123), PALF_BLOCK_SIZE, false /* need_header */);

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            s.prepare_async_block_for_write(share::SCN::min_scn()));

  s.is_inited_ = false;
}

TEST(TestPalfAsyncStorage, PrepareNormalBoundaryPropagatesHeaderWriteFailure)
{
  LogStorage s;
  init_storage_state(s, PALF_BLOCK_SIZE, LSN(PALF_BLOCK_SIZE),
                     PALF_BLOCK_SIZE, true /* need_header */);

  EXPECT_EQ(OB_NOT_INIT,
            s.prepare_async_block_for_write(share::SCN::min_scn()));

  s.is_inited_ = false;
}

TEST(TestPalfAsyncStorage, PrepareNormalBoundaryPropagatesSwitchFailure)
{
  LogStorage s;
  init_storage_state(s, PALF_BLOCK_SIZE, LSN(PALF_BLOCK_SIZE),
                     0, false /* need_header */);

  EXPECT_EQ(OB_NOT_INIT,
            s.prepare_async_block_for_write(share::SCN::min_scn()));

  s.is_inited_ = false;
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_palf_async_storage.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_palf_async_storage");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

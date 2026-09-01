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
#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/time/ob_time_utility.h"
#include "logservice/palf/log_io_task.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "unittest/logservice/mock_palf_handle_impl_for_async.h"

#define private public
#define protected public
#include "logservice/palf/log_async_write_planner.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

static const uint64_t TEST_TENANT_ID = 1001;

// Owns test tasks and their DIO-addressable source buffers until planner teardown.
class PlannerTaskArena
{
public:
  PlannerTaskArena() : task_count_(0), buffer_count_(0), tasks_(), buffers_() {}
  ~PlannerTaskArena()
  {
    reset();
  }

  void reset()
  {
    for (int64_t i = 0; i < task_count_; ++i)
    {
      if (OB_NOT_NULL(tasks_[i]))
      {
        tasks_[i]->destroy();
        delete tasks_[i];
        tasks_[i] = NULL;
      }
    }
    for (int64_t i = 0; i < buffer_count_; ++i)
    {
      if (OB_NOT_NULL(buffers_[i]))
      {
        ::free(buffers_[i]);
        buffers_[i] = NULL;
      }
    }
    task_count_ = 0;
    buffer_count_ = 0;
  }

  char *alloc_buffer(const LSN &begin_lsn, const int64_t len)
  {
    char *buf = NULL;
    void *base = NULL;
    const int64_t phase = begin_lsn.is_valid() ? begin_lsn.val_ % LOG_DIO_ALIGN_SIZE : 0;
    if (begin_lsn.is_valid() && len > 0 && buffer_count_ < MAX_BUFFER_COUNT &&
        0 == ::posix_memalign(&base, LOG_DIO_ALIGN_SIZE, len + LOG_DIO_ALIGN_SIZE))
    {
      buffers_[buffer_count_++] = static_cast<char *>(base);
      buf = static_cast<char *>(base) + phase;
      MEMSET(base, 0xa5, len + LOG_DIO_ALIGN_SIZE);
    }
    return buf;
  }

  LogIOFlushLogTask *make_task(const int64_t log_id, const LSN &begin_lsn, const int64_t logical_len, const char *buf0,
                               const int64_t len0, const char *buf1, const int64_t len1)
  {
    int ret = OB_SUCCESS;
    LogIOFlushLogTask *task = NULL;
    LogWriteBuf write_buf;
    share::SCN scn;
    if (OB_FAIL(scn.convert_for_logservice(1000 + log_id)))
    {
    } else if (task_count_ >= MAX_TASK_COUNT || !begin_lsn.is_valid() || logical_len <= 0 || OB_ISNULL(buf0) ||
               len0 <= 0 || len1 < 0 || (len1 > 0 && OB_ISNULL(buf1)))
    {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(write_buf.push_back(buf0, len0)))
    {
    } else if (len1 > 0 && OB_FAIL(write_buf.push_back(buf1, len1)))
    {
    } else if (OB_ISNULL(task = new LogIOFlushLogTask(1 /* palf_id */, 0 /* palf_epoch */)))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else
    {
      FlushLogCbCtx cb_ctx(log_id, scn, begin_lsn, 1 /* log_proposal_id */, logical_len, 1 /* curr_log_proposal_id */,
                           1 /* begin_ts */);
      if (OB_FAIL(task->init(cb_ctx, write_buf)))
      {
        delete task;
        task = NULL;
      } else
      {
        tasks_[task_count_++] = task;
      }
    }
    return task;
  }

  LogIOFlushLogTask *make_one_buffer_task(const int64_t log_id, const LSN &begin_lsn, const int64_t len,
                                          const char *buf)
  {
    return make_task(log_id, begin_lsn, len, buf, len, NULL, 0);
  }

private:
  static const int64_t MAX_TASK_COUNT = 128;
  static const int64_t MAX_BUFFER_COUNT = 128;
  int64_t task_count_;
  int64_t buffer_count_;
  LogIOFlushLogTask *tasks_[MAX_TASK_COUNT];
  char *buffers_[MAX_BUFFER_COUNT];
};

// Provides a fresh planner/pool pair and verifies that every case releases queued bookkeeping.
class TestLogAsyncWritePlanner : public testing::Test
{
public:
  TestLogAsyncWritePlanner() : arena_(), pool_(), planner_(pool_) {}

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, pool_.init());
    ASSERT_EQ(OB_SUCCESS, planner_.init());
    planner_.init_plan_state(LSN(0));
  }

  void TearDown() override
  {
    LogIOFlushLogTask *task = NULL;
    LogAsyncWritePlanner::QueuedFragmentRef *queued_ref = NULL;
    if (planner_.pending_task_queue_.is_inited())
    {
      while (OB_SUCCESS == planner_.pending_task_queue_.pop(task))
      {
        task = NULL;
      }
    }
    if (planner_.fragment_ref_queue_.is_inited())
    {
      while (OB_SUCCESS == planner_.fragment_ref_queue_.pop(queued_ref))
      {
        planner_.free_fragment_ref_item_(queued_ref);
        queued_ref = NULL;
      }
    }
    for (int64_t i = 0; i < LogAsyncWritePlanner::PENDING_SOURCE_CNT; ++i)
    {
      planner_.pending_sources_[i].reset();
    }
    planner_.last_fragment_ref_.reset();
    planner_.last_fragment_ref_item_ = NULL;
    if (planner_.inited_)
    {
      planner_.reset();
    }
  }

protected:
  void expect_status(const LSN &planned_end_lsn, const LSN &persisted_lsn, const int64_t pending_task_count,
                     const int64_t active_fragment_count, const bool has_pending_source)
  {
    PlannerStatus status;
    planner_.get_status(status);
    EXPECT_EQ(planned_end_lsn, status.get_planned_end_lsn());
    EXPECT_EQ(persisted_lsn, status.get_persisted_lsn());
    EXPECT_EQ(pending_task_count, status.get_pending_task_count());
    EXPECT_EQ(active_fragment_count, status.get_active_fragment_count());
    EXPECT_EQ(has_pending_source, status.has_pending_source());
  }

  int finish_fragment(PhysicalWriteFragment &fragment)
  {
    int ret = OB_SUCCESS;
    bool completed_by_me = false;
    int64_t completed_data_len = 0;
    int64_t submit_ts = OB_INVALID_TIMESTAMP;
    const FragmentRef ref = fragment.get_fragment_ref();
    if (fragment.is_ready() || fragment.is_failed())
    {
      if (OB_FAIL(fragment.mark_submitted(ref, 1 /* submit_ts */)))
      {
      }
    } else if (!fragment.is_submitted())
    {
      ret = OB_STATE_NOT_MATCH;
    }
    if (OB_SUCC(ret) && OB_FAIL(fragment.mark_io_completed(
        ref, OB_SUCCESS, 0 /* next_retry_ts */,
        common::ObTimeUtility::current_time() /* finish_ts */,
        completed_by_me, completed_data_len, submit_ts)))
    {
    } else if (OB_SUCC(ret) && !completed_by_me)
    {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int collect_fragments(const AsyncFragmentState state, ObIArray<PhysicalWriteFragment *> &fragments)
  {
    const PhysicalWriteFragmentStateFilter filter(state);
    return pool_.collect_fragments(fragments, filter);
  }

  PlannerTaskArena arena_;
  PhysicalWriteFragmentPool pool_;
  LogAsyncWritePlanner planner_;
};

// Fills only the ref queue to exercise alloc-slot-then-enqueue rollback deterministically.
class FragmentRefQueueFullGuard
{
public:
  explicit FragmentRefQueueFullGuard(LogAsyncWritePlanner &planner) : planner_(planner) {}

  ~FragmentRefQueueFullGuard()
  {
    LogAsyncWritePlanner::QueuedFragmentRef *item = NULL;
    while (OB_SUCCESS == planner_.fragment_ref_queue_.pop(item))
    {
      planner_.free_fragment_ref_item_(item);
      item = NULL;
    }
    planner_.last_fragment_ref_item_ = NULL;
  }

  int fill()
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < LogAsyncWritePlanner::FRAGMENT_REF_QUEUE_CAPACITY; ++i)
    {
      const LSN begin_lsn(i);
      const LSN end_lsn(i + 1);
      if (OB_FAIL(planner_.push_fragment_ref_(FragmentRef(0, 0), begin_lsn, end_lsn)))
      {
      }
    }
    return ret;
  }

private:
  LogAsyncWritePlanner &planner_;
};

TEST(LogAsyncWritePlannerLifecycle, RejectsOperationsBeforeInitAndAfterInvalidation)
{
  PhysicalWriteFragmentPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  LogAsyncWritePlanner planner(pool);
  bool consumed = false;
  LogIOFlushLogTask *publishable_task = NULL;

  EXPECT_EQ(OB_INVALID_ARGUMENT, planner.admit_task(NULL, consumed));
  EXPECT_EQ(OB_INVALID_ARGUMENT, planner.plan_pending_tasks());
  EXPECT_EQ(OB_INVALID_ARGUMENT, planner.advance_finished_fragment_prefix());
  EXPECT_EQ(OB_INVALID_ARGUMENT, planner.peek_publishable_task(publishable_task));

  ASSERT_EQ(OB_SUCCESS, planner.init());
  EXPECT_EQ(OB_INIT_TWICE, planner.init());
  planner.init_plan_state(LSN(4096));
  PlannerStatus status;
  planner.get_status(status);
  EXPECT_EQ(LSN(4096), status.get_planned_end_lsn());
  EXPECT_EQ(LSN(4096), status.get_persisted_lsn());

  planner.invalidate_plan_state();
  EXPECT_EQ(OB_INVALID_ARGUMENT, planner.plan_pending_tasks());
  EXPECT_EQ(OB_INVALID_ARGUMENT, planner.advance_finished_fragment_prefix());
  EXPECT_EQ(OB_INVALID_ARGUMENT, planner.peek_publishable_task(publishable_task));
  planner.init_plan_state(LSN(8192));
  EXPECT_EQ(OB_SUCCESS, planner.plan_pending_tasks());

  planner.reset();
  EXPECT_EQ(OB_SUCCESS, planner.init());
  planner.init_plan_state(LSN(0));
  planner.reset();
}

TEST_F(TestLogAsyncWritePlanner, PendingSourceRangeKeepsLsnBufferAndLengthTogether)
{
  LogAsyncWritePlanner::PendingSourceRange source;
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));

  ASSERT_EQ(OB_SUCCESS, source.init(LSN(0), buf, 1024));
  ASSERT_EQ(OB_SUCCESS, source.append(LSN(1024), buf + 1024, 2048));
  EXPECT_EQ(LSN(0), source.get_begin_lsn());
  EXPECT_EQ(LSN(3072), source.get_end_lsn());
  EXPECT_EQ(buf, source.get_buf());
  EXPECT_EQ(3072, source.get_len());

  ASSERT_EQ(OB_SUCCESS, source.consume(1024));
  EXPECT_EQ(LSN(1024), source.get_begin_lsn());
  EXPECT_EQ(LSN(3072), source.get_end_lsn());
  EXPECT_EQ(buf + 1024, source.get_buf());
  EXPECT_EQ(2048, source.get_len());
  ASSERT_EQ(OB_SUCCESS, source.consume(2048));
  EXPECT_TRUE(source.is_empty());
}

TEST_F(TestLogAsyncWritePlanner, PendingSourceRangeRejectsInvalidMutation)
{
  LogAsyncWritePlanner::PendingSourceRange source;
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  char *other_buf = arena_.alloc_buffer(LSN(1024), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_TRUE(OB_NOT_NULL(other_buf));

  EXPECT_EQ(OB_INVALID_ARGUMENT, source.init(LSN(), buf, 1024));
  EXPECT_EQ(OB_INVALID_ARGUMENT, source.init(LSN(0), NULL, 1024));
  EXPECT_EQ(OB_INVALID_ARGUMENT, source.init(LSN(0), buf, 0));
  ASSERT_EQ(OB_SUCCESS, source.init(LSN(0), buf, 2048));
  EXPECT_EQ(OB_INIT_TWICE, source.init(LSN(2048), buf + 2048, 1024));
  EXPECT_EQ(OB_ERR_UNEXPECTED, source.append(LSN(1024), buf + 2048, 1024));
  EXPECT_EQ(OB_ERR_UNEXPECTED, source.append(LSN(2048), other_buf, 1024));
  EXPECT_EQ(OB_ERR_UNEXPECTED, source.consume(0));
  EXPECT_EQ(OB_ERR_UNEXPECTED, source.consume(2049));
  EXPECT_EQ(LSN(0), source.get_begin_lsn());
  EXPECT_EQ(buf, source.get_buf());
  EXPECT_EQ(2048, source.get_len());
}

TEST_F(TestLogAsyncWritePlanner, AdmissionRejectsInvalidTaskWithoutChangingState)
{
  bool consumed = true;
  LogIOFlushMetaTask barrier_task(1 /* palf_id */, 0 /* palf_epoch */);
  char *buf = arena_.alloc_buffer(LSN(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *gap_task =
      arena_.make_one_buffer_task(1 /* log_id */, LSN(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_TRUE(OB_NOT_NULL(gap_task));

  EXPECT_EQ(OB_INVALID_ARGUMENT, planner_.admit_task(NULL, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(OB_INVALID_ARGUMENT, planner_.admit_task(&barrier_task, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(OB_ERR_UNEXPECTED, planner_.admit_task(gap_task, consumed));
  EXPECT_FALSE(consumed);
  expect_status(LSN(0), LSN(0), 0, 0, false);
  EXPECT_TRUE(planner_.pending_sources_[0].is_empty());
  EXPECT_TRUE(planner_.pending_sources_[1].is_empty());
}

TEST_F(TestLogAsyncWritePlanner, AdmissionRejectsGapAndOverlapWithoutChangingAcceptedPrefix)
{
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  char *other_buf = arena_.alloc_buffer(LSN(1024), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *accepted_task = arena_.make_one_buffer_task(1, LSN(0), 2048, buf);
  LogIOFlushLogTask *gap_task = arena_.make_one_buffer_task(2, LSN(3072), 512, other_buf + 2048);
  LogIOFlushLogTask *overlap_task = arena_.make_one_buffer_task(3, LSN(1024), 512, other_buf);
  ASSERT_TRUE(OB_NOT_NULL(accepted_task));
  ASSERT_TRUE(OB_NOT_NULL(gap_task));
  ASSERT_TRUE(OB_NOT_NULL(overlap_task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(accepted_task, consumed));
  ASSERT_TRUE(consumed);

  EXPECT_EQ(OB_ERR_UNEXPECTED, planner_.admit_task(gap_task, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(OB_ERR_UNEXPECTED, planner_.admit_task(overlap_task, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(LSN(2048), planner_.queue_end_lsn_);
  EXPECT_EQ(LSN(0), planner_.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf, planner_.pending_sources_[0].get_buf());
  EXPECT_EQ(2048, planner_.pending_sources_[0].get_len());
  EXPECT_TRUE(planner_.pending_sources_[1].is_empty());
  EXPECT_EQ(1, planner_.pending_task_queue_.get_total());
}

TEST_F(TestLogAsyncWritePlanner, AdmissionMergesOnlyContinuousMemory)
{
  char *continuous_buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  char *wrapped_buf = arena_.alloc_buffer(LSN(LOG_DIO_ALIGN_SIZE), 1024);
  ASSERT_TRUE(OB_NOT_NULL(continuous_buf));
  ASSERT_TRUE(OB_NOT_NULL(wrapped_buf));
  LogIOFlushLogTask *task0 = arena_.make_one_buffer_task(1, LSN(0), 1024, continuous_buf);
  LogIOFlushLogTask *task1 = arena_.make_one_buffer_task(2, LSN(1024), 3072, continuous_buf + 1024);
  LogIOFlushLogTask *task2 = arena_.make_one_buffer_task(3, LSN(LOG_DIO_ALIGN_SIZE), 1024, wrapped_buf);
  LogIOFlushLogTask *task3 = arena_.make_one_buffer_task(4, LSN(LOG_DIO_ALIGN_SIZE + 1024),
                                                        1024, wrapped_buf + 1024);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  ASSERT_TRUE(OB_NOT_NULL(task2));
  ASSERT_TRUE(OB_NOT_NULL(task3));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task0, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task1, consumed));
  ASSERT_TRUE(consumed);
  EXPECT_EQ(4096, planner_.pending_sources_[0].get_len());
  EXPECT_TRUE(planner_.pending_sources_[1].is_empty());

  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task2, consumed));
  ASSERT_TRUE(consumed);
  EXPECT_EQ(4096, planner_.pending_sources_[0].get_len());
  EXPECT_EQ(1024, planner_.pending_sources_[1].get_len());
  EXPECT_EQ(wrapped_buf, planner_.pending_sources_[1].get_buf());
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task3, consumed));
  ASSERT_TRUE(consumed);
  EXPECT_EQ(2048, planner_.pending_sources_[1].get_len());
  expect_status(LSN(0), LSN(0), 4, 0, true);
}

TEST_F(TestLogAsyncWritePlanner, AdmissionRejectsThirdSourceAndRollsBack)
{
  char *buf0 = arena_.alloc_buffer(LSN(0), 1024);
  char *buf1 = arena_.alloc_buffer(LSN(1024), 1024);
  char *buf2 = arena_.alloc_buffer(LSN(2048), 1024);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  ASSERT_TRUE(OB_NOT_NULL(buf2));
  LogIOFlushLogTask *task0 = arena_.make_one_buffer_task(1, LSN(0), 1024, buf0);
  LogIOFlushLogTask *task1 = arena_.make_one_buffer_task(2, LSN(1024), 1024, buf1);
  LogIOFlushLogTask *task2 = arena_.make_one_buffer_task(3, LSN(2048), 1024, buf2);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  ASSERT_TRUE(OB_NOT_NULL(task2));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task0, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task1, consumed));
  const LogAsyncWritePlanner::PendingSourceRange source0 = planner_.pending_sources_[0];
  const LogAsyncWritePlanner::PendingSourceRange source1 = planner_.pending_sources_[1];
  EXPECT_EQ(OB_ERR_UNEXPECTED, planner_.admit_task(task2, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(LSN(2048), planner_.queue_end_lsn_);
  EXPECT_EQ(source0.get_begin_lsn(), planner_.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(source0.get_buf(), planner_.pending_sources_[0].get_buf());
  EXPECT_EQ(source0.get_len(), planner_.pending_sources_[0].get_len());
  EXPECT_EQ(source1.get_begin_lsn(), planner_.pending_sources_[1].get_begin_lsn());
  EXPECT_EQ(source1.get_buf(), planner_.pending_sources_[1].get_buf());
  EXPECT_EQ(source1.get_len(), planner_.pending_sources_[1].get_len());
  EXPECT_EQ(2, planner_.pending_task_queue_.get_total());
}

TEST_F(TestLogAsyncWritePlanner, AdmissionHandlesTwoBuffersFromOneTask)
{
  char *buf0 = arena_.alloc_buffer(LSN(0), 2048);
  char *buf1 = arena_.alloc_buffer(LSN(2048), 2048);
  ASSERT_TRUE(OB_NOT_NULL(buf0));
  ASSERT_TRUE(OB_NOT_NULL(buf1));
  LogIOFlushLogTask *task = arena_.make_task(1 /* log_id */, LSN(0), LOG_DIO_ALIGN_SIZE, buf0, 2048, buf1, 2048);
  ASSERT_TRUE(OB_NOT_NULL(task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  EXPECT_TRUE(consumed);
  EXPECT_EQ(LSN(0), planner_.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf0, planner_.pending_sources_[0].get_buf());
  EXPECT_EQ(2048, planner_.pending_sources_[0].get_len());
  EXPECT_EQ(LSN(2048), planner_.pending_sources_[1].get_begin_lsn());
  EXPECT_EQ(buf1, planner_.pending_sources_[1].get_buf());
  EXPECT_EQ(2048, planner_.pending_sources_[1].get_len());
}

TEST_F(TestLogAsyncWritePlanner, AdmissionRollsBackLengthMismatchAndFullQueue)
{
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  LogIOFlushLogTask *mismatched_task =
      arena_.make_task(1 /* log_id */, LSN(0), 2048 /* logical_len */, buf, 1024, NULL, 0);
  LogIOFlushLogTask *valid_task = arena_.make_one_buffer_task(2 /* log_id */, LSN(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(mismatched_task));
  ASSERT_TRUE(OB_NOT_NULL(valid_task));
  bool consumed = false;

  EXPECT_EQ(OB_ERR_UNEXPECTED, planner_.admit_task(mismatched_task, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(LSN(0), planner_.queue_end_lsn_);
  EXPECT_TRUE(planner_.pending_sources_[0].is_empty());

  // Inject queue occupancy directly; the case verifies admission rollback, not task ownership.
  for (int64_t i = 0; i < LogAsyncWritePlanner::TASK_QUEUE_CAPACITY; ++i)
  {
    LogIOFlushLogTask *queued_task = valid_task;
    ASSERT_EQ(OB_SUCCESS, planner_.pending_task_queue_.push(queued_task));
  }
  EXPECT_EQ(OB_SIZE_OVERFLOW, planner_.admit_task(valid_task, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(LSN(0), planner_.queue_end_lsn_);
  EXPECT_TRUE(planner_.pending_sources_[0].is_empty());
  EXPECT_TRUE(planner_.pending_sources_[1].is_empty());
}

TEST_F(TestLogAsyncWritePlanner, AdmissionLeavesNextBlockTaskForCaller)
{
  const LSN block_tail(PALF_BLOCK_SIZE - LOG_DIO_ALIGN_SIZE);
  planner_.init_plan_state(block_tail);
  char *tail_buf = arena_.alloc_buffer(block_tail, LOG_DIO_ALIGN_SIZE);
  char *next_buf = arena_.alloc_buffer(LSN(PALF_BLOCK_SIZE), LOG_DIO_ALIGN_SIZE);
  ASSERT_TRUE(OB_NOT_NULL(tail_buf));
  ASSERT_TRUE(OB_NOT_NULL(next_buf));
  LogIOFlushLogTask *tail_task = arena_.make_one_buffer_task(1, block_tail, LOG_DIO_ALIGN_SIZE, tail_buf);
  LogIOFlushLogTask *next_task = arena_.make_one_buffer_task(2, LSN(PALF_BLOCK_SIZE), LOG_DIO_ALIGN_SIZE, next_buf);
  ASSERT_TRUE(OB_NOT_NULL(tail_task));
  ASSERT_TRUE(OB_NOT_NULL(next_task));

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(tail_task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(next_task, consumed));
  EXPECT_FALSE(consumed);
  EXPECT_EQ(1, planner_.pending_task_queue_.get_total());
  EXPECT_EQ(LSN(PALF_BLOCK_SIZE), planner_.queue_end_lsn_);
}

struct PlannerSplitCase
{
  PlannerSplitCase(const offset_t begin_lsn_value, const int64_t data_len_value)
      : begin_lsn(begin_lsn_value), data_len(data_len_value)
  {}
  offset_t begin_lsn;
  int64_t data_len;
};

class TestLogAsyncWritePlannerSplit : public TestLogAsyncWritePlanner,
                                      public testing::WithParamInterface<PlannerSplitCase>
{
};

TEST_P(TestLogAsyncWritePlannerSplit, SplitsAtAlignmentAndFragmentSizeBoundaries)
{
  const PlannerSplitCase test_case = GetParam();
  const LSN begin_lsn(test_case.begin_lsn);
  char *buf = arena_.alloc_buffer(begin_lsn, test_case.data_len);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1 /* log_id */, begin_lsn, test_case.data_len, buf);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_TRUE(OB_NOT_NULL(task));
  planner_.init_plan_state(begin_lsn);

  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_TRUE(consumed);
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());

  const int64_t first_prefix = begin_lsn.val_ % LOG_DIO_ALIGN_SIZE;
  const int64_t first_capacity = NORMAL_FRAGMENT_MAX_SIZE - first_prefix;
  const int64_t remaining_after_first = test_case.data_len > first_capacity
      ? test_case.data_len - first_capacity : 0;
  const int64_t expected_fragment_count =
      1 + (remaining_after_first + NORMAL_FRAGMENT_MAX_SIZE - 1) / NORMAL_FRAGMENT_MAX_SIZE;
  expect_status(begin_lsn + static_cast<offset_t>(test_case.data_len), begin_lsn,
                1, expected_fragment_count, false);
  EXPECT_EQ(expected_fragment_count, planner_.fragment_ref_queue_.get_total());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(expected_fragment_count, ready_fragments.count());
  LSN logical_cursor = begin_lsn;
  int64_t source_offset = 0;
  int64_t remaining_len = test_case.data_len;
  for (int64_t i = 0; i < ready_fragments.count(); ++i)
  {
    PhysicalWriteFragment *fragment = ready_fragments.at(i);
    ASSERT_TRUE(OB_NOT_NULL(fragment));
    const int64_t prefix = logical_cursor.val_ % LOG_DIO_ALIGN_SIZE;
    const int64_t planned_len = MIN(remaining_len, NORMAL_FRAGMENT_MAX_SIZE - prefix);
    const LSN fragment_begin(logical_cursor.val_ - prefix);
    int64_t data_len = 0;
    ASSERT_EQ(OB_SUCCESS, fragment->get_data_len(data_len));
    EXPECT_EQ(fragment_begin, fragment->get_begin_lsn());
    EXPECT_EQ(logical_cursor + static_cast<offset_t>(planned_len), fragment->get_end_lsn());
    EXPECT_EQ(buf + source_offset - prefix, fragment->get_buf());
    EXPECT_EQ(prefix + planned_len, data_len);
    EXPECT_EQ(0, fragment->get_begin_lsn().val_ % LOG_DIO_ALIGN_SIZE);
    EXPECT_EQ(0, reinterpret_cast<uintptr_t>(fragment->get_buf()) % LOG_DIO_ALIGN_SIZE);
    EXPECT_LE(data_len, fragment->get_fragment_max_size());
    logical_cursor = logical_cursor + static_cast<offset_t>(planned_len);
    source_offset += planned_len;
    remaining_len -= planned_len;
  }
  EXPECT_EQ(0, remaining_len);
  EXPECT_EQ(begin_lsn + static_cast<offset_t>(test_case.data_len), logical_cursor);
}

INSTANTIATE_TEST_CASE_P(
    AlignmentAndLengthBoundaries, TestLogAsyncWritePlannerSplit,
    testing::Values(PlannerSplitCase(0, 1), PlannerSplitCase(0, LOG_DIO_ALIGN_SIZE - 1),
                    PlannerSplitCase(0, LOG_DIO_ALIGN_SIZE), PlannerSplitCase(0, LOG_DIO_ALIGN_SIZE + 1),
                    PlannerSplitCase(0, NORMAL_FRAGMENT_MAX_SIZE - 1),
                    PlannerSplitCase(0, NORMAL_FRAGMENT_MAX_SIZE),
                    PlannerSplitCase(0, NORMAL_FRAGMENT_MAX_SIZE + 1), PlannerSplitCase(1, 1),
                    PlannerSplitCase(1, NORMAL_FRAGMENT_MAX_SIZE),
                    PlannerSplitCase(LOG_DIO_ALIGN_SIZE - 1, 1),
                    PlannerSplitCase(LOG_DIO_ALIGN_SIZE - 1, NORMAL_FRAGMENT_MAX_SIZE + 1)));

TEST_F(TestLogAsyncWritePlanner, PoolFullKeepsExactUnplannedSuffixForRetry)
{
  FragmentRef reserved_refs[FRAGMENT_SLOT_CNT_PER_PALF - 1];
  int64_t planned_len = 0;
  for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF - 1; ++i)
  {
    const LSN reserved_lsn(PALF_BLOCK_SIZE + i * LOG_DIO_ALIGN_SIZE);
    char *reserved_buf = arena_.alloc_buffer(reserved_lsn, LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(reserved_buf));
    ASSERT_EQ(OB_SUCCESS, pool_.alloc_slot(reserved_lsn, reserved_buf, LOG_DIO_ALIGN_SIZE,
                                           NORMAL_FRAGMENT_MAX_SIZE,
                                           FragmentRef(), reserved_refs[i], planned_len));
  }
  const int64_t task_len = NORMAL_FRAGMENT_MAX_SIZE + LOG_DIO_ALIGN_SIZE;
  char *buf = arena_.alloc_buffer(LSN(0), task_len);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1, LSN(0), task_len, buf);
  ASSERT_TRUE(OB_NOT_NULL(buf));
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  EXPECT_EQ(LSN(NORMAL_FRAGMENT_MAX_SIZE), planner_.get_planned_end_lsn_());
  EXPECT_EQ(LSN(NORMAL_FRAGMENT_MAX_SIZE), planner_.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf + NORMAL_FRAGMENT_MAX_SIZE, planner_.pending_sources_[0].get_buf());
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, planner_.pending_sources_[0].get_len());
  EXPECT_EQ(1, planner_.fragment_ref_queue_.get_total());

  ASSERT_EQ(OB_SUCCESS, pool_.free_slot(reserved_refs[0]));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  EXPECT_EQ(LSN(task_len), planner_.get_planned_end_lsn_());
  EXPECT_TRUE(planner_.pending_sources_[0].is_empty());
  EXPECT_EQ(2, planner_.fragment_ref_queue_.get_total());
}

TEST_F(TestLogAsyncWritePlanner, PartialSecondSourceMovesToPlanningHead)
{
  static const int64_t RESERVED_COUNT = FRAGMENT_SLOT_CNT_PER_PALF - 2;
  FragmentRef reserved_refs[RESERVED_COUNT];
  int64_t planned_len = 0;
  for (int64_t i = 0; i < RESERVED_COUNT; ++i)
  {
    const LSN reserved_lsn(PALF_BLOCK_SIZE + i * LOG_DIO_ALIGN_SIZE);
    char *reserved_buf = arena_.alloc_buffer(reserved_lsn, LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(reserved_buf));
    ASSERT_EQ(OB_SUCCESS, pool_.alloc_slot(reserved_lsn, reserved_buf, LOG_DIO_ALIGN_SIZE,
                                          NORMAL_FRAGMENT_MAX_SIZE, FragmentRef(), reserved_refs[i], planned_len));
  }
  char *first_buf = arena_.alloc_buffer(LSN(0), NORMAL_FRAGMENT_MAX_SIZE);
  char *second_buf = arena_.alloc_buffer(
      LSN(NORMAL_FRAGMENT_MAX_SIZE), NORMAL_FRAGMENT_MAX_SIZE + LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *first_task =
      arena_.make_one_buffer_task(1, LSN(0), NORMAL_FRAGMENT_MAX_SIZE, first_buf);
  LogIOFlushLogTask *second_task = arena_.make_one_buffer_task(
      2, LSN(NORMAL_FRAGMENT_MAX_SIZE), NORMAL_FRAGMENT_MAX_SIZE + LOG_DIO_ALIGN_SIZE, second_buf);
  ASSERT_TRUE(OB_NOT_NULL(first_task));
  ASSERT_TRUE(OB_NOT_NULL(second_task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(first_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(second_task, consumed));

  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  EXPECT_EQ(LSN(2 * NORMAL_FRAGMENT_MAX_SIZE), planner_.get_planned_end_lsn_());
  EXPECT_EQ(LSN(2 * NORMAL_FRAGMENT_MAX_SIZE), planner_.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(second_buf + NORMAL_FRAGMENT_MAX_SIZE, planner_.pending_sources_[0].get_buf());
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, planner_.pending_sources_[0].get_len());
  EXPECT_TRUE(planner_.pending_sources_[1].is_empty());

  ASSERT_EQ(OB_SUCCESS, pool_.free_slot(reserved_refs[0]));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  EXPECT_EQ(LSN(2 * NORMAL_FRAGMENT_MAX_SIZE + LOG_DIO_ALIGN_SIZE), planner_.get_planned_end_lsn_());
  EXPECT_TRUE(planner_.pending_sources_[0].is_empty());
}

TEST_F(TestLogAsyncWritePlanner, InitiallyFullPoolMakesNoProgressAndCanRetry)
{
  FragmentRef reserved_refs[FRAGMENT_SLOT_CNT_PER_PALF];
  int64_t planned_len = 0;
  for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF; ++i)
  {
    const LSN reserved_lsn(PALF_BLOCK_SIZE + i * LOG_DIO_ALIGN_SIZE);
    char *reserved_buf = arena_.alloc_buffer(reserved_lsn, LOG_DIO_ALIGN_SIZE);
    ASSERT_TRUE(OB_NOT_NULL(reserved_buf));
    ASSERT_EQ(OB_SUCCESS, pool_.alloc_slot(reserved_lsn, reserved_buf, LOG_DIO_ALIGN_SIZE,
                                           NORMAL_FRAGMENT_MAX_SIZE,
                                           FragmentRef(), reserved_refs[i], planned_len));
  }
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1, LSN(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_TRUE(consumed);

  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  EXPECT_EQ(LSN(0), planner_.get_planned_end_lsn_());
  EXPECT_EQ(LSN(0), planner_.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf, planner_.pending_sources_[0].get_buf());
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, planner_.pending_sources_[0].get_len());
  EXPECT_EQ(0, planner_.fragment_ref_queue_.get_total());

  ASSERT_EQ(OB_SUCCESS, pool_.free_slot(reserved_refs[0]));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  EXPECT_EQ(LSN(LOG_DIO_ALIGN_SIZE), planner_.get_planned_end_lsn_());
  EXPECT_TRUE(planner_.pending_sources_[0].is_empty());
  EXPECT_EQ(1, planner_.fragment_ref_queue_.get_total());
}

TEST_F(TestLogAsyncWritePlanner, FullFragmentRefQueueRollsBackSlotAndCursor)
{
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1, LSN(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_TRUE(consumed);

  {
    FragmentRefQueueFullGuard guard(planner_);
    ASSERT_EQ(OB_SUCCESS, guard.fill());
    EXPECT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
    EXPECT_EQ(0, pool_.get_used_slot_count());
    EXPECT_EQ(LogAsyncWritePlanner::FRAGMENT_REF_QUEUE_CAPACITY, planner_.fragment_ref_queue_.get_total());
    EXPECT_EQ(LSN(0), planner_.get_planned_end_lsn_());
    EXPECT_EQ(LSN(0), planner_.pending_sources_[0].get_begin_lsn());
    EXPECT_EQ(buf, planner_.pending_sources_[0].get_buf());
    EXPECT_EQ(LOG_DIO_ALIGN_SIZE, planner_.pending_sources_[0].get_len());
  }

  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  EXPECT_EQ(1, pool_.get_used_slot_count());
  EXPECT_EQ(1, planner_.fragment_ref_queue_.get_total());
  EXPECT_EQ(LSN(LOG_DIO_ALIGN_SIZE), planner_.get_planned_end_lsn_());
}

TEST_F(TestLogAsyncWritePlanner, WaitParentDependsOnUnfinishedTailPage)
{
  char *parent_buf = arena_.alloc_buffer(LSN(0), 2048);
  char *child_buf = arena_.alloc_buffer(LSN(2048), 1024);
  LogIOFlushLogTask *parent_task = arena_.make_one_buffer_task(1, LSN(0), 2048, parent_buf);
  LogIOFlushLogTask *child_task = arena_.make_one_buffer_task(2, LSN(2048), 1024, child_buf);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));
  bool consumed = false;

  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(parent_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  const FragmentRef parent_ref = ready_fragments.at(0)->get_fragment_ref();

  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(child_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  ASSERT_EQ(OB_SUCCESS, collect_fragments(AsyncFragmentState::WAIT_PARENT, wait_fragments));
  ASSERT_EQ(1, wait_fragments.count());
  EXPECT_TRUE(wait_fragments.at(0)->get_parent_ref().is_equal(parent_ref));
  EXPECT_EQ(LSN(0), wait_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(LSN(3072), wait_fragments.at(0)->get_end_lsn());
  EXPECT_EQ(WAIT_PARENT_FRAGMENT_MAX_SIZE, wait_fragments.at(0)->get_fragment_max_size());
}

TEST_F(TestLogAsyncWritePlanner, WaitParentIsNotNeededAtAlignedBoundary)
{
  char *buf0 = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  char *buf1 = arena_.alloc_buffer(LSN(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task0 = arena_.make_one_buffer_task(1, LSN(0), LOG_DIO_ALIGN_SIZE, buf0);
  LogIOFlushLogTask *task1 = arena_.make_one_buffer_task(2, LSN(LOG_DIO_ALIGN_SIZE), LOG_DIO_ALIGN_SIZE, buf1);
  ASSERT_TRUE(OB_NOT_NULL(task0));
  ASSERT_TRUE(OB_NOT_NULL(task1));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task0, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task1, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(OB_SUCCESS, collect_fragments(AsyncFragmentState::WAIT_PARENT, wait_fragments));
  EXPECT_EQ(2, ready_fragments.count());
  EXPECT_EQ(0, wait_fragments.count());
}

TEST_F(TestLogAsyncWritePlanner, WaitParentIsNotNeededAfterParentFinishes)
{
  char *parent_buf = arena_.alloc_buffer(LSN(0), 2048);
  char *child_buf = arena_.alloc_buffer(LSN(2048), 1024);
  LogIOFlushLogTask *parent_task = arena_.make_one_buffer_task(1, LSN(0), 2048, parent_buf);
  LogIOFlushLogTask *child_task = arena_.make_one_buffer_task(2, LSN(2048), 1024, child_buf);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(parent_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_EQ(OB_SUCCESS, finish_fragment(*ready_fragments.at(0)));

  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(child_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ready_fragments.reset();
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(OB_SUCCESS, collect_fragments(AsyncFragmentState::WAIT_PARENT, wait_fragments));
  EXPECT_EQ(1, ready_fragments.count());
  EXPECT_EQ(0, wait_fragments.count());
}

TEST_F(TestLogAsyncWritePlanner, WaitParentContinuousSourceExtendsOneFragmentAndCachedRange)
{
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *parent_task = arena_.make_one_buffer_task(1, LSN(0), 2048, buf);
  LogIOFlushLogTask *child0 = arena_.make_one_buffer_task(2, LSN(2048), 1024, buf + 2048);
  LogIOFlushLogTask *child1 = arena_.make_one_buffer_task(3, LSN(3072), 1024, buf + 3072);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child0));
  ASSERT_TRUE(OB_NOT_NULL(child1));
  bool consumed = false;

  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(parent_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_EQ(OB_SUCCESS,
            ready_fragments.at(0)->mark_submitted(ready_fragments.at(0)->get_fragment_ref(), 1 /* submit_ts */));

  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(child0, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(child1, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  ASSERT_EQ(OB_SUCCESS, collect_fragments(AsyncFragmentState::WAIT_PARENT, wait_fragments));
  ASSERT_EQ(1, wait_fragments.count());
  EXPECT_EQ(LSN(LOG_DIO_ALIGN_SIZE), wait_fragments.at(0)->get_end_lsn());
  EXPECT_EQ(2, planner_.fragment_ref_queue_.get_total());
  ASSERT_TRUE(OB_NOT_NULL(planner_.last_fragment_ref_item_));
  EXPECT_EQ(LSN(LOG_DIO_ALIGN_SIZE), planner_.last_fragment_ref_item_->end_lsn_);
}

TEST_F(TestLogAsyncWritePlanner, WaitParentUsesParentTailPageForLongFragment)
{
  const offset_t parent_begin = 2 * LOG_DIO_ALIGN_SIZE;
  const offset_t parent_end = 198 * 1024;
  const int64_t parent_tail_prefix = parent_end % LOG_DIO_ALIGN_SIZE;
  const int64_t wait_parent_data_len = WAIT_PARENT_FRAGMENT_MAX_SIZE - parent_tail_prefix;
  const int64_t child_len = wait_parent_data_len + 2 * LOG_DIO_ALIGN_SIZE;
  const offset_t wait_parent_begin = parent_end - parent_tail_prefix;
  const offset_t wait_parent_end = parent_end + wait_parent_data_len;
  const offset_t child_end = parent_end + child_len;
  planner_.init_plan_state(LSN(parent_begin));
  char *parent_buf = arena_.alloc_buffer(LSN(parent_begin), parent_end - parent_begin);
  char *child_buf = arena_.alloc_buffer(LSN(parent_end), child_len);
  LogIOFlushLogTask *parent_task =
      arena_.make_one_buffer_task(1, LSN(parent_begin), parent_end - parent_begin, parent_buf);
  LogIOFlushLogTask *child_task = arena_.make_one_buffer_task(2, LSN(parent_end), child_len, child_buf);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(parent_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_EQ(OB_SUCCESS,
            ready_fragments.at(0)->mark_submitted(ready_fragments.at(0)->get_fragment_ref(), 1 /* submit_ts */));

  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(child_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  ASSERT_EQ(OB_SUCCESS, collect_fragments(AsyncFragmentState::WAIT_PARENT, wait_fragments));
  ASSERT_EQ(1, wait_fragments.count());
  EXPECT_EQ(LSN(wait_parent_begin), wait_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(LSN(wait_parent_end), wait_fragments.at(0)->get_end_lsn());
  EXPECT_EQ(WAIT_PARENT_FRAGMENT_MAX_SIZE, wait_fragments.at(0)->get_fragment_max_size());
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(LSN(wait_parent_end), ready_fragments.at(0)->get_begin_lsn());
  EXPECT_EQ(LSN(child_end), ready_fragments.at(0)->get_end_lsn());
  EXPECT_TRUE(planner_.pending_sources_[0].is_empty());
  EXPECT_TRUE(planner_.pending_sources_[1].is_empty());
  expect_status(LSN(child_end), LSN(parent_begin), 2, 3, false);
}

TEST_F(TestLogAsyncWritePlanner, FailedParentKeepsChildWaitingUntilParentFinishes)
{
  char *parent_buf = arena_.alloc_buffer(LSN(0), 2048);
  char *child_buf = arena_.alloc_buffer(LSN(2048), 1024);
  LogIOFlushLogTask *parent_task = arena_.make_one_buffer_task(1, LSN(0), 2048, parent_buf);
  LogIOFlushLogTask *child_task = arena_.make_one_buffer_task(2, LSN(2048), 1024, child_buf);
  ASSERT_TRUE(OB_NOT_NULL(parent_task));
  ASSERT_TRUE(OB_NOT_NULL(child_task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(parent_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  PhysicalWriteFragment *parent = ready_fragments.at(0);
  const FragmentRef parent_ref = parent->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, parent->mark_submitted(parent_ref, 1 /* submit_ts */));
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(child_task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> wait_fragments;
  ASSERT_EQ(OB_SUCCESS, collect_fragments(AsyncFragmentState::WAIT_PARENT, wait_fragments));
  ASSERT_EQ(1, wait_fragments.count());

  ASSERT_EQ(OB_SUCCESS, parent->mark_failed(parent_ref, OB_IO_ERROR, 0 /* next_retry_ts */));
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  EXPECT_EQ(0, ready_fragments.count());
  EXPECT_TRUE(wait_fragments.at(0)->is_wait_parent());

  ASSERT_EQ(OB_SUCCESS, finish_fragment(*parent));
  ready_fragments.reset();
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  EXPECT_EQ(wait_fragments.at(0), ready_fragments.at(0));
}

TEST_F(TestLogAsyncWritePlanner, PersistedPrefixWaitsForOutOfOrderCompletion)
{
  PhysicalWriteFragment *fragments[3] = {NULL, NULL, NULL};
  for (int64_t i = 0; i < 3; ++i)
  {
    const LSN begin_lsn(i * LOG_DIO_ALIGN_SIZE);
    char *buf = arena_.alloc_buffer(begin_lsn, LOG_DIO_ALIGN_SIZE);
    LogIOFlushLogTask *task = arena_.make_one_buffer_task(i + 1, begin_lsn, LOG_DIO_ALIGN_SIZE, buf);
    ASSERT_TRUE(OB_NOT_NULL(task));
    bool consumed = false;
    ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
    ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  }
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(3, ready_fragments.count());
  for (int64_t i = 0; i < ready_fragments.count(); ++i)
  {
    fragments[i] = ready_fragments.at(i);
  }

  ASSERT_EQ(OB_SUCCESS, finish_fragment(*fragments[2]));
  ASSERT_EQ(OB_SUCCESS, finish_fragment(*fragments[1]));
  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  expect_status(LSN(3 * LOG_DIO_ALIGN_SIZE), LSN(0), 3, 3, false);

  ASSERT_EQ(OB_SUCCESS, finish_fragment(*fragments[0]));
  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  expect_status(LSN(3 * LOG_DIO_ALIGN_SIZE), LSN(3 * LOG_DIO_ALIGN_SIZE), 3, 3, false);
}

TEST_F(TestLogAsyncWritePlanner, PersistedPrefixAdvancesOnlyFinishedHead)
{
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1, LSN(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *fragment = ready_fragments.at(0);
  const FragmentRef ref = fragment->get_fragment_ref();

  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  expect_status(LSN(LOG_DIO_ALIGN_SIZE), LSN(0), 1, 1, false);
  ASSERT_EQ(OB_SUCCESS, fragment->mark_submitted(ref, 1 /* submit_ts */));
  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  expect_status(LSN(LOG_DIO_ALIGN_SIZE), LSN(0), 1, 1, false);
  ASSERT_EQ(OB_SUCCESS, fragment->mark_failed(ref, OB_IO_ERROR, 0 /* next_retry_ts */));
  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  expect_status(LSN(LOG_DIO_ALIGN_SIZE), LSN(0), 1, 1, false);
  ASSERT_EQ(OB_SUCCESS, finish_fragment(*fragment));
  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  expect_status(LSN(LOG_DIO_ALIGN_SIZE), LSN(LOG_DIO_ALIGN_SIZE), 1, 1, false);
}

TEST_F(TestLogAsyncWritePlanner, PersistedPrefixUsesCachedRangeAfterSlotReuse)
{
  char *buf = arena_.alloc_buffer(LSN(0), 2 * LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1, LSN(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  PhysicalWriteFragment *old_fragment = ready_fragments.at(0);
  const FragmentRef old_ref = old_fragment->get_fragment_ref();
  ASSERT_EQ(OB_SUCCESS, finish_fragment(*old_fragment));
  ASSERT_EQ(OB_SUCCESS, pool_.free_slot(old_ref));

  FragmentRef new_ref;
  int64_t planned_len = 0;
  ASSERT_EQ(OB_SUCCESS, pool_.alloc_slot(LSN(LOG_DIO_ALIGN_SIZE), buf + LOG_DIO_ALIGN_SIZE,
                                         LOG_DIO_ALIGN_SIZE, NORMAL_FRAGMENT_MAX_SIZE,
                                         FragmentRef(), new_ref, planned_len));
  ASSERT_EQ(old_ref.slot_id, new_ref.slot_id);
  ASSERT_NE(old_ref.generation, new_ref.generation);
  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  expect_status(LSN(LOG_DIO_ALIGN_SIZE), LSN(LOG_DIO_ALIGN_SIZE), 1, 1, false);
}

TEST_F(TestLogAsyncWritePlanner, PersistedPrefixRejectsNonContinuousCachedRange)
{
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1, LSN(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_EQ(OB_SUCCESS, finish_fragment(*ready_fragments.at(0)));
  LogAsyncWritePlanner::QueuedFragmentRef *queued_ref = NULL;
  ASSERT_EQ(OB_SUCCESS, planner_.fragment_ref_queue_.head_unsafe(queued_ref));
  ASSERT_TRUE(OB_NOT_NULL(queued_ref));
  queued_ref->begin_lsn_ = LSN(1);

  EXPECT_EQ(OB_ERR_UNEXPECTED, planner_.advance_finished_fragment_prefix());
  EXPECT_EQ(1, planner_.fragment_ref_queue_.get_total());
  expect_status(LSN(LOG_DIO_ALIGN_SIZE), LSN(0), 1, 1, false);
}

TEST_F(TestLogAsyncWritePlanner, PublishFollowsPersistedTaskPrefixInAdmissionOrder)
{
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *tasks[3] = {arena_.make_one_buffer_task(1, LSN(0), 1024, buf),
                                 arena_.make_one_buffer_task(2, LSN(1024), 1024, buf + 1024),
                                 arena_.make_one_buffer_task(3, LSN(2048), 2048, buf + 2048)};
  bool consumed = false;
  for (int64_t i = 0; i < 3; ++i)
  {
    ASSERT_TRUE(OB_NOT_NULL(tasks[i]));
    ASSERT_EQ(OB_SUCCESS, planner_.admit_task(tasks[i], consumed));
    ASSERT_TRUE(consumed);
  }
  ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks());
  LogIOFlushLogTask *publishable_task = NULL;
  EXPECT_EQ(OB_ITER_END, planner_.peek_publishable_task(publishable_task));

  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
  ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
  ASSERT_EQ(1, ready_fragments.count());
  ASSERT_EQ(OB_SUCCESS, finish_fragment(*ready_fragments.at(0)));
  ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
  for (int64_t i = 0; i < 3; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, planner_.peek_publishable_task(publishable_task));
    EXPECT_EQ(tasks[i], publishable_task);
    planner_.pop_published_task();
  }
  EXPECT_EQ(OB_ITER_END, planner_.peek_publishable_task(publishable_task));
}

TEST_F(TestLogAsyncWritePlanner, TailResetUsesMockOnlyForNonAlignedPrefix)
{
  MockAsyncPalfHandleImpl handle;
  PlannerStatus status;
  static const offset_t PREFIX_LENGTHS[] = {1, 123, LOG_DIO_ALIGN_SIZE - 1};
  ASSERT_EQ(OB_SUCCESS, planner_.reset_after_tail_changed(&handle, LSN(0)));
  EXPECT_EQ(0, handle.get_tail_page_read_cnt());
  ASSERT_EQ(OB_SUCCESS, planner_.reset_after_tail_changed(&handle, LSN(LOG_DIO_ALIGN_SIZE)));
  EXPECT_EQ(0, handle.get_tail_page_read_cnt());

  handle.set_tail_page_read_size(LOG_DIO_ALIGN_SIZE);
  for (int64_t i = 0; i < ARRAYSIZEOF(PREFIX_LENGTHS); ++i)
  {
    const LSN tail_lsn(LOG_DIO_ALIGN_SIZE + PREFIX_LENGTHS[i]);
    ASSERT_EQ(OB_SUCCESS, planner_.reset_after_tail_changed(&handle, tail_lsn));
    EXPECT_EQ(i + 1, handle.get_tail_page_read_cnt());
    EXPECT_EQ(i + 1, handle.get_tail_prefix_fill_cnt());
    EXPECT_EQ(PREFIX_LENGTHS[i], handle.get_tail_prefix_fill_size());
    planner_.get_status(status);
    EXPECT_EQ(tail_lsn, status.get_planned_end_lsn());
    EXPECT_EQ(tail_lsn, status.get_persisted_lsn());
  }
}

TEST_F(TestLogAsyncWritePlanner, TailResetFailureInvalidatesStateAndCanRetry)
{
  MockAsyncPalfHandleImpl handle;
  const LSN tail_lsn(LOG_DIO_ALIGN_SIZE + 123);
  PlannerStatus status;
  handle.set_carry_read_ret(OB_IO_ERROR);
  EXPECT_EQ(OB_IO_ERROR, planner_.reset_after_tail_changed(&handle, tail_lsn));
  planner_.get_status(status);
  EXPECT_FALSE(status.get_planned_end_lsn().is_valid());
  EXPECT_FALSE(status.get_persisted_lsn().is_valid());

  handle.set_carry_read_ret(OB_SUCCESS);
  handle.set_tail_page_read_size(122);
  EXPECT_EQ(OB_ERR_UNEXPECTED, planner_.reset_after_tail_changed(&handle, tail_lsn));
  planner_.get_status(status);
  EXPECT_FALSE(status.get_planned_end_lsn().is_valid());

  handle.set_tail_page_read_size(LOG_DIO_ALIGN_SIZE);
  handle.set_tail_prefix_fill_ret(OB_IO_ERROR);
  EXPECT_EQ(OB_IO_ERROR, planner_.reset_after_tail_changed(&handle, tail_lsn));
  handle.set_tail_prefix_fill_ret(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, planner_.reset_after_tail_changed(&handle, tail_lsn));
  planner_.get_status(status);
  EXPECT_EQ(tail_lsn, status.get_planned_end_lsn());
  EXPECT_EQ(tail_lsn, status.get_persisted_lsn());
}

TEST_F(TestLogAsyncWritePlanner, TailResetRejectsOutstandingPlannerWorkWithoutMutation)
{
  MockAsyncPalfHandleImpl handle;
  char *buf = arena_.alloc_buffer(LSN(0), LOG_DIO_ALIGN_SIZE);
  LogIOFlushLogTask *task = arena_.make_one_buffer_task(1, LSN(0), LOG_DIO_ALIGN_SIZE, buf);
  ASSERT_TRUE(OB_NOT_NULL(task));
  bool consumed = false;
  ASSERT_EQ(OB_SUCCESS, planner_.admit_task(task, consumed));
  ASSERT_TRUE(consumed);

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            planner_.reset_after_tail_changed(&handle, LSN(LOG_DIO_ALIGN_SIZE + 1)));
  EXPECT_EQ(0, handle.get_tail_page_read_cnt());
  expect_status(LSN(0), LSN(0), 1, 0, true);
  EXPECT_EQ(LSN(LOG_DIO_ALIGN_SIZE), planner_.queue_end_lsn_);
  EXPECT_EQ(LSN(0), planner_.pending_sources_[0].get_begin_lsn());
  EXPECT_EQ(buf, planner_.pending_sources_[0].get_buf());
  EXPECT_EQ(LOG_DIO_ALIGN_SIZE, planner_.pending_sources_[0].get_len());
}

TEST_F(TestLogAsyncWritePlanner, DeterministicSequencePreservesPlanningPersistenceAndPublishOrder)
{
  static const int64_t TASK_COUNT = 12;
  static const int64_t TASK_LENGTHS[TASK_COUNT] = {1, 1023, 3072, 4096, 4097, 8191, 512, 3584, 16384, 4095, 1, 12288};
  LogIOFlushLogTask *tasks[TASK_COUNT];
  LSN task_begin(0);
  bool consumed = false;
  for (int64_t i = 0; i < TASK_COUNT; ++i)
  {
    const int64_t len = TASK_LENGTHS[i];
    if (i % 3 == 2 && len > 1)
    {
      const int64_t first_len = len / 2;
      char *buf0 = arena_.alloc_buffer(task_begin, first_len);
      char *buf1 = arena_.alloc_buffer(task_begin + static_cast<offset_t>(first_len), len - first_len);
      tasks[i] = arena_.make_task(i + 1, task_begin, len, buf0, first_len, buf1, len - first_len);
    } else
    {
      char *buf = arena_.alloc_buffer(task_begin, len);
      tasks[i] = arena_.make_one_buffer_task(i + 1, task_begin, len, buf);
    }
    ASSERT_TRUE(OB_NOT_NULL(tasks[i])) << "task_index=" << i;
    ASSERT_EQ(OB_SUCCESS, planner_.admit_task(tasks[i], consumed)) << "task_index=" << i;
    ASSERT_TRUE(consumed) << "task_index=" << i;
    ASSERT_EQ(OB_SUCCESS, planner_.plan_pending_tasks()) << "task_index=" << i;
    EXPECT_TRUE(planner_.is_valid_()) << "task_index=" << i;
    task_begin = task_begin + static_cast<offset_t>(len);
  }
  const LSN expected_end = task_begin;
  PlannerStatus status;
  planner_.get_status(status);
  EXPECT_EQ(expected_end, status.get_planned_end_lsn());
  EXPECT_EQ(TASK_COUNT, status.get_pending_task_count());
  EXPECT_FALSE(status.has_pending_source());

  LSN previous_persisted(0);
  bool all_finished = false;
  for (int64_t round = 0; round < FRAGMENT_SLOT_CNT_PER_PALF && !all_finished; ++round)
  {
    ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> ready_fragments;
    ASSERT_EQ(OB_SUCCESS, pool_.collect_ready_fragments(ready_fragments, 0 /* aio_delay_us */, NULL, NULL, NULL));
    for (int64_t i = ready_fragments.count() - 1; i >= 0; --i)
    {
      ASSERT_TRUE(OB_NOT_NULL(ready_fragments.at(i)));
      ASSERT_EQ(OB_SUCCESS, finish_fragment(*ready_fragments.at(i)));
    }
    ASSERT_EQ(OB_SUCCESS, pool_.free_all_finished_fragments(0 /* aio_delay_us */, NULL));
    ASSERT_EQ(OB_SUCCESS, planner_.advance_finished_fragment_prefix());
    planner_.get_status(status);
    EXPECT_LE(previous_persisted, status.get_persisted_lsn());
    EXPECT_LE(status.get_persisted_lsn(), status.get_planned_end_lsn());
    previous_persisted = status.get_persisted_lsn();
    all_finished = (0 == status.get_active_fragment_count());
  }
  ASSERT_TRUE(all_finished);
  EXPECT_EQ(expected_end, previous_persisted);

  LogIOFlushLogTask *publishable_task = NULL;
  for (int64_t i = 0; i < TASK_COUNT; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, planner_.peek_publishable_task(publishable_task));
    EXPECT_EQ(tasks[i], publishable_task) << "task_index=" << i;
    planner_.pop_published_task();
  }
  EXPECT_EQ(OB_ITER_END, planner_.peek_publishable_task(publishable_task));
  expect_status(expected_end, expected_end, 0, 0, false);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(oceanbase::unittest::TEST_TENANT_ID);
  oceanbase::share::ObTenantBase tenant_base(oceanbase::unittest::TEST_TENANT_ID);
  oceanbase::share::ObTenantEnv::set_tenant(&tenant_base);
  oceanbase::common::ObLogger::get_logger().set_file_name("test_log_async_write_planner.log", true);
  testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  oceanbase::ObMallocAllocator::get_instance()->recycle_tenant_allocator(oceanbase::unittest::TEST_TENANT_ID);
  return ret;
}

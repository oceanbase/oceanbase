/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "rootserver/mview/ob_mview_pending_task_queue.h"
#include "rootserver/mview/ob_mview_pending_task_define.h"

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const uint64_t TENANT_ID  = OB_SYS_TENANT_ID; // sys tenant always has an allocator in unit tests
static const int64_t  REFRESH_ID = 100L;

// Build a basic task. dep_ids / dep_cnt are the prerequisite mview IDs that
// must be SUCCESS before this task is eligible for dispatch.
static ObMViewPendingTask make_task(uint64_t mview_id,
                                    int64_t  flags = 0,
                                    uint64_t *dep_ids = nullptr,
                                    int64_t  dep_cnt = 0);

// Build a task with an explicit status (for reload-from-DB scenarios).
static ObMViewPendingTask make_task_with_status(uint64_t mview_id,
                                                ObMViewTaskStatus status,
                                                int64_t  flags = 0,
                                                uint64_t *dep_ids = nullptr,
                                                int64_t  dep_cnt = 0)
{
  ObMViewPendingTask t = make_task(mview_id, flags, dep_ids, dep_cnt);
  t.status_ = status;
  return t;
}

static ObMViewPendingTask make_task(uint64_t mview_id,
                                    int64_t  flags,
                                    uint64_t *dep_ids,
                                    int64_t  dep_cnt)
{
  ObMViewPendingTask t;
  t.tenant_id_            = TENANT_ID;
  t.refresh_id_           = REFRESH_ID;
  t.mview_id_             = mview_id;
  t.target_data_sync_scn_ = 0;
  t.status_               = MV_TASK_PENDING;
  t.skip_cnt_             = 0;
  t.retry_count_          = 0;
  t.next_retry_ts_        = 0;
  t.flags_                = flags;
  t.dep_mview_id_cnt_     = dep_cnt;
  t.dep_mview_ids_        = dep_ids;
  t.gmt_create_           = ObTimeUtility::current_time();
  t.gmt_modified_         = ObTimeUtility::current_time();
  return t;
}

static int push_one(ObMViewPendingTaskQueue &q, ObMViewPendingTask &t)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMViewPendingTask *, 8> arr;
  if (OB_FAIL(arr.push_back(&t))) {
    LOG_WARN("push back failed", K(ret));
  } else {
    ret = q.push_tasks(arr);
  }
  return ret;
}

template <typename... Tasks>
static int push_many(ObMViewPendingTaskQueue &q, Tasks&... tasks)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMViewPendingTask *, 8> arr;
  ObMViewPendingTask* ptrs[] = {&tasks...};
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(sizeof...(tasks)); ++i) {
    if (OB_FAIL(arr.push_back(ptrs[i]))) {
      LOG_WARN("push back failed", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    ret = q.push_tasks(arr);
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class ObMViewPendingTaskQueueTest : public ::testing::Test
{
public:
  static constexpr uint64_t LEAF_MV = 1001UL; // base mview, no deps
  static constexpr uint64_t MID_MV  = 1002UL; // depends on LEAF_MV
  static constexpr uint64_t ROOT_MV = 1003UL; // root mview (is_root_task), depends on MID_MV

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, queue_.init(TENANT_ID));
  }

  void TearDown() override
  {
    // destroy is idempotent for the purposes of cleanup
    queue_.destroy();
  }

  // Fetch a copy of the refresh ctx for inspection inside tests.
  ObMViewPendingRefreshCtx get_ctx(int64_t refresh_id = REFRESH_ID)
  {
    ObMViewPendingRefreshCtx ctx;
    EXPECT_EQ(OB_SUCCESS,
              queue_.get_refresh_ctx_for_test(TENANT_ID, refresh_id, ctx));
    return ctx;
  }

  // Transition helper: push a task as RUNNING state by first pushing then
  // calling set_task_running. Returns OB_SUCCESS if both succeed.
  int push_and_run(uint64_t mview_id, int64_t flags = 0,
                   uint64_t *dep_ids = nullptr, int64_t dep_cnt = 0)
  {
    int ret = OB_SUCCESS;
    ObMViewPendingTask t = make_task(mview_id, flags, dep_ids, dep_cnt);
    if (OB_FAIL(push_one(queue_, t))) {
    } else if (OB_FAIL(queue_.set_task_running(TENANT_ID, REFRESH_ID, mview_id))) {
    }
    return ret;
  }

  ObMViewPendingTaskQueue queue_;
};

// ---------------------------------------------------------------------------
// 1. Lifecycle
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, InitDestroyIdempotent)
{
  // SetUp already called init. init a second time must fail.
  EXPECT_EQ(OB_INIT_TWICE, queue_.init(TENANT_ID));

  // destroy then destroy again – second call should be harmless
  EXPECT_EQ(OB_SUCCESS, queue_.destroy());
  EXPECT_EQ(OB_SUCCESS, queue_.destroy());
}

// ---------------------------------------------------------------------------
// 2. Push / Peek
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, PeekEmptyQueueReturnsEAGAIN)
{
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));
}

TEST_F(ObMViewPendingTaskQueueTest, PushAndPeekSingleTask)
{
  ObMViewPendingTask t = make_task(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  ObMViewPendingTask out;
  ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
  EXPECT_EQ(TENANT_ID,  out.tenant_id_);
  EXPECT_EQ(REFRESH_ID, out.refresh_id_);
  EXPECT_EQ(LEAF_MV,    out.mview_id_);
}

TEST_F(ObMViewPendingTaskQueueTest, PeekSkipsNonPendingTasks)
{
  // Push one task and put it into RUNNING state – it should no longer be peekable.
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG));

  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));
}

TEST_F(ObMViewPendingTaskQueueTest, GetRunningJobInfosOnlyReturnsRunningTasks)
{
  static const uint64_t RETRY_MV = 1101UL;
  static const uint64_t SUCCESS_MV = 1102UL;
  static const uint64_t PENDING_MV = 1103UL;
  const uint64_t target_scn = 123456UL;
  const int64_t refresh_parallel = 8;
  const int64_t create_ts = 1000000L;
  bool finished = false;
  ObMViewPendingTask running_task = make_task(LEAF_MV);
  ObMViewPendingTask retry_task = make_task(RETRY_MV);
  ObMViewPendingTask success_task = make_task(SUCCESS_MV);
  ObMViewPendingTask pending_task = make_task(PENDING_MV, ObMViewPendingTask::ROOT_TASK_FLAG);
  running_task.target_data_sync_scn_ = target_scn;
  running_task.refresh_method_ = share::schema::ObMVRefreshMethod::FAST;
  running_task.refresh_parallel_ = refresh_parallel;
  running_task.gmt_create_ = create_ts;
  running_task.gmt_modified_ = create_ts;
  ASSERT_EQ(OB_SUCCESS,
            push_many(queue_, running_task, retry_task, success_task, pending_task));
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, RETRY_MV));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, RETRY_MV, finished));
  ASSERT_FALSE(finished);
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, SUCCESS_MV));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, SUCCESS_MV, finished));
  ASSERT_FALSE(finished);

  ObSEArray<ObMViewPendingRunningJobInfo, 4> running_job_infos;
  ASSERT_EQ(OB_SUCCESS, queue_.collect_running_jobs(running_job_infos));
  ASSERT_EQ(1, running_job_infos.count());
  EXPECT_EQ(TENANT_ID, running_job_infos.at(0).tenant_id_);
  EXPECT_EQ(REFRESH_ID, running_job_infos.at(0).refresh_id_);
  EXPECT_EQ(LEAF_MV, running_job_infos.at(0).mview_id_);
  EXPECT_EQ(target_scn, running_job_infos.at(0).target_data_sync_scn_);
  EXPECT_EQ(share::schema::ObMVRefreshMethod::FAST,
            running_job_infos.at(0).refresh_method_);
  EXPECT_EQ(refresh_parallel, running_job_infos.at(0).refresh_parallel_);
  EXPECT_EQ(create_ts, running_job_infos.at(0).gmt_create_);
  EXPECT_LT(create_ts, running_job_infos.at(0).gmt_modified_);
}

// ---------------------------------------------------------------------------
// 3. Refresh status
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RefreshStatusPendingAfterPush)
{
  ObMViewPendingTask t = make_task(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
}

TEST_F(ObMViewPendingTaskQueueTest, RefreshStatusRunningWhenTaskRunning)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG));

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
}

TEST_F(ObMViewPendingTaskQueueTest, RefreshStatusSuccessAfterSingleTaskSucceeds)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG));

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_TRUE(finished);

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_SUCCESS, status);
}

// ---------------------------------------------------------------------------
// 4. Dependency blocking in peek
// ---------------------------------------------------------------------------

// Topology: LEAF (no deps, non-root) --> ROOT (depends on LEAF, is_root_task)
// Only LEAF should be peekable until LEAF is SUCCESS.
TEST_F(ObMViewPendingTaskQueueTest, DependencyBlocksPeek)
{
  // Root depends on LEAF
  uint64_t root_deps[] = { LEAF_MV };
  ObMViewPendingTask leaf = make_task(LEAF_MV);
  ObMViewPendingTask root = make_task(ROOT_MV, ObMViewPendingTask::ROOT_TASK_FLAG,
                                      root_deps, 1);

  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, root));

  // peek must return LEAF (its deps are empty, so it is ready)
  ObMViewPendingTask out;
  ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
  EXPECT_EQ(LEAF_MV, out.mview_id_);

  // Move LEAF to RUNNING; root is still not peekable (dep not yet SUCCESS)
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));

  // LEAF succeeds; now ROOT should become peekable
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished); // root still pending

  ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
  EXPECT_EQ(ROOT_MV, out.mview_id_);
}

// ---------------------------------------------------------------------------
// 5. Task failure cascades pending tasks
// ---------------------------------------------------------------------------

// Topology (deepest dep first):
//   LEAF (no deps) -> MID (depends on LEAF) -> ROOT (depends on MID, root)
// When LEAF fails (after running), MID and ROOT must be cancelled, and
// refresh_finished must be true.
TEST_F(ObMViewPendingTaskQueueTest, FailureCascadesCancelsDependents)
{
  uint64_t mid_deps[]  = { LEAF_MV };
  uint64_t root_deps[] = { MID_MV  };

  ObMViewPendingTask leaf = make_task(LEAF_MV);
  ObMViewPendingTask mid  = make_task(MID_MV,  0,
                                     mid_deps, 1);
  ObMViewPendingTask root = make_task(ROOT_MV,
                                     ObMViewPendingTask::ROOT_TASK_FLAG,
                                     root_deps, 1);

  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, mid, root));

  // Run LEAF then fail it — mid and root are still PENDING so refresh is not done yet
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_failed(TENANT_ID, REFRESH_ID, LEAF_MV, OB_SUCCESS, finished));
  EXPECT_FALSE(finished);

  // Cascade-cancel the remaining PENDING tasks (MID, ROOT)
  ASSERT_EQ(OB_SUCCESS,
            queue_.cancel_all_pending_tasks(TENANT_ID, REFRESH_ID, finished));
  // All three tasks are now terminal (LEAF=failed, MID=cancelled, ROOT=cancelled)
  EXPECT_TRUE(finished);

  // Overall refresh status reflects failure
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_FAILED, status);

  // No more tasks are dispatchable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));
}

// ---------------------------------------------------------------------------
// 6. Retry-wait: task goes back to PENDING-equivalent (running_cnt drops)
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RetryWaitDecrementsRunningCount)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG));

  // Status is RUNNING (running_task_cnt_ == 1)
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  // running_task_cnt_ is now 0, unfinished_task_cnt_ still 1 -> PENDING
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
}

// ---------------------------------------------------------------------------
// 7. set_task_cancelled (external cancel while running)
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, CancelRunningTask)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG));

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_cancelled(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_TRUE(finished);

  // Refresh is done; no terminal failure, but root did not succeed either.
  // get_refresh_status: unfinished==0, running==0, has_terminal_failure==false
  // -> reports SUCCESS (root_task_succeeded_ is a separate flag but status uses
  //    has_terminal_failure to distinguish)
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  // CANCELLED tasks do not set has_terminal_failure_, so status appears SUCCESS.
  // This is the existing contract; verify it doesn't change unexpectedly.
  EXPECT_EQ(MV_TASK_SUCCESS, status);
}

// ---------------------------------------------------------------------------
// 8. recycle_refresh cleans up all tasks
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RecycleRefreshCleansUp)
{
  uint64_t root_deps[] = { LEAF_MV };
  ObMViewPendingTask leaf_task = make_task(LEAF_MV);
  ObMViewPendingTask root_task = make_task(ROOT_MV,
                                           ObMViewPendingTask::ROOT_TASK_FLAG,
                                           root_deps, 1);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, root_task));

  // Finish the refresh (run LEAF -> success, run ROOT -> success)
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  bool fin = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, fin));
  EXPECT_FALSE(fin);
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, ROOT_MV));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, ROOT_MV, fin));
  EXPECT_TRUE(fin);

  // recycle
  ASSERT_EQ(OB_SUCCESS, queue_.recycle_refresh(TENANT_ID, REFRESH_ID));

  // After recycling, the refresh context is gone; get_refresh_status fails
  ObMViewTaskStatus status;
  EXPECT_NE(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));

  // Queue is now empty
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));
}

// ---------------------------------------------------------------------------
// 9. Two independent refreshes do not interfere
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, MultipleRefreshesAreIndependent)
{
  const int64_t REFRESH_A = 200L;
  const int64_t REFRESH_B = 201L;

  ObMViewPendingTask ta;
  ta.tenant_id_        = TENANT_ID;
  ta.refresh_id_       = REFRESH_A;
  ta.mview_id_         = LEAF_MV;
  ta.status_           = MV_TASK_PENDING;
  ta.flags_            = ObMViewPendingTask::ROOT_TASK_FLAG;
  ta.dep_mview_id_cnt_ = 0;
  ta.dep_mview_ids_    = nullptr;
  ta.gmt_create_       = ObTimeUtility::current_time();
  ta.gmt_modified_     = ObTimeUtility::current_time();
  ta.target_data_sync_scn_ = 0; ta.skip_cnt_ = 0;
  ta.retry_count_ = 0; ta.next_retry_ts_ = 0;

  ObMViewPendingTask tb = ta;
  tb.refresh_id_ = REFRESH_B;

  ASSERT_EQ(OB_SUCCESS, push_one(queue_, ta));
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, tb));

  // Mark REFRESH_A's task as running
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_A, LEAF_MV));

  ObMViewTaskStatus sa, sb;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_A, sa));
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_B, sb));
  EXPECT_EQ(MV_TASK_RUNNING, sa);
  EXPECT_EQ(MV_TASK_PENDING,  sb); // B is unaffected
}

// ---------------------------------------------------------------------------
// 10. Key hashing and equality
// ---------------------------------------------------------------------------

TEST(ObMViewPendingTaskKeyTest, HashAndEquality)
{
  ObMViewPendingTaskKey k1(1001, 100, 2001);
  ObMViewPendingTaskKey k2(1001, 100, 2001);
  ObMViewPendingTaskKey k3(1001, 100, 2002);

  EXPECT_TRUE(k1 == k2);
  EXPECT_FALSE(k1 == k3);
  EXPECT_EQ(k1.hash(), k2.hash());
  // Different mview_id should (almost certainly) produce different hash
  EXPECT_NE(k1.hash(), k3.hash());
  EXPECT_TRUE(k1.is_valid());
}

TEST(ObMViewRefreshKeyTest, HashAndEquality)
{
  ObMViewRefreshKey r1(1001, 100);
  ObMViewRefreshKey r2(1001, 100);
  ObMViewRefreshKey r3(1001, 101);

  EXPECT_TRUE(r1 == r2);
  EXPECT_FALSE(r1 == r3);
  EXPECT_EQ(r1.hash(), r2.hash());
  EXPECT_NE(r1.hash(), r3.hash());
  EXPECT_TRUE(r1.is_valid());
}

// ---------------------------------------------------------------------------
// 11. ObMViewPendingRefreshCtx::is_task_finished
// ---------------------------------------------------------------------------

TEST(ObMViewPendingRefreshCtxTest, IsTaskFinished)
{
  ObMViewPendingRefreshCtx ctx;
  ctx.tenant_id_         = 1001;
  ctx.refresh_id_        = 1;
  ctx.root_mview_id_     = OB_INVALID_ID; // not yet set
  ctx.unfinished_task_cnt_ = 1;

  EXPECT_FALSE(ctx.is_task_finished()); // root_mview_id_ invalid

  ctx.root_mview_id_ = 2001;
  EXPECT_FALSE(ctx.is_task_finished()); // unfinished_task_cnt_ > 0

  ctx.unfinished_task_cnt_ = 0;
  EXPECT_TRUE(ctx.is_task_finished());
}

// ---------------------------------------------------------------------------
// 12. Concurrent push from multiple threads (different refresh_ids)
//
// Each thread pushes tasks for a different refresh_id.
// All pushes must succeed and every task must be visible via peek.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ConcurrentPushDifferentRefreshes)
{
  static const int      THREAD_COUNT    = 8;
  static const uint64_t BASE_MV_ID      = 3000UL;
  static const int64_t  BASE_REFRESH_ID = 500L;

  // Concurrent pushes — each thread creates its own refresh context.
  std::vector<std::thread> threads;
  std::vector<int> results(THREAD_COUNT, OB_SUCCESS);
  for (int i = 0; i < THREAD_COUNT; ++i) {
    threads.emplace_back([this, i, &results]() {
      ObMViewPendingTask t;
      t.tenant_id_            = TENANT_ID;
      t.refresh_id_           = BASE_REFRESH_ID + i;
      t.mview_id_             = BASE_MV_ID + static_cast<uint64_t>(i);
      t.target_data_sync_scn_ = 0;
      t.status_               = MV_TASK_PENDING;
      t.skip_cnt_             = 0;
      t.retry_count_          = 0;
      t.next_retry_ts_        = 0;
      t.flags_                = ObMViewPendingTask::ROOT_TASK_FLAG;
      t.dep_mview_id_cnt_     = 0;
      t.dep_mview_ids_        = nullptr;
      t.gmt_create_           = ObTimeUtility::current_time();
      t.gmt_modified_         = ObTimeUtility::current_time();
      results[i] = push_one(queue_, t);
    });
  }
  for (auto &th : threads) {
    th.join();
  }

  for (int i = 0; i < THREAD_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS, results[i]) << "thread " << i << " push failed";
  }

  // Drain: mark every pending task running then success, count total.
  int consumed = 0;
  ObMViewPendingTask out;
  while (OB_SUCCESS == queue_.peek_task(out)) {
    ASSERT_EQ(OB_SUCCESS,
              queue_.set_task_running(out.tenant_id_, out.refresh_id_, out.mview_id_));
    bool fin = false;
    ASSERT_EQ(OB_SUCCESS,
              queue_.set_task_success(out.tenant_id_, out.refresh_id_, out.mview_id_, fin));
    ++consumed;
  }
  EXPECT_EQ(THREAD_COUNT, consumed);
}

// ---------------------------------------------------------------------------
// 13. Concurrent push (producers) and peek (consumer)
//
// A single consumer thread polls peek_task and marks each task running then
// success.  THREAD_COUNT producer threads each push one task.  After all
// producers finish and the consumer drains the queue, every task must have
// been consumed exactly once.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ConcurrentPushAndPeek)
{
  static const int      THREAD_COUNT    = 8;
  static const uint64_t BASE_MV_ID      = 4000UL;
  static const int64_t  BASE_REFRESH_ID = 600L;

  std::atomic<int> consumed{0};
  std::atomic<bool> producers_done{false};

  // Consumer: spins until producers are done and queue is empty.
  std::thread consumer([&]() {
    while (true) {
      ObMViewPendingTask out;
      int ret = queue_.peek_task(out);
      if (OB_SUCCESS == ret) {
        if (OB_SUCCESS == queue_.set_task_running(
                              out.tenant_id_, out.refresh_id_, out.mview_id_)) {
          bool fin = false;
          if (OB_SUCCESS == queue_.set_task_success(
                                out.tenant_id_, out.refresh_id_, out.mview_id_, fin)) {
            ++consumed;
          }
        }
      } else if (producers_done.load()) {
        // No more tasks and all producers finished — we are done.
        break;
      }
    }
  });

  // Producers: each thread pushes one task with its own refresh_id.
  std::vector<std::thread> producers;
  for (int i = 0; i < THREAD_COUNT; ++i) {
    producers.emplace_back([&, i]() {
      ObMViewPendingTask t;
      t.tenant_id_            = TENANT_ID;
      t.refresh_id_           = BASE_REFRESH_ID + i;
      t.mview_id_             = BASE_MV_ID + static_cast<uint64_t>(i);
      t.status_               = MV_TASK_PENDING;
      t.flags_                = ObMViewPendingTask::ROOT_TASK_FLAG;
      t.dep_mview_id_cnt_     = 0;
      t.dep_mview_ids_        = nullptr;
      t.target_data_sync_scn_ = 0;
      t.skip_cnt_             = 0;
      t.retry_count_          = 0;
      t.next_retry_ts_        = 0;
      t.gmt_create_           = ObTimeUtility::current_time();
      t.gmt_modified_         = ObTimeUtility::current_time();
      EXPECT_EQ(OB_SUCCESS, push_one(queue_, t));
    });
  }
  for (auto &p : producers) {
    p.join();
  }
  producers_done.store(true);
  consumer.join();

  EXPECT_EQ(THREAD_COUNT, consumed.load());
}

// ---------------------------------------------------------------------------
// 17. clear() empties the queue and removes all refresh contexts
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ClearEmptiesQueueAndContexts)
{
  uint64_t root_deps[] = { LEAF_MV };
  ObMViewPendingTask leaf_task = make_task(LEAF_MV);
  ObMViewPendingTask root_task = make_task(ROOT_MV,
                                           ObMViewPendingTask::ROOT_TASK_FLAG,
                                           root_deps, 1);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, root_task));

  ASSERT_EQ(OB_SUCCESS, queue_.clear());

  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));

  ObMViewTaskStatus status;
  EXPECT_NE(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
}

// ---------------------------------------------------------------------------
// 18. Pushing a task with the same (tenant, refresh, mview) key twice fails
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, DuplicatePushFails)
{
  ObMViewPendingTask t = make_task(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));
  // ObHashMap::set_refactored returns OB_HASH_EXIST for duplicate keys
  EXPECT_NE(OB_SUCCESS, push_one(queue_, t));
}

// ---------------------------------------------------------------------------
// 19. set_task_running on an already-RUNNING task returns OB_EAGAIN
//     (from_status=PENDING does not match current=RUNNING)
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, SetRunningOnAlreadyRunningReturnsEAGAIN)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG));
  EXPECT_EQ(OB_EAGAIN,
            queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
}

// ---------------------------------------------------------------------------
// 20. set_task_success from PENDING (not RUNNING) returns OB_EAGAIN
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, SuccessFromNonRunningStateReturnsEAGAIN)
{
  ObMViewPendingTask leaf_task = make_task(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, leaf_task));
  bool finished = false;
  // Task is PENDING; set_task_success expects from_status=RUNNING
  EXPECT_EQ(OB_EAGAIN,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
}

// ---------------------------------------------------------------------------
// 21. get_refresh_status for a non-existent refresh_id returns OB_ENTRY_NOT_EXIST
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, GetRefreshStatusNonExistentRefresh)
{
  ObMViewTaskStatus status;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            queue_.get_refresh_status(TENANT_ID, 9999L, status));
}

// ---------------------------------------------------------------------------
// 22. RETRY_WAIT task is NOT visible via peek_task (status != PENDING)
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RetryWaitTaskIsNotPeekable)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, ObMViewPendingTask::ROOT_TASK_FLAG));

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  // RETRY_WAIT is not PENDING -> peek must skip it
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));

  // task still exists in the queue with RETRY_WAIT status
  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_RETRY_WAIT, status);
}

// ---------------------------------------------------------------------------
// 23. Partial success (leaf done, root still pending) does not finish the refresh
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, PartialSuccessRefreshStillPending)
{
  uint64_t root_deps[] = { LEAF_MV };
  ObMViewPendingTask leaf_task = make_task(LEAF_MV);
  ObMViewPendingTask root_task = make_task(ROOT_MV,
                                           ObMViewPendingTask::ROOT_TASK_FLAG,
                                           root_deps, 1);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, root_task));

  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished); // ROOT still unfinished

  // running_cnt==0, unfinished_cnt==1 -> PENDING
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
}

// ---------------------------------------------------------------------------
// 24. Diamond dependency: A -> {B, C} -> D(root)
//
// After A succeeds both B and C become dispatchable independently.
// D is unblocked only after both B and C succeed.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, DiamondDependency)
{
  static constexpr uint64_t A = 2001, B = 2002, C = 2003, D = 2004;
  uint64_t b_deps[] = { A };
  uint64_t c_deps[] = { A };
  uint64_t d_deps[] = { B, C };

  ObMViewPendingTask task_a = make_task(A);
  ObMViewPendingTask task_b = make_task(B, 0, b_deps, 1);
  ObMViewPendingTask task_c = make_task(C, 0, c_deps, 1);
  ObMViewPendingTask task_d = make_task(D, ObMViewPendingTask::ROOT_TASK_FLAG, d_deps, 2);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, task_a, task_b, task_c, task_d));
  // Only A is ready initially
  ObMViewPendingTask out;
  ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
  EXPECT_EQ(A, out.mview_id_);
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, A));

  // B and C are blocked while A is running
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));

  // A succeeds; B and C both become dispatchable
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_success(TENANT_ID, REFRESH_ID, A, finished));
  EXPECT_FALSE(finished);

  // Drain B and C in whatever order the queue offers them.
  // After A succeeds both B and C are independently dispatchable; they can run
  // concurrently so there is no OB_EAGAIN between dispatching them.
  for (int step = 0; step < 2; ++step) {
    ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
    EXPECT_TRUE(out.mview_id_ == B || out.mview_id_ == C)
        << "unexpected task: " << out.mview_id_;
    uint64_t mid = out.mview_id_;
    ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, mid));
    ASSERT_EQ(OB_SUCCESS,
              queue_.set_task_success(TENANT_ID, REFRESH_ID, mid, finished));
    EXPECT_FALSE(finished); // D still has at least one unsatisfied dependency
  }

  // Both B and C succeeded; D is now ready
  ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
  EXPECT_EQ(D, out.mview_id_);
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, D));
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_success(TENANT_ID, REFRESH_ID, D, finished));
  EXPECT_TRUE(finished);
}

// ---------------------------------------------------------------------------
// 25. Deep chain: five-level linear dependency A->B->C->D->E(root)
//
// At each step exactly one task is dispatchable; tasks unlock one at a time.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, DeepChainFiveLevels)
{
  static const uint64_t IDS[]  = { 3001UL, 3002UL, 3003UL, 3004UL, 3005UL };
  static const int       DEPTH = 5;

  // Dependency arrays must remain valid during push
  uint64_t dep1[] = { IDS[0] };
  uint64_t dep2[] = { IDS[1] };
  uint64_t dep3[] = { IDS[2] };
  uint64_t dep4[] = { IDS[3] };

  ObMViewPendingTask t0 = make_task(IDS[0]);
  ObMViewPendingTask t1 = make_task(IDS[1], 0, dep1, 1);
  ObMViewPendingTask t2 = make_task(IDS[2], 0, dep2, 1);
  ObMViewPendingTask t3 = make_task(IDS[3], 0, dep3, 1);
  ObMViewPendingTask t4 = make_task(IDS[4], ObMViewPendingTask::ROOT_TASK_FLAG, dep4, 1);

  ASSERT_EQ(OB_SUCCESS, push_many(queue_, t0, t1, t2, t3, t4));

  for (int i = 0; i < DEPTH; ++i) {
    ObMViewPendingTask out;
    ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
    EXPECT_EQ(IDS[i], out.mview_id_) << "wrong task at depth " << i;
    ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, IDS[i]));
    bool finished = false;
    ASSERT_EQ(OB_SUCCESS,
              queue_.set_task_success(TENANT_ID, REFRESH_ID, IDS[i], finished));
    EXPECT_EQ(i == DEPTH - 1, finished);
  }
}

// ---------------------------------------------------------------------------
// 26. Reload batch: root was RUNNING when RS crashed
//
// After reload running_task_cnt_ == 1, so get_refresh_status returns RUNNING
// and total_running_cnt_ is updated.  When the inspect-task call reports
// set_task_success, the refresh finishes.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithRunningTask)
{
  ObMViewPendingTask leaf = make_task_with_status(LEAF_MV, MV_TASK_SUCCESS);
  ObMViewPendingTask root = make_task_with_status(
      ROOT_MV, MV_TASK_RUNNING, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, root));

  // Directly verify ctx fields set by accum_ctx_stats_for_task
  {
    ObMViewPendingRefreshCtx ctx = get_ctx();
    EXPECT_EQ(1,       ctx.running_task_cnt_);
    EXPECT_EQ(1,       ctx.unfinished_task_cnt_);
    EXPECT_FALSE(ctx.has_terminal_failure_);
    EXPECT_FALSE(ctx.root_task_succeeded_);
    EXPECT_EQ(ROOT_MV, ctx.root_mview_id_);
  }

  // running_task_cnt_ == 1 -> RUNNING
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);

  // total_running_cnt_ must account for the reloaded RUNNING task
  EXPECT_EQ(1, queue_.get_total_running_cnt());

  // RUNNING task is not peekable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));

  // Inspect-task call completes the root task
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, ROOT_MV, finished));
  EXPECT_TRUE(finished);

  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_SUCCESS, status);
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

// ---------------------------------------------------------------------------
// 27. Reload batch: root was RETRY_WAIT when RS crashed
//
// unfinished_task_cnt_ == 1, running_task_cnt_ == 0 -> PENDING.
// RETRY_WAIT tasks are not peekable; set_task_pending re-enables dispatch.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithRetryWaitRoot)
{
  ObMViewPendingTask root = make_task_with_status(
      ROOT_MV, MV_TASK_RETRY_WAIT, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, root));

  // Directly verify ctx: unfinished=1, running=0 (RETRY_WAIT counts as unfinished)
  {
    ObMViewPendingRefreshCtx ctx = get_ctx();
    EXPECT_EQ(0, ctx.running_task_cnt_);
    EXPECT_EQ(1, ctx.unfinished_task_cnt_);
    EXPECT_FALSE(ctx.has_terminal_failure_);
    EXPECT_FALSE(ctx.root_task_succeeded_);
  }

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);

  // RETRY_WAIT is not peekable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));

  // set_task_pending moves it back to PENDING so it can be dispatched again
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_pending(TENANT_ID, REFRESH_ID, ROOT_MV));
  ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
  EXPECT_EQ(ROOT_MV, out.mview_id_);
}

// ---------------------------------------------------------------------------
// 28. Reload batch: all tasks already in terminal-failure state
//
// FAILED leaf + CANCELLED root -> has_terminal_failure_ set, unfinished == 0
// -> get_refresh_status returns FAILED immediately after push.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithAllTerminalFailure)
{
  ObMViewPendingTask leaf = make_task_with_status(LEAF_MV, MV_TASK_FAILED);
  ObMViewPendingTask root = make_task_with_status(
      ROOT_MV, MV_TASK_CANCELLED, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, root));

  // Directly verify ctx: FAILED sets has_terminal_failure_, CANCELLED does not
  // increment unfinished, so unfinished == 0 and running == 0
  {
    ObMViewPendingRefreshCtx ctx = get_ctx();
    EXPECT_EQ(0,       ctx.running_task_cnt_);
    EXPECT_EQ(0,       ctx.unfinished_task_cnt_);
    EXPECT_TRUE(ctx.has_terminal_failure_);
    EXPECT_FALSE(ctx.root_task_succeeded_);
    EXPECT_EQ(ROOT_MV, ctx.root_mview_id_);
  }

  // has_terminal_failure_ == true, unfinished == 0 -> FAILED
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_FAILED, status);

  // Nothing to dispatch
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

// ---------------------------------------------------------------------------
// 29. Reload batch: root already SUCCESS
//
// root_task_succeeded_ == true, unfinished == 0 -> SUCCESS immediately.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithRootAlreadySucceeded)
{
  ObMViewPendingTask root = make_task_with_status(
      ROOT_MV, MV_TASK_SUCCESS, ObMViewPendingTask::ROOT_TASK_FLAG);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, root));

  // root_task_succeeded_ must be set when root is loaded as SUCCESS
  {
    ObMViewPendingRefreshCtx ctx = get_ctx();
    EXPECT_EQ(0,       ctx.running_task_cnt_);
    EXPECT_EQ(0,       ctx.unfinished_task_cnt_);
    EXPECT_FALSE(ctx.has_terminal_failure_);
    EXPECT_TRUE(ctx.root_task_succeeded_);
    EXPECT_EQ(ROOT_MV, ctx.root_mview_id_);
  }

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_SUCCESS, status);

  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));
}

// ---------------------------------------------------------------------------
// 30. Reload mixed-status batch: leaf=SUCCESS, mid=RUNNING, root=PENDING
//
// Simulates a crash after leaf finished but while mid was still running.
// - running_cnt=1 (mid), unfinished_cnt=2 (mid+root) -> RUNNING initially
// - root is PENDING but its dep (mid) is RUNNING, not SUCCESS -> not peekable
// - After mid is marked success, root becomes peekable
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadMixedStatusBatch)
{
  uint64_t mid_deps[]  = { LEAF_MV };
  uint64_t root_deps[] = { MID_MV  };

  ObMViewPendingTask leaf = make_task_with_status(LEAF_MV, MV_TASK_SUCCESS);
  ObMViewPendingTask mid  = make_task_with_status(MID_MV,  MV_TASK_RUNNING, 0,
                                                  mid_deps, 1);
  ObMViewPendingTask root = make_task_with_status(ROOT_MV, MV_TASK_PENDING,
                                                  ObMViewPendingTask::ROOT_TASK_FLAG,
                                                  root_deps, 1);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, mid, root));

  // leaf=SUCCESS contributes nothing; mid=RUNNING counts as unfinished+running;
  // root=PENDING counts as unfinished only -> running=1, unfinished=2
  {
    ObMViewPendingRefreshCtx ctx = get_ctx();
    EXPECT_EQ(1,       ctx.running_task_cnt_);
    EXPECT_EQ(2,       ctx.unfinished_task_cnt_);
    EXPECT_FALSE(ctx.has_terminal_failure_);
    EXPECT_FALSE(ctx.root_task_succeeded_);
    EXPECT_EQ(ROOT_MV, ctx.root_mview_id_);
  }

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, queue_.get_refresh_status(TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
  EXPECT_EQ(1, queue_.get_total_running_cnt());

  // root is PENDING but mid (its dep) is not SUCCESS yet -> not peekable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, queue_.peek_task(out));

  // Inspect-task marks mid as success
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, MID_MV, finished));
  EXPECT_FALSE(finished); // root still pending

  // Now root's dep is satisfied -> peekable
  ASSERT_EQ(OB_SUCCESS, queue_.peek_task(out));
  EXPECT_EQ(ROOT_MV, out.mview_id_);

  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, ROOT_MV));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, ROOT_MV, finished));
  EXPECT_TRUE(finished);
}

// ---------------------------------------------------------------------------
// 31. Push a task with an invalid status value
//
// accum_ctx_stats_for_task hits the default branch and returns
// OB_ERR_UNEXPECTED; push_tasks must propagate that error.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, PushTaskWithInvalidStatusFails)
{
  ObMViewPendingTask root = make_task(ROOT_MV, ObMViewPendingTask::ROOT_TASK_FLAG);
  root.status_ = 99; // not a valid ObMViewTaskStatus value
  EXPECT_NE(OB_SUCCESS, push_one(queue_, root));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

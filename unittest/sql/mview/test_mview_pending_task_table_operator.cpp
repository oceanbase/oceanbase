/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "rootserver/mview/ob_mview_pending_task_table_operator.h"
#include "rootserver/mview/ob_mview_pending_task_define.h"
#include "share/mock_mysql_proxy.h"
#include "fake_mysql_result.h"

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::unittest;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

// ===========================================================================
// Table Operator Skeleton Tests  (DISABLED_ - needs a live SQL proxy)
// ===========================================================================
//
// All single-task lifecycle tests below are disabled until ObMockSQLProxy (or
// a real test-tenant connection) is wired in.  load_tasks_batch is covered by
// the LoadTasksBatch_* tests further down using a FakeMySQLResult.
// ===========================================================================

class ObMViewPendingTaskTableOperatorTest : public ::testing::Test
{
public:
  static constexpr uint64_t OP_TENANT_ID  = OB_SYS_TENANT_ID;
  static constexpr int64_t  OP_REFRESH_ID = 500L;
  static constexpr uint64_t OP_MVIEW_ID   = 5001UL;

  static ObMViewPendingTask make_db_task(uint64_t mview_id,
                                        int64_t  flags = ObMViewPendingTask::ROOT_TASK_FLAG)
  {
    ObMViewPendingTask t;
    t.tenant_id_            = OP_TENANT_ID;
    t.refresh_id_           = OP_REFRESH_ID;
    t.mview_id_             = mview_id;
    t.target_data_sync_scn_ = 0;
    t.status_               = MV_TASK_PENDING;
    t.skip_cnt_             = 0;
    t.retry_count_          = 0;
    t.next_retry_ts_        = 0;
    t.flags_                = flags;
    t.dep_mview_id_cnt_     = 0;
    t.dep_mview_ids_        = nullptr;
    t.gmt_create_           = ObTimeUtility::current_time();
    t.gmt_modified_         = ObTimeUtility::current_time();
    return t;
  }

  void SetUp() override
  {
    // TODO: provide a real SQL proxy and call op_.init(&sql_proxy_)
    // e.g.:
    //   ASSERT_EQ(OB_SUCCESS, op_.init(&sql_proxy_));
  }

  void TearDown() override
  {
    op_.destroy();
  }

  // common::ObMockSQLProxy sql_proxy_;  // TODO: instantiate a real test proxy
  ObMViewPendingTaskTableOperator op_;
};

// ---------------------------------------------------------------------------
// 1. DISABLED: init / destroy lifecycle
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorInitDestroy)
{
  // ASSERT_EQ(OB_SUCCESS, op_.init(&sql_proxy_));
  // op_.destroy();
  // op_.destroy(); // second destroy must be a safe no-op
}

// ---------------------------------------------------------------------------
// 2. DISABLED: insert_tasks then load_task round-trips all fields
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorInsertAndLoadTask)
{
  // ObMViewPendingTask task = make_db_task(OP_MVIEW_ID);
  // ObSEArray<ObMViewPendingTask *, 4> task_ptrs;
  // ASSERT_EQ(OB_SUCCESS, task_ptrs.push_back(&task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(task_ptrs));

  // ObMViewPendingTask loaded;
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID, loaded));
  // EXPECT_EQ(OP_TENANT_ID,  loaded.tenant_id_);
  // EXPECT_EQ(OP_REFRESH_ID, loaded.refresh_id_);
  // EXPECT_EQ(OP_MVIEW_ID,   loaded.mview_id_);
  // EXPECT_EQ(MV_TASK_PENDING, loaded.status_);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 3. DISABLED: update_task_to_running persists RUNNING status
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorUpdateTaskToRunning)
{
  // ObMViewPendingTask task = make_db_task(OP_MVIEW_ID);
  // ObSEArray<ObMViewPendingTask *, 4> task_ptrs;
  // ASSERT_EQ(OB_SUCCESS, task_ptrs.push_back(&task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(task_ptrs));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_running(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));

  // ObMViewPendingTask loaded;
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID, loaded));
  // EXPECT_EQ(MV_TASK_RUNNING, loaded.status_);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 4. DISABLED: update_task_to_success persists SUCCESS status
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorUpdateTaskToSuccess)
{
  // ObMViewPendingTask task = make_db_task(OP_MVIEW_ID);
  // ObSEArray<ObMViewPendingTask *, 4> task_ptrs;
  // ASSERT_EQ(OB_SUCCESS, task_ptrs.push_back(&task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(task_ptrs));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_running(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_success(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));

  // ObMViewPendingTask loaded;
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID, loaded));
  // EXPECT_EQ(MV_TASK_SUCCESS, loaded.status_);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 5. DISABLED: update_task_to_failed persists FAILED status
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorUpdateTaskToFailed)
{
  // ObMViewPendingTask task = make_db_task(OP_MVIEW_ID);
  // ObSEArray<ObMViewPendingTask *, 4> task_ptrs;
  // ASSERT_EQ(OB_SUCCESS, task_ptrs.push_back(&task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(task_ptrs));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_running(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_failed(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));

  // ObMViewPendingTask loaded;
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID, loaded));
  // EXPECT_EQ(MV_TASK_FAILED, loaded.status_);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 6. DISABLED: update_task_to_cancelled persists CANCELLED status
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorUpdateTaskToCancelled)
{
  // ObMViewPendingTask task = make_db_task(OP_MVIEW_ID);
  // ObSEArray<ObMViewPendingTask *, 4> task_ptrs;
  // ASSERT_EQ(OB_SUCCESS, task_ptrs.push_back(&task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(task_ptrs));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_running(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_cancelled(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));

  // ObMViewPendingTask loaded;
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID, loaded));
  // EXPECT_EQ(MV_TASK_CANCELLED, loaded.status_);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 7. DISABLED: update_task_to_retry_wait persists RETRY_WAIT status
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorUpdateTaskToRetryWait)
{
  // ObMViewPendingTask task = make_db_task(OP_MVIEW_ID);
  // ObSEArray<ObMViewPendingTask *, 4> task_ptrs;
  // ASSERT_EQ(OB_SUCCESS, task_ptrs.push_back(&task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(task_ptrs));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_running(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_retry_wait(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID));

  // ObMViewPendingTask loaded;
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID, loaded));
  // EXPECT_EQ(MV_TASK_RETRY_WAIT, loaded.status_);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 8. DISABLED: batch_update_tasks_to_cancelled marks downstream pending tasks
//
// Topology: LEAF (RUNNING) --fails--> ROOT (PENDING, dep: LEAF)
// After batch_update for LEAF, ROOT must be CANCELLED.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorBatchCancelDependents)
{
  // const uint64_t LEAF = OP_MVIEW_ID, ROOT = OP_MVIEW_ID + 1;
  // ObMViewPendingTask leaf_task = make_db_task(LEAF);
  // ObMViewPendingTask root_task = make_db_task(ROOT);
  // ObSEArray<ObMViewPendingTask *, 4> ptrs;
  // ASSERT_EQ(OB_SUCCESS, ptrs.push_back(&leaf_task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(ptrs));
  // ptrs.reset();
  // ASSERT_EQ(OB_SUCCESS, ptrs.push_back(&root_task));
  // ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(ptrs));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.update_task_to_running(OP_TENANT_ID, OP_REFRESH_ID, LEAF));
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.batch_update_tasks_to_cancelled(OP_TENANT_ID, OP_REFRESH_ID, LEAF));

  // ObMViewPendingTask loaded;
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, ROOT, loaded));
  // EXPECT_EQ(MV_TASK_CANCELLED, loaded.status_);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 9. DISABLED: delete_tasks_by_refresh_id removes every task of a refresh
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorDeleteByRefreshId)
{
  // static const int N = 3;
  // for (int i = 0; i < N; ++i) {
  //   ObMViewPendingTask t = make_db_task(OP_MVIEW_ID + i);
  //   ObSEArray<ObMViewPendingTask *, 4> ptrs;
  //   ASSERT_EQ(OB_SUCCESS, ptrs.push_back(&t));
  //   ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(ptrs));
  // }
  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));

  // for (int i = 0; i < N; ++i) {
  //   ObMViewPendingTask loaded;
  //   EXPECT_NE(OB_SUCCESS,
  //             op_.load_task(OP_TENANT_ID, OP_REFRESH_ID, OP_MVIEW_ID + i, loaded));
  // }
}

// ---------------------------------------------------------------------------
// 10. DISABLED: load_all_tasks returns at least the tasks that were inserted
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskTableOperatorTest, DISABLED_OperatorLoadAllTasks)
{
  // static const int N = 4;
  // for (int i = 0; i < N; ++i) {
  //   ObMViewPendingTask t = make_db_task(OP_MVIEW_ID + i);
  //   ObSEArray<ObMViewPendingTask *, 4> ptrs;
  //   ASSERT_EQ(OB_SUCCESS, ptrs.push_back(&t));
  //   ASSERT_EQ(OB_SUCCESS, op_.insert_tasks(ptrs));
  // }

  // common::ObSEArray<ObMViewPendingTask, 8> tasks;
  // ASSERT_EQ(OB_SUCCESS, op_.load_all_tasks(tasks));
  // EXPECT_GE(tasks.count(), N);

  // ASSERT_EQ(OB_SUCCESS,
  //           op_.delete_tasks_by_refresh_id(OP_TENANT_ID, OP_REFRESH_ID));
}

// ===========================================================================
// load_tasks_batch tests
// ===========================================================================
//
// These tests cover the full streaming aggregation path of load_tasks_batch
// without requiring a live SQL proxy. A MockMySQLProxy intercepts the read()
// call and plants a FakeResultHandler that returns canned FakeRow data.
//
// Column order in FakeRow:
//   refresh_id, mview_id, seq, status, flags, skip_cnt, retry_count,
//   next_retry_ts, refresh_method, refresh_parallel, gmt_create_ts,
//   gmt_modified_ts, dep_mview_id, dep_is_null, target_data_sync_scn
// ===========================================================================

class ObLoadTasksBatchTest : public ::testing::Test
{
public:
  void SetUp() override
  {
    proxy_ = new common::MockMySQLProxy();
    fake_  = new FakeMySQLResult();

    EXPECT_CALL(*proxy_, read(_, _, _))
        .WillRepeatedly(Invoke([this](common::ObISQLClient::ReadResult &res, const uint64_t, const char *) {
          // Reset cursor so the same fake can be re-iterated by repeat calls.
          fake_->reset_cursor();
          FakeResultHandler *h = nullptr;
          return res.create_handler(h, fake_);
        }));

    ASSERT_EQ(OB_SUCCESS, op_.init(proxy_));
  }

  void TearDown() override
  {
    op_.destroy();
    delete fake_; fake_ = nullptr;
    delete proxy_; proxy_ = nullptr;
  }

  static const ObMViewPendingTask *find_by_mview(const ObIArray<ObMViewPendingTask *> &tasks,
                                                  uint64_t m)
  {
    for (int64_t i = 0; i < tasks.count(); ++i) {
      if (OB_NOT_NULL(tasks.at(i)) && tasks.at(i)->mview_id_ == m) return tasks.at(i);
    }
    return nullptr;
  }

  common::MockMySQLProxy *proxy_ = nullptr;
  FakeMySQLResult *fake_ = nullptr;
  ObMViewPendingTaskTableOperator op_;
};

// ---------------------------------------------------------------------------
// LoadTasksBatch_SignedMviewIdRoundTrip
//
// Regression test for the patch that switches mview_id / dep_mview_id from
// EXTRACT_UINT_FIELD_MYSQL to EXTRACT_INT_FIELD_MYSQL. FakeMySQLResult only
// exposes mview_id / dep_mview_id via get_int (modeling the BIGINT signed
// inner-table column type); the pre-fix code path would call get_uint and
// receive OB_ERR_COLUMN_NOT_FOUND, failing this test.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, SignedMviewIdRoundTrip)
{
  static const FakeRow ROWS[] = {
    // rid mview seq sts flg skp retry nrt rm rp gc gm dep null tgt
    {  7,  500001, 0,  0,  1,  0,   0,  0, 0, 1, 100, 200,    0, true,  1000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(1, tasks.count());

  const ObMViewPendingTask *t = tasks.at(0);
  ASSERT_NE(nullptr, t);
  EXPECT_EQ(7,             t->refresh_id_);
  EXPECT_EQ(500001UL,      t->mview_id_);
  EXPECT_EQ(1000UL,        t->target_data_sync_scn_);
  EXPECT_EQ(MV_TASK_PENDING, t->status_);
  EXPECT_EQ(1,             t->flags_);
  EXPECT_EQ(0,             t->dep_mview_id_cnt_);
  EXPECT_EQ(nullptr,       t->dep_mview_ids_);
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_NullDepIsTolerated
//
// A LEFT JOIN row with no dependency yields dep_mview_id IS NULL.
// extract path must convert OB_ERR_NULL_VALUE back to OB_SUCCESS and not push
// any dep_mview_id for the task.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, NullDepIsTolerated)
{
  static const FakeRow ROWS[] = {
    {  10, 600001, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 0, true /*dep null*/, 2000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(1, tasks.count());
  EXPECT_EQ(0,       tasks.at(0)->dep_mview_id_cnt_);
  EXPECT_EQ(nullptr, tasks.at(0)->dep_mview_ids_);
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_MultiRowSameTaskGroupsDeps
//
// Two LEFT JOIN rows for the same (refresh_id, mview_id) must collapse into a
// single task with two dep_mview_ids. A second task with one dep follows.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, MultiRowSameTaskGroupsDeps)
{
  static const FakeRow ROWS[] = {
    // 800001: RUNNING (in-flight during RS restart) — svr_ip/port must survive reload
    {  20, 800001, 0, MV_TASK_RUNNING, 0, 0, 0, 0, 0, 1, 100, 200, 0, true,  3000UL, false, "10.0.0.1", 2881 },
    // 800002/800003: PENDING (not yet dispatched)
    {  20, 800002, 0, MV_TASK_PENDING, 0, 0, 0, 0, 0, 1, 100, 200, 0, true,  3000UL, false, nullptr, 0 },
    {  20, 800003, 0, MV_TASK_PENDING, 0, 0, 0, 0, 0, 1, 100, 200, 0, true,  3000UL, false, nullptr, 0 },
    // task 700001 with deps {800001, 800002} — two LEFT JOIN rows collapse into one task
    {  20, 700001, 0, MV_TASK_PENDING, 1, 0, 0, 0, 0, 1, 100, 200, 800001, false, 3000UL, false, nullptr, 0 },
    {  20, 700001, 0, MV_TASK_PENDING, 1, 0, 0, 0, 0, 1, 100, 200, 800002, false, 3000UL, false, nullptr, 0 },
    // task 700002 with dep {800003}
    {  20, 700002, 0, MV_TASK_PENDING, 1, 0, 0, 0, 0, 1, 100, 200, 800003, false, 3000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 8> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(5, tasks.count());

  const ObMViewPendingTask *t1    = find_by_mview(tasks, 700001);
  const ObMViewPendingTask *t2    = find_by_mview(tasks, 700002);
  const ObMViewPendingTask *leaf1 = find_by_mview(tasks, 800001);
  const ObMViewPendingTask *leaf2 = find_by_mview(tasks, 800002);
  ASSERT_NE(nullptr, t1);
  ASSERT_NE(nullptr, t2);
  ASSERT_NE(nullptr, leaf1);
  ASSERT_NE(nullptr, leaf2);

  // RUNNING leaf must have svr_addr populated from the reloaded row
  EXPECT_EQ(MV_TASK_RUNNING, leaf1->status_);
  EXPECT_TRUE(leaf1->svr_addr_.is_valid());
  EXPECT_EQ(2881, leaf1->svr_addr_.get_port());

  // PENDING leaf must have svr_addr reset
  EXPECT_EQ(MV_TASK_PENDING, leaf2->status_);
  EXPECT_FALSE(leaf2->svr_addr_.is_valid());

  ASSERT_EQ(2, t1->dep_mview_id_cnt_);
  ASSERT_NE(nullptr, t1->dep_mview_ids_);
  EXPECT_EQ(800001UL, t1->dep_mview_ids_[0]);
  EXPECT_EQ(800002UL, t1->dep_mview_ids_[1]);

  ASSERT_EQ(1, t2->dep_mview_id_cnt_);
  ASSERT_NE(nullptr, t2->dep_mview_ids_);
  EXPECT_EQ(800003UL, t2->dep_mview_ids_[0]);
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_EmptyResult
//
// An empty result set must produce an empty tasks array and OB_SUCCESS.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, EmptyResult)
{
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(nullptr, 0));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  EXPECT_EQ(0, tasks.count());
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_NotInitedReturnsError
//
// destroy() the operator before the call; load_tasks_batch must short-circuit
// to OB_NOT_INIT without touching the proxy.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, NotInitedReturnsError)
{
  op_.destroy();
  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  EXPECT_EQ(OB_NOT_INIT, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  EXPECT_EQ(0, tasks.count());
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_AllFieldsPopulated
//
// extract_task_from_result must populate every column of ObMViewPendingTask
// from the result row. Use distinctive non-zero values for each field.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, AllFieldsPopulated)
{
  static const FakeRow ROWS[] = {
    // refresh_id, mview_id, seq, status, flags, skip_cnt, retry_count, next_retry_ts,
    //   refresh_method, refresh_parallel, gmt_create_ts, gmt_modified_ts,
    //   dep_mview_id, dep_is_null, target_data_sync_scn, next_retry_ts_is_null,
    //   svr_ip, svr_port
    { 42, 900001, 7, MV_TASK_RETRY_WAIT, 1, 3, 5, 1700000000,
      2, 8, 1234567, 1234999,
      0, true, 7777UL, false,
      "10.1.2.3", 2881 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(1, tasks.count());

  const ObMViewPendingTask *t = tasks.at(0);
  ASSERT_NE(nullptr, t);
  EXPECT_EQ(42,                  t->refresh_id_);
  EXPECT_EQ(900001UL,            t->mview_id_);
  EXPECT_EQ(7,                   t->seq_);
  EXPECT_EQ(MV_TASK_RETRY_WAIT,  t->status_);
  EXPECT_EQ(1,                   t->flags_);
  EXPECT_EQ(3,                   t->skip_cnt_);
  EXPECT_EQ(5,                   t->retry_count_);
  EXPECT_EQ(1700000000,          t->next_retry_ts_);
  EXPECT_EQ(static_cast<share::schema::ObMVRefreshMethod>(2), t->refresh_method_);
  EXPECT_EQ(8,                   t->refresh_parallel_);
  EXPECT_EQ(1234567,             t->gmt_create_);
  EXPECT_EQ(1234999,             t->gmt_modified_);
  EXPECT_EQ(7777UL,              t->target_data_sync_scn_);
  EXPECT_TRUE(t->svr_addr_.is_valid());
  EXPECT_EQ(2881,                t->svr_addr_.get_port());
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_NextRetryTsNullToleratedAsZero
//
// next_retry_ts is read with EXTRACT_INT_FIELD_MYSQL_SKIP_RET, so a NULL value
// must succeed silently and leave task.next_retry_ts_ at 0.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, NextRetryTsNullToleratedAsZero)
{
  FakeRow row = { 11, 910001, 0, 0, 1, 0, 0, /*next_retry_ts*/ 999, 0, 1, 100, 200,
                  0, true, 1000UL, /*next_retry_ts_is_null*/ true, nullptr, 0 };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(&row, 1));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(1, tasks.count());
  EXPECT_EQ(0, tasks.at(0)->next_retry_ts_);
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_ZeroAndInvalidDepFiltered
//
// A non-NULL dep_mview_id of 0 or OB_INVALID_ID must NOT be added to the task's
// dep array (filtered by `OB_INVALID_ID != dep_id && 0 != dep_id`).
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, ZeroAndInvalidDepFiltered)
{
  static const FakeRow ROWS[] = {
    // task 850001: leaf, no deps — must be in the batch so 920003's dep survives filtering
    { 13, 850001, 0, 0, 0, 0, 0, 0, 0, 1, 100, 200, 0, true, 4000UL, false, nullptr, 0 },
    // task 920001: dep_id == 0 (sentinel, must be skipped before batch check)
    { 13, 920001, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 0,                              false, 4000UL, false, nullptr, 0 },
    // task 920002: dep_id == OB_INVALID_ID (must be skipped before batch check)
    { 13, 920002, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, static_cast<int64_t>(OB_INVALID_ID), false, 4000UL, false, nullptr, 0 },
    // task 920003: real dep on 850001 (in batch) — must survive
    { 13, 920003, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 850001,                         false, 4000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 8> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(4, tasks.count());

  const ObMViewPendingTask *t1 = find_by_mview(tasks, 920001);
  const ObMViewPendingTask *t2 = find_by_mview(tasks, 920002);
  const ObMViewPendingTask *t3 = find_by_mview(tasks, 920003);
  ASSERT_NE(nullptr, t1);
  ASSERT_NE(nullptr, t2);
  ASSERT_NE(nullptr, t3);
  EXPECT_EQ(0, t1->dep_mview_id_cnt_);
  EXPECT_EQ(0, t2->dep_mview_id_cnt_);
  ASSERT_EQ(1, t3->dep_mview_id_cnt_);
  EXPECT_EQ(850001UL, t3->dep_mview_ids_[0]);
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_DifferentRefreshIdSeparatesTasks
//
// Two rows with the same mview_id but different refresh_id must yield two
// separate tasks. Exercises the (refresh_id != prev || mview_id != prev) gate.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, DifferentRefreshIdSeparatesTasks)
{
  static const FakeRow ROWS[] = {
    { 100, 930001, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 0, true, 5000UL, false, nullptr, 0 },
    { 101, 930001, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 0, true, 5000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(2, tasks.count());
  EXPECT_EQ(100, tasks.at(0)->refresh_id_);
  EXPECT_EQ(101, tasks.at(1)->refresh_id_);
  EXPECT_EQ(930001UL, tasks.at(0)->mview_id_);
  EXPECT_EQ(930001UL, tasks.at(1)->mview_id_);
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_MixedDepsAcrossTasks
//
// Three tasks back-to-back: one with two deps, one with no deps (NULL row from
// LEFT JOIN), one with a single dep. Validates that dep_start[i] correctly
// delimits each task's slice of flat_deps even when some tasks contribute zero.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, MixedDepsAcrossTasks)
{
  static const FakeRow ROWS[] = {
    // leaf tasks (no deps) — must be in the batch so A and C's deps survive filtering
    { 200, 860001, 0, 0, 0, 0, 0, 0, 0, 1, 100, 200, 0, true, 6000UL, false, nullptr, 0 },
    { 200, 860002, 0, 0, 0, 0, 0, 0, 0, 1, 100, 200, 0, true, 6000UL, false, nullptr, 0 },
    { 200, 860003, 0, 0, 0, 0, 0, 0, 0, 1, 100, 200, 0, true, 6000UL, false, nullptr, 0 },
    // task A: mview 940001 with deps 860001, 860002 — two LEFT JOIN rows collapse into one task
    { 200, 940001, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 860001, false, 6000UL, false, nullptr, 0 },
    { 200, 940001, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 860002, false, 6000UL, false, nullptr, 0 },
    // task B: mview 940002 with NULL dep (no row in mview_dep)
    { 200, 940002, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 0,      true,  6000UL, false, nullptr, 0 },
    // task C: mview 940003 with single dep 860003
    { 200, 940003, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 860003, false, 6000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 8> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(6, tasks.count());

  const ObMViewPendingTask *a = find_by_mview(tasks, 940001);
  const ObMViewPendingTask *b = find_by_mview(tasks, 940002);
  const ObMViewPendingTask *c = find_by_mview(tasks, 940003);
  ASSERT_NE(nullptr, a);
  ASSERT_NE(nullptr, b);
  ASSERT_NE(nullptr, c);

  ASSERT_EQ(2, a->dep_mview_id_cnt_);
  EXPECT_EQ(860001UL, a->dep_mview_ids_[0]);
  EXPECT_EQ(860002UL, a->dep_mview_ids_[1]);

  EXPECT_EQ(0,       b->dep_mview_id_cnt_);
  EXPECT_EQ(nullptr, b->dep_mview_ids_);

  ASSERT_EQ(1, c->dep_mview_id_cnt_);
  EXPECT_EQ(860003UL, c->dep_mview_ids_[0]);
}

// ---------------------------------------------------------------------------
// LoadTasksBatch_SqlReadFailsPropagates
//
// If sql_proxy_->read returns a non-success ret, load_tasks_batch must
// propagate it without producing any tasks.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, SqlReadFailsPropagates)
{
  ::testing::Mock::VerifyAndClearExpectations(proxy_);
  EXPECT_CALL(*proxy_, read(_, _, _)).WillOnce(Return(OB_ERR_UNEXPECTED));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  EXPECT_EQ(OB_ERR_UNEXPECTED, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  EXPECT_EQ(0, tasks.count());
}

// ===========================================================================
// Dep filtering tests via load_tasks_batch
//
// These three tests verify assign_batch_dep_ids / fill_task_dep_ids through
// the public load_tasks_batch interface using FakeRow data.
// ===========================================================================

// ---------------------------------------------------------------------------
// DepInSameGroupKept
//
// Dep mview that IS a pending task in the same refresh group must be retained.
// Topology: leaf(110001) <- root(110002), both in refresh_id=30.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, DepInSameGroupKept)
{
  static const FakeRow ROWS[] = {
    // leaf: no deps
    { 30, 110001, 0, 0, 0, 0, 0, 0, 0, 1, 100, 200,      0, true,  8000UL, false, nullptr, 0 },
    // root: depends on leaf(110001)
    { 30, 110002, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 110001, false, 8000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(2, tasks.count());

  const ObMViewPendingTask *leaf = find_by_mview(tasks, 110001);
  const ObMViewPendingTask *root = find_by_mview(tasks, 110002);
  ASSERT_NE(nullptr, leaf);
  ASSERT_NE(nullptr, root);

  EXPECT_EQ(0,       leaf->dep_mview_id_cnt_);
  EXPECT_EQ(nullptr, leaf->dep_mview_ids_);

  ASSERT_EQ(1,        root->dep_mview_id_cnt_);
  ASSERT_NE(nullptr,  root->dep_mview_ids_);
  EXPECT_EQ(110001UL, root->dep_mview_ids_[0]);
}

// ---------------------------------------------------------------------------
// DepMixedBatchAndNonBatch
//
// A task lists two deps from the __all_mview_dep join: one is a real pending
// task in the same batch (must be kept), the other is a base table or a mview
// not registered as a pending task (must be dropped).
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, DepMixedBatchAndNonBatch)
{
  static const FakeRow ROWS[] = {
    // leaf(120001): no deps — IS a pending task in the batch
    { 31, 120001, 0, 0, 0, 0, 0, 0, 0, 1, 100, 200,      0, true,  9000UL, false, nullptr, 0 },
    // root(120002): dep on leaf(120001, in batch) + dep on 999999 (base table, NOT in batch)
    { 31, 120002, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 120001, false, 9000UL, false, nullptr, 0 },
    { 31, 120002, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 999999, false, 9000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(2, tasks.count());

  const ObMViewPendingTask *leaf = find_by_mview(tasks, 120001);
  const ObMViewPendingTask *root = find_by_mview(tasks, 120002);
  ASSERT_NE(nullptr, leaf);
  ASSERT_NE(nullptr, root);

  EXPECT_EQ(0,       leaf->dep_mview_id_cnt_);
  EXPECT_EQ(nullptr, leaf->dep_mview_ids_);

  ASSERT_EQ(1,        root->dep_mview_id_cnt_);
  ASSERT_NE(nullptr,  root->dep_mview_ids_);
  EXPECT_EQ(120001UL, root->dep_mview_ids_[0]);
}

// ---------------------------------------------------------------------------
// DepCrossRefreshGroupFiltered
//
// A dep mview that belongs to a different refresh_id group must be filtered:
// each refresh is an independent DAG and cross-group edges are not valid
// in-memory deps.
//
// Topology: task(r=40, m=130001) lists dep 130002 which is a task in r=41.
// Since the dep_key is (refresh_id=40, mview=130002) and that key is absent
// from group r=40, the dep must be dropped.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, DepCrossRefreshGroupFiltered)
{
  static const FakeRow ROWS[] = {
    // group r=40
    { 40, 130001, 0, 0, 1, 0, 0, 0, 0, 1, 100, 200, 130002, false, 10000UL, false, nullptr, 0 },
    // group r=41 — 130002 exists here, but NOT under r=40
    { 41, 130002, 0, 0, 0, 0, 0, 0, 0, 1, 100, 200,      0, true,  10000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(2, tasks.count());

  const ObMViewPendingTask *t40 = find_by_mview(tasks, 130001);
  const ObMViewPendingTask *t41 = find_by_mview(tasks, 130002);
  ASSERT_NE(nullptr, t40);
  ASSERT_NE(nullptr, t41);

  EXPECT_EQ(0,       t40->dep_mview_id_cnt_);
  EXPECT_EQ(nullptr, t40->dep_mview_ids_);
  EXPECT_EQ(0,       t41->dep_mview_id_cnt_);
  EXPECT_EQ(nullptr, t41->dep_mview_ids_);
}

// ---------------------------------------------------------------------------
// RunningTaskWithSvrAddrPopulated
//
// Reload scenario: a RUNNING task persisted svr_ip/svr_port to disk (set by
// mark_task_running). extract_task_from_result must parse them back into
// svr_addr_ so enqueue_reload_task can register it for recovery.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, RunningTaskWithSvrAddrPopulated)
{
  static const FakeRow ROWS[] = {
    // rid  mview   seq  status           flg skp retry nrt rm rp gc  gm  dep  null tgt    nrt_null svr_ip       svr_port
    {  50, 140001, 0, MV_TASK_RUNNING,   1,  0,  0,   0,  0, 1, 100, 200, 0, true, 5000UL, false, "10.10.0.1", 2881 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(1, tasks.count());

  const ObMViewPendingTask *t = tasks.at(0);
  ASSERT_NE(nullptr, t);
  EXPECT_EQ(MV_TASK_RUNNING, t->status_);
  EXPECT_TRUE(t->svr_addr_.is_valid());
  EXPECT_EQ(2881, t->svr_addr_.get_port());
}

// ---------------------------------------------------------------------------
// SvrIpNullYieldsInvalidAddr
//
// When svr_ip is NULL (PENDING task never dispatched), svr_addr_ must be
// reset to an invalid address — not left with stale data.
// ---------------------------------------------------------------------------

TEST_F(ObLoadTasksBatchTest, SvrIpNullYieldsInvalidAddr)
{
  static const FakeRow ROWS[] = {
    // svr_ip == nullptr → NULL in result → svr_addr_.reset()
    {  51, 150001, 0, MV_TASK_PENDING, 1, 0, 0, 0, 0, 1, 100, 200, 0, true, 6000UL, false, nullptr, 0 },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTask *, 4> tasks;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, tasks, 0, -1, 1024));
  ASSERT_EQ(1, tasks.count());

  const ObMViewPendingTask *t = tasks.at(0);
  ASSERT_NE(nullptr, t);
  EXPECT_EQ(MV_TASK_PENDING, t->status_);
  EXPECT_FALSE(t->svr_addr_.is_valid());
  EXPECT_EQ(0U, t->session_id_);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

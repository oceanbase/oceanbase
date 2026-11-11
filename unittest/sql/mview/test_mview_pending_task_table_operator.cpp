/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_sql_string.h"
#include "rootserver/mview/ob_mview_pending_task_table_operator.h"
#include "rootserver/mview/ob_mview_pending_task_define.h"
#include "share/mock_mysql_proxy.h"
#include "share/schema/ob_schema_struct.h"
#include "fake_mysql_result.h"

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::unittest;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class ObMViewPendingTaskTableOperatorTest : public ::testing::Test
{
public:
  void SetUp() override {}
  void TearDown() override { op_.destroy(); }

  ObMViewPendingTaskTableOperator op_;
};

TEST_F(ObMViewPendingTaskTableOperatorTest, OperatorInitDestroy)
{
  common::MockMySQLProxy proxy;
  ASSERT_EQ(OB_SUCCESS, op_.init(&proxy));
  op_.destroy();
}

// ===========================================================================
// load_tasks_batch tests
//
// Column order in FakeRow (matches build_load_tasks_sql SELECT):
//   refresh_id, mview_id, seq, target_data_sync_scn, status, flags, skip_cnt,
//   retry_count, next_retry_ts, refresh_method, refresh_parallel, start_time_ts,
//   svr_ip, svr_port, timeout_us, next_retry_ts_is_null
// ===========================================================================

class ObLoadTasksBatchTest : public ::testing::Test
{
public:
  void SetUp() override
  {
    proxy_ = new common::MockMySQLProxy();
    fake_  = new FakeMySQLResult();

    EXPECT_CALL(*proxy_, read(_, _, _))
        .WillRepeatedly(Invoke(this, &ObLoadTasksBatchTest::handle_read));

    ASSERT_EQ(OB_SUCCESS, op_.init(proxy_));
  }

  int handle_read(common::ObISQLClient::ReadResult &res, const uint64_t, const char *sql)
  {
    int ret = last_read_sql_.assign(sql);
    FakeResultHandler *handler = nullptr;
    if (OB_SUCC(ret)) {
      fake_->reset_cursor();
      ret = res.create_handler(handler, fake_);
    }
    return ret;
  }

  void TearDown() override
  {
    op_.destroy();
    delete fake_; fake_ = nullptr;
    delete proxy_; proxy_ = nullptr;
  }

  static const ObMViewPendingTask *find_task_in_groups(
      const ObIArray<ObMViewPendingTaskGroup *> &groups,
      uint64_t mview_id)
  {
    const ObMViewPendingTask *found = nullptr;
    for (int64_t g = 0; nullptr == found && g < groups.count(); ++g) {
      const ObMViewPendingTaskGroup *group = groups.at(g);
      if (OB_NOT_NULL(group)) {
        for (int64_t i = 0; nullptr == found && i < group->tasks_.count(); ++i) {
          if (group->tasks_.at(i).mview_id_ == mview_id) {
            found = &group->tasks_.at(i);
          }
        }
      }
    }
    return found;
  }

  static const ObMViewPendingTaskGroup *find_group(
      const ObIArray<ObMViewPendingTaskGroup *> &groups,
      int64_t refresh_id)
  {
    const ObMViewPendingTaskGroup *found = nullptr;
    for (int64_t g = 0; nullptr == found && g < groups.count(); ++g) {
      const ObMViewPendingTaskGroup *group = groups.at(g);
      if (OB_NOT_NULL(group) && group->info_.refresh_id_ == refresh_id) {
        found = group;
      }
    }
    return found;
  }

  common::MockMySQLProxy *proxy_ = nullptr;
  FakeMySQLResult *fake_ = nullptr;
  ObSqlString last_read_sql_;
  ObMViewPendingTaskTableOperator op_;
};

TEST_F(ObLoadTasksBatchTest, MviewIdRoundTrip)
{
  static const FakeRow ROWS[] = {
    { 7, 500001, 0, 1000UL, 0, 1, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  ASSERT_EQ(1, groups.count());
  ASSERT_NE(nullptr, groups.at(0));
  EXPECT_EQ(7, groups.at(0)->info_.refresh_id_);
  EXPECT_EQ(1000UL, groups.at(0)->info_.target_data_sync_scn_);
  ASSERT_EQ(1, groups.at(0)->tasks_.count());

  const ObMViewPendingTask &t = groups.at(0)->tasks_.at(0);
  EXPECT_EQ(500001UL, t.mview_id_);
  EXPECT_EQ(MV_TASK_PENDING, t.status_);
  EXPECT_EQ(1, t.flags_);
  EXPECT_EQ(200, t.start_time_);
}

TEST_F(ObLoadTasksBatchTest, MultiTaskSameRefreshOneGroup)
{
  static const FakeRow ROWS[] = {
    { 20, 800001, 0, 3000UL, MV_TASK_RUNNING, 0, 0, 0, 0, 0, 1, 200, "10.0.0.1", 2881, 0, false },
    { 20, 800002, 0, 3000UL, MV_TASK_PENDING, 0, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
    { 20, 800003, 0, 3000UL, MV_TASK_PENDING, 0, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  ASSERT_EQ(1, groups.count());
  ASSERT_EQ(3, groups.at(0)->tasks_.count());
  EXPECT_EQ(3000UL, groups.at(0)->info_.target_data_sync_scn_);

  const ObMViewPendingTask *task1 = find_task_in_groups(groups, 800001);
  const ObMViewPendingTask *task2 = find_task_in_groups(groups, 800002);
  ASSERT_NE(nullptr, task1);
  ASSERT_NE(nullptr, task2);
  EXPECT_EQ(MV_TASK_RUNNING, task1->status_);
  EXPECT_TRUE(task1->svr_addr_.is_valid());
  EXPECT_EQ(2881, task1->svr_addr_.get_port());
  EXPECT_EQ(MV_TASK_PENDING, task2->status_);
  EXPECT_FALSE(task2->svr_addr_.is_valid());
}

TEST_F(ObLoadTasksBatchTest, EmptyResult)
{
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(nullptr, 0));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  EXPECT_EQ(0, groups.count());
}

TEST_F(ObLoadTasksBatchTest, LoadTasksSqlFiltersExistingMaterializedViews)
{
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(nullptr, 0));
  ObSqlString type_filter;
  ASSERT_EQ(OB_SUCCESS,
            type_filter.assign_fmt("t.table_type = %ld ",
                static_cast<int64_t>(share::schema::ObTableType::MATERIALIZED_VIEW)));
  const uint64_t cursors[] = {0, 5000};
  for (const uint64_t last_scn : cursors) {
    SCOPED_TRACE(last_scn);
    ObArenaAllocator alloc;
    ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
    ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, last_scn, 42, 3));
    EXPECT_TRUE(groups.empty());
    ASSERT_NE(nullptr, last_read_sql_.ptr());
    EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "INNER JOIN `oceanbase`.`__all_table` t"));
    EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "ON r.mview_id = t.table_id AND "));
    EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), type_filter.ptr()));
    EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "WHERE r.tenant_id = 0"));
    EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "ORDER BY target_data_sync_scn, refresh_id LIMIT 3"));
    EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "ORDER BY r.target_data_sync_scn, r.refresh_id, r.seq"));
    if (0 == last_scn) {
      EXPECT_EQ(nullptr, strstr(last_read_sql_.ptr(), "target_data_sync_scn >"));
    } else {
      EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(),
          "target_data_sync_scn > 5000 OR (target_data_sync_scn = 5000 AND refresh_id > 42)"));
    }
  }
}

TEST_F(ObLoadTasksBatchTest, InvalidArgumentsAreRejectedAfterDestroy)
{
  op_.destroy();
  ObMViewTaskStatus status = MV_TASK_PENDING;
  int64_t next_retry_ts = 0;
  int64_t start_time = 0;
  uint64_t target_data_sync_scn = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            op_.get_task_sync_info(OB_INVALID_TENANT_ID,
                                    1,
                                    500001,
                                    status,
                                    next_retry_ts,
                                    start_time,
                                    target_data_sync_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            op_.cancel_tasks_by_refresh_id(OB_INVALID_TENANT_ID, 1));
}

TEST_F(ObLoadTasksBatchTest, GetTaskSyncInfoRestoresStartTime)
{
  static const FakeRow ROWS[] = {
    { 17, 500001, 0, 4321UL, MV_TASK_RUNNING, 0, 0, 2, 1700000000,
      1, 4, 1234567, "10.0.0.1", 2881, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObMViewTaskStatus status = MV_TASK_PENDING;
  int64_t next_retry_ts = 0;
  int64_t start_time = 0;
  uint64_t target_data_sync_scn = 0;
  ASSERT_EQ(OB_SUCCESS,
            op_.get_task_sync_info(OB_SYS_TENANT_ID,
                                    17,
                                    500001,
                                    status,
                                    next_retry_ts,
                                    start_time,
                                    target_data_sync_scn));
  EXPECT_EQ(MV_TASK_RUNNING, status);
  EXPECT_EQ(1700000000, next_retry_ts);
  EXPECT_EQ(1234567, start_time);
  EXPECT_EQ(4321UL, target_data_sync_scn);
}

TEST_F(ObLoadTasksBatchTest, AllFieldsPopulated)
{
  static const FakeRow ROWS[] = {
    { 42, 900001, 7, 7777UL, MV_TASK_RETRY_WAIT, 1, 3, 5, 1700000000,
      2, 8, 1234999,
      "10.1.2.3", 2881, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  ASSERT_EQ(1, groups.count());
  EXPECT_EQ(42, groups.at(0)->info_.refresh_id_);
  EXPECT_EQ(7777UL, groups.at(0)->info_.target_data_sync_scn_);
  ASSERT_EQ(1, groups.at(0)->tasks_.count());

  const ObMViewPendingTask &t = groups.at(0)->tasks_.at(0);
  EXPECT_EQ(900001UL, t.mview_id_);
  EXPECT_EQ(7, t.seq_);
  EXPECT_EQ(MV_TASK_RETRY_WAIT, t.status_);
  EXPECT_EQ(1, t.flags_);
  EXPECT_EQ(0, t.runtime_flags_);
  EXPECT_FALSE(t.is_cancelled());
  EXPECT_EQ(3, t.skip_cnt_);
  EXPECT_EQ(5, t.retry_count_);
  EXPECT_EQ(1700000000, t.next_retry_ts_);
  EXPECT_EQ(static_cast<share::schema::ObMVRefreshMethod>(2), t.refresh_method_);
  EXPECT_EQ(8, t.refresh_parallel_);
  EXPECT_EQ(1234999, t.start_time_);
  EXPECT_TRUE(t.svr_addr_.is_valid());
  EXPECT_EQ(2881, t.svr_addr_.get_port());
}

TEST_F(ObLoadTasksBatchTest, NextRetryTsNullToleratedAsZero)
{
  FakeRow row = { 11, 910001, 0, 1000UL, 0, 1, 0, 0, 999, 0, 1, 200,
                  nullptr, 0, 0, true };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(&row, 1));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  ASSERT_EQ(1, groups.count());
  ASSERT_EQ(1, groups.at(0)->tasks_.count());
  EXPECT_EQ(0, groups.at(0)->tasks_.at(0).next_retry_ts_);
}

TEST_F(ObLoadTasksBatchTest, LoadTasksBatchSqlOrdersByScnAndRefreshId)
{
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(nullptr, 0));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 1000UL, 20, 16));
  ASSERT_NE(nullptr, last_read_sql_.ptr());
  EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(),
                            "AND (target_data_sync_scn > 1000 "
                            "OR (target_data_sync_scn = 1000 AND refresh_id > 20))"));
  EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "ORDER BY target_data_sync_scn, refresh_id "));
  EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(),
                            "ORDER BY r.target_data_sync_scn, r.refresh_id, r.seq"));
}

TEST_F(ObLoadTasksBatchTest, DifferentRefreshIdSeparatesGroups)
{
  static const FakeRow ROWS[] = {
    { 100, 930001, 0, 5000UL, 0, 1, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
    { 101, 930001, 0, 5000UL, 0, 1, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  ASSERT_EQ(2, groups.count());
  const ObMViewPendingTaskGroup *g100 = find_group(groups, 100);
  const ObMViewPendingTaskGroup *g101 = find_group(groups, 101);
  ASSERT_NE(nullptr, g100);
  ASSERT_NE(nullptr, g101);
  ASSERT_EQ(1, g100->tasks_.count());
  ASSERT_EQ(1, g101->tasks_.count());
  EXPECT_EQ(930001UL, g100->tasks_.at(0).mview_id_);
  EXPECT_EQ(930001UL, g101->tasks_.at(0).mview_id_);
}

TEST_F(ObLoadTasksBatchTest, SqlReadFailsPropagates)
{
  ::testing::Mock::VerifyAndClearExpectations(proxy_);
  EXPECT_CALL(*proxy_, read(_, _, _)).WillOnce(Return(OB_ERR_UNEXPECTED));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  EXPECT_EQ(OB_ERR_UNEXPECTED, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  EXPECT_EQ(0, groups.count());
}

TEST_F(ObLoadTasksBatchTest, LoadTasksBatchTimeoutUsRoundTrip)
{
  static const FakeRow ROWS[] = {
    { 99, 990001, 0, 1000UL, MV_TASK_PENDING, 0, 0, 0, 0, 0, 1, 200, nullptr, 0, 60000000, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  ASSERT_EQ(1, groups.count());
  ASSERT_EQ(1, groups.at(0)->tasks_.count());
  EXPECT_EQ(60000000, groups.at(0)->tasks_.at(0).timeout_us_);
}

TEST_F(ObLoadTasksBatchTest, SvrIpNullYieldsInvalidAddr)
{
  static const FakeRow ROWS[] = {
    { 51, 150001, 0, 6000UL, MV_TASK_PENDING, 1, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObArenaAllocator alloc;
  ObSEArray<ObMViewPendingTaskGroup *, 4> groups;
  ASSERT_EQ(OB_SUCCESS, op_.load_tasks_batch(alloc, groups, 0, -1, 1024));
  ASSERT_EQ(1, groups.count());
  ASSERT_EQ(1, groups.at(0)->tasks_.count());
  const ObMViewPendingTask &t = groups.at(0)->tasks_.at(0);
  EXPECT_EQ(MV_TASK_PENDING, t.status_);
  EXPECT_FALSE(t.svr_addr_.is_valid());
  EXPECT_EQ(0U, t.session_id_);
}

// ===========================================================================
// get_active_refresh_ids_by_mview tests
//
// An initial refresh may only reuse a COMPLETE or FORCE refresh, so it asks for
// the method filter. Cancelling on behalf of a force refresh must see every
// active refresh instead, so it asks for the unfiltered set. Both orders the
// newest refresh first.
// ===========================================================================

TEST_F(ObLoadTasksBatchTest, GetActiveRefreshIdsAppliesRequestedMethodFilter)
{
  static const FakeRow ROWS[] = {
    { 31, 500001, 0, 2000UL, MV_TASK_RUNNING, 0, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
    { 30, 500001, 0, 1000UL, MV_TASK_PENDING, 0, 0, 0, 0, 0, 1, 200, nullptr, 0, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObSEArray<int64_t, 4> refresh_ids;
  ASSERT_EQ(OB_SUCCESS,
            op_.get_active_refresh_ids_by_mview(OB_SYS_TENANT_ID,
                                                500001,
                                                true,
                                                refresh_ids));
  ASSERT_EQ(2, refresh_ids.count());
  EXPECT_EQ(31, refresh_ids.at(0));
  EXPECT_EQ(30, refresh_ids.at(1));

  ASSERT_NE(nullptr, last_read_sql_.ptr());
  EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "mview_id = 500001"));
  EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "refresh_method IN (1, 3)"));
  EXPECT_EQ(nullptr, strstr(last_read_sql_.ptr(), "target_data_sync_scn <"));
  EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "ORDER BY target_data_sync_scn DESC"));
}

TEST_F(ObLoadTasksBatchTest, GetActiveRefreshIdsKeepsEveryMethodWhenFilterNotRequested)
{
  static const FakeRow ROWS[] = {
    { 41, 500002, 0, 1000UL, MV_TASK_RETRY_WAIT, 0, 0, 0, 0, 2, 1, 200, nullptr, 0, 0, false },
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(ROWS, ARRAYSIZEOF(ROWS)));

  ObSEArray<int64_t, 4> refresh_ids;
  ASSERT_EQ(OB_SUCCESS,
            op_.get_active_refresh_ids_by_mview(OB_SYS_TENANT_ID,
                                                500002,
                                                false,
                                                refresh_ids));
  ASSERT_EQ(1, refresh_ids.count());
  EXPECT_EQ(41, refresh_ids.at(0));

  ASSERT_NE(nullptr, last_read_sql_.ptr());
  EXPECT_EQ(nullptr, strstr(last_read_sql_.ptr(), "refresh_method"));
  EXPECT_EQ(nullptr, strstr(last_read_sql_.ptr(), "target_data_sync_scn <"));
  EXPECT_NE(nullptr, strstr(last_read_sql_.ptr(), "ORDER BY target_data_sync_scn DESC"));
}

TEST_F(ObLoadTasksBatchTest, GetActiveRefreshIdsRejectsInvalidArguments)
{
  ObSEArray<int64_t, 4> refresh_ids;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            op_.get_active_refresh_ids_by_mview(OB_INVALID_TENANT_ID,
                                                500001,
                                                true,
                                                refresh_ids));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            op_.get_active_refresh_ids_by_mview(OB_SYS_TENANT_ID,
                                                OB_INVALID_ID,
                                                true,
                                                refresh_ids));
  EXPECT_EQ(0, refresh_ids.count());
}

class ObMinPendingTaskSnapshotTest : public ::testing::Test
{
public:
  void SetUp() override
  {
    proxy_ = new common::MockMySQLProxy();
    fake_  = new FakeMySQLResult();

    EXPECT_CALL(*proxy_, read(_, _, _))
        .WillRepeatedly(Invoke([this](common::ObISQLClient::ReadResult &res, const uint64_t, const char *sql) {
          EXPECT_NE(nullptr, strstr(sql, "MIN(target_data_sync_scn) AS min_scn"));
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

  common::MockMySQLProxy *proxy_ = nullptr;
  FakeMySQLResult *fake_ = nullptr;
  ObMViewPendingTaskTableOperator op_;
};

TEST_F(ObMinPendingTaskSnapshotTest, NullMinScnLeavesInvalid)
{
  fake_->set_min_scn_result(0, true /*is_null*/);

  share::SCN scn;
  scn.set_invalid();
  ASSERT_EQ(OB_SUCCESS, op_.get_min_pending_task_snapshot(scn));
  EXPECT_FALSE(scn.is_valid());
  EXPECT_FALSE(scn.is_min());
}

TEST_F(ObMinPendingTaskSnapshotTest, ValidMinScnConverted)
{
  fake_->set_min_scn_result(123456789UL, false /*is_null*/);

  share::SCN scn;
  scn.set_invalid();
  ASSERT_EQ(OB_SUCCESS, op_.get_min_pending_task_snapshot(scn));
  EXPECT_TRUE(scn.is_valid());
  EXPECT_EQ(123456789UL, scn.get_val_for_inner_table_field());
}

TEST_F(ObMinPendingTaskSnapshotTest, ZeroMinScnIsValidMin)
{
  fake_->set_min_scn_result(0, false /*is_null*/);

  share::SCN scn;
  scn.set_invalid();
  ASSERT_EQ(OB_SUCCESS, op_.get_min_pending_task_snapshot(scn));
  EXPECT_TRUE(scn.is_valid());
  EXPECT_TRUE(scn.is_min());
}

TEST_F(ObMinPendingTaskSnapshotTest, EmptyResultLeavesInvalid)
{
  fake_->set_empty_min_scn_result();

  share::SCN scn;
  scn.set_invalid();
  ASSERT_EQ(OB_SUCCESS, op_.get_min_pending_task_snapshot(scn));
  EXPECT_FALSE(scn.is_valid());
}

TEST_F(ObMinPendingTaskSnapshotTest, OversizedMinScnFails)
{
  fake_->set_min_scn_result(share::OB_MAX_SCN_TS_NS + 1, false /*is_null*/);

  share::SCN scn;
  scn.set_invalid();
  EXPECT_EQ(OB_INVALID_ARGUMENT, op_.get_min_pending_task_snapshot(scn));
  EXPECT_FALSE(scn.is_valid());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

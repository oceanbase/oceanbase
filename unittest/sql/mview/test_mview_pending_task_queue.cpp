/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <thread>
#include <atomic>
#include <vector>
#define private public   // 获取private成员
#define protected public // 获取protected成员
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "rootserver/mview/ob_mview_sched_info_mgr.h"
#include "rootserver/mview/ob_mview_pending_task_queue.h"
#include "rootserver/mview/ob_mview_pending_task_define.h"
#include "share/ob_cluster_version.h"
#include "share/mock_mysql_proxy.h"
#include "share/scn.h"
#include "share/schema/ob_mview_info.h"
#include "fake_mysql_result.h"
#undef private
#undef protected
#include "share/ob_mview_args.h"

using ::testing::_;
using ::testing::Invoke;

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::unittest;

static const uint64_t TENANT_ID = OB_SYS_TENANT_ID; // sys tenant always has an allocator in unit tests

// ---------------------------------------------------------------------------
// Fake SQL result for load_mview_sched_base_info.
// ---------------------------------------------------------------------------

struct FakeSchedBaseInfoRow
{
  FakeSchedBaseInfoRow(const uint64_t id = OB_INVALID_ID,
                      const uint64_t scn = 0,
                      const uint64_t flags = 0,
                      const int64_t interval = 0,
                      const int64_t start = OB_INVALID_TIMESTAMP,
                      const int64_t create_time = 0,
                      const share::schema::ObMVRefreshMode mode = share::schema::ObMVRefreshMode::DEMAND)
    : mview_id(id),
      last_start_time_scn(scn),
      refresh_flags(flags),
      refresh_interval(interval),
      refresh_start(start),
      mview_create_time(create_time),
      refresh_mode(mode)
  {}

  uint64_t mview_id;
  uint64_t last_start_time_scn;
  uint64_t refresh_flags;
  int64_t refresh_interval;
  int64_t refresh_start;
  int64_t mview_create_time;
  share::schema::ObMVRefreshMode refresh_mode;

  int64_t to_string(char *, const int64_t) const { return 0; }
};

class FakeSchedBaseInfoResult : public sqlclient::ObMySQLResult
{
public:
  FakeSchedBaseInfoResult() : cursor_(-1) {}
  virtual ~FakeSchedBaseInfoResult() {}

  int load_rows(const FakeSchedBaseInfoRow *rows, int64_t count)
  {
    int ret = OB_SUCCESS;
    rows_.reset();
    cursor_ = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(rows_.push_back(rows[i]))) {
        // propagate
      }
    }
    return ret;
  }

  void reset_cursor() { cursor_ = -1; }
  void clear_rows()
  {
    rows_.reset();
    cursor_ = -1;
  }

  int64_t get_column_count() const override { return 7; }

  int close() override { return OB_SUCCESS; }
  int next() override
  {
    return (++cursor_ < rows_.count()) ? OB_SUCCESS : OB_ITER_END;
  }

  int get_int(const char *col_name, int64_t &v) const override
  {
    int ret = OB_SUCCESS;
    if (cursor_ < 0 || cursor_ >= rows_.count()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (0 == strcmp(col_name, "mview_id")) {
      v = static_cast<int64_t>(rows_.at(cursor_).mview_id);
    } else if (0 == strcmp(col_name, "refresh_interval")) {
      v = rows_.at(cursor_).refresh_interval;
    } else if (0 == strcmp(col_name, "refresh_mode")) {
      v = static_cast<int64_t>(rows_.at(cursor_).refresh_mode);
    } else if (0 == strcmp(col_name, "refresh_start")) {
      v = rows_.at(cursor_).refresh_start;
    } else if (0 == strcmp(col_name, "mview_create_time")) {
      v = rows_.at(cursor_).mview_create_time;
    } else {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
    return ret;
  }

  int get_uint(const char *col_name, uint64_t &v) const override
  {
    int ret = OB_SUCCESS;
    if (cursor_ < 0 || cursor_ >= rows_.count()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (0 == strcmp(col_name, "last_start_time")) {
      v = rows_.at(cursor_).last_start_time_scn;
    } else if (0 == strcmp(col_name, "refresh_flags")) {
      v = rows_.at(cursor_).refresh_flags;
    } else {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
    return ret;
  }

  int get_varchar(const char *, ObString &) const override { return OB_NOT_SUPPORTED; }
  int get_int(const int64_t, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_uint(const int64_t, uint64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_datetime(const int64_t, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_date(const int64_t, int32_t &) const override { return OB_NOT_SUPPORTED; }
  int get_time(const int64_t, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_year(const int64_t, uint8_t &) const override { return OB_NOT_SUPPORTED; }
  int get_bool(const int64_t, bool &) const override { return OB_NOT_SUPPORTED; }
  int get_varchar(const int64_t, ObString &) const override { return OB_NOT_SUPPORTED; }
  int get_raw(const int64_t, ObString &) const override { return OB_NOT_SUPPORTED; }
  int get_float(const int64_t, float &) const override { return OB_NOT_SUPPORTED; }
  int get_double(const int64_t, double &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp(const int64_t, const ObTimeZoneInfo *, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_otimestamp_value(const int64_t, const ObTimeZoneInfo &,
                           const ObObjType, ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp_tz(const int64_t, const ObTimeZoneInfo &,
                       ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp_ltz(const int64_t, const ObTimeZoneInfo &,
                        ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp_nano(const int64_t, const ObTimeZoneInfo &,
                         ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_type(const int64_t, ObObjMeta &) const override { return OB_NOT_SUPPORTED; }
  int get_col_meta(const int64_t, bool, ObString &, ObDataType &) const override { return OB_NOT_SUPPORTED; }
  int get_obj(const int64_t, ObObj &, const ObTimeZoneInfo *,
              ObIAllocator *) const override { return OB_NOT_SUPPORTED; }
  int get_interval_ym(const int64_t, ObIntervalYMValue &) const override { return OB_NOT_SUPPORTED; }
  int get_interval_ds(const int64_t, ObIntervalDSValue &) const override { return OB_NOT_SUPPORTED; }
  int get_nvarchar2(const int64_t, ObString &) const override { return OB_NOT_SUPPORTED; }
  int get_nchar(const int64_t, ObString &) const override { return OB_NOT_SUPPORTED; }
  int get_datetime(const char *, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_date(const char *, int32_t &) const override { return OB_NOT_SUPPORTED; }
  int get_time(const char *, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_year(const char *, uint8_t &) const override { return OB_NOT_SUPPORTED; }
  int get_bool(const char *, bool &) const override { return OB_NOT_SUPPORTED; }
  int get_raw(const char *, ObString &) const override { return OB_NOT_SUPPORTED; }
  int get_float(const char *, float &) const override { return OB_NOT_SUPPORTED; }
  int get_double(const char *, double &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp(const char *, const ObTimeZoneInfo *, int64_t &) const override { return OB_NOT_SUPPORTED; }
  int get_otimestamp_value(const char *, const ObTimeZoneInfo &,
                           const ObObjType, ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp_tz(const char *, const ObTimeZoneInfo &,
                       ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp_ltz(const char *, const ObTimeZoneInfo &,
                        ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_timestamp_nano(const char *, const ObTimeZoneInfo &,
                         ObOTimestampData &) const override { return OB_NOT_SUPPORTED; }
  int get_type(const char *, ObObjMeta &) const override { return OB_NOT_SUPPORTED; }
  int get_obj(const char *, ObObj &) const override { return OB_NOT_SUPPORTED; }
  int get_interval_ym(const char *, ObIntervalYMValue &) const override { return OB_NOT_SUPPORTED; }
  int get_interval_ds(const char *, ObIntervalDSValue &) const override { return OB_NOT_SUPPORTED; }
  int get_nvarchar2(const char *, ObString &) const override { return OB_NOT_SUPPORTED; }
  int get_nchar(const char *, ObString &) const override { return OB_NOT_SUPPORTED; }

private:
  int inner_get_number(const int64_t, number::ObNumber &, IAllocator &) const override
  { return OB_NOT_SUPPORTED; }
  int inner_get_number(const char *, number::ObNumber &, IAllocator &) const override
  { return OB_NOT_SUPPORTED; }
  int inner_get_urowid(const char *, ObURowIDData &, ObIAllocator &) const override
  { return OB_NOT_SUPPORTED; }
  int inner_get_urowid(const int64_t, ObURowIDData &, ObIAllocator &) const override
  { return OB_NOT_SUPPORTED; }
  int inner_get_lob_locator(const char *, common::ObLobLocator *&, ObIAllocator &) const override
  { return OB_NOT_SUPPORTED; }
  int inner_get_lob_locator(const int64_t, common::ObLobLocator *&, ObIAllocator &) const override
  { return OB_NOT_SUPPORTED; }

  ObSEArray<FakeSchedBaseInfoRow, 4> rows_;
  int64_t cursor_;
};

class FakeSchedBaseInfoResultHandler : public sqlclient::ObISQLResultHandler
{
public:
  explicit FakeSchedBaseInfoResultHandler(FakeSchedBaseInfoResult *result)
    : result_(result)
  {
  }
  virtual ~FakeSchedBaseInfoResultHandler() {}
  sqlclient::ObMySQLResult *mysql_result() override { return result_; }

private:
  FakeSchedBaseInfoResult *result_;
};

class ObMViewSchedInfoMgrLoadBaseInfoTest : public ::testing::Test
{
public:
  static const uint64_t TEST_MVIEW_ID = 500001UL;
  static const uint64_t TEST_SCN_VAL = 100000UL;
  static const int64_t TEST_REFRESH_INTERVAL = 10LL * 1000 * 1000;
  static const int64_t TEST_REFRESH_START = 20LL * 1000 * 1000;
  static const int64_t TEST_MVIEW_CREATE_TIME = 5LL * 1000 * 1000;

  static int64_t scn_to_last_start_time_us(const uint64_t scn_val)
  {
    share::SCN scn;
    EXPECT_EQ(OB_SUCCESS, scn.convert_for_inner_table_field(scn_val));
    return scn.convert_to_ts();
  }

  int handle_read(ObISQLClient::ReadResult &res,
                  const uint64_t,
                  const char *sql)
  {
    int ret = last_sql_.assign(sql);
    const bool is_dep_query = NULL != strstr(sql, "dep_mview_id");
    if (OB_FAIL(ret)) {
    } else if (is_dep_query && fail_dep_read_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_dep_query ? fake_->clear_rows() : fake_->reset_cursor();
      FakeSchedBaseInfoResultHandler *handler = nullptr;
      ret = res.create_handler(handler, fake_);
    }
    return ret;
  }

  void SetUp() override
  {
    ObClusterVersion::get_instance().update_cluster_version(DATA_CURRENT_VERSION);
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    proxy_ = new MockMySQLProxy();
    fake_ = new FakeSchedBaseInfoResult();
    EXPECT_CALL(*proxy_, read(_, _, _))
        .WillRepeatedly(Invoke(
            this, &ObMViewSchedInfoMgrLoadBaseInfoTest::handle_read));
    ASSERT_EQ(OB_SUCCESS, mgr_.init(TENANT_ID, 64L));
  }

  void TearDown() override
  {
    ObClusterVersion::get_instance().update_cluster_version(DATA_CURRENT_VERSION);
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ASSERT_EQ(OB_SUCCESS, mgr_.destroy());
    delete fake_;
    fake_ = nullptr;
    delete proxy_;
    proxy_ = nullptr;
  }

  int load_one_row(const uint64_t mview_id,
                   const uint64_t scn_val,
                   const int64_t refresh_interval = TEST_REFRESH_INTERVAL,
                   const int64_t refresh_start = OB_INVALID_TIMESTAMP,
                   const uint64_t refresh_flags = 0,
                   const int64_t mview_create_time = TEST_MVIEW_CREATE_TIME,
                   const share::schema::ObMVRefreshMode refresh_mode = share::schema::ObMVRefreshMode::DEMAND)
  {
    const FakeSchedBaseInfoRow row = {
      mview_id, scn_val, refresh_flags, refresh_interval, refresh_start, mview_create_time,
      refresh_mode
    };
    return fake_->load_rows(&row, 1);
  }

  int get_next_trigger_ts(const uint64_t mview_id, int64_t &next_trigger_ts)
  {
    int ret = OB_SUCCESS;
    const ObMViewSchedInfo *sched_info = mgr_.get_sched_info(mview_id);
    ObMViewSchedGroupInfo group(mgr_.alloc_);
    next_trigger_ts = OB_INVALID_TIMESTAMP;
    if (OB_ISNULL(sched_info) || OB_UNLIKELY(!sched_info->need_periodic_schedule())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(group.top_mview_ids_.push_back(mview_id))) {
    } else {
      group.refresh_interval_ = sched_info->refresh_interval_;
      group.modified_ts_ = 1;
      ret = mgr_.get_sched_group_next_trigger_ts(group, next_trigger_ts);
    }
    return ret;
  }

  ObMViewSchedInfoMgr mgr_;
  MockMySQLProxy *proxy_ = nullptr;
  FakeSchedBaseInfoResult *fake_ = nullptr;
  ObSqlString last_sql_;
  bool fail_dep_read_ = false;
};

const uint64_t ObMViewSchedInfoMgrLoadBaseInfoTest::TEST_MVIEW_ID;
const uint64_t ObMViewSchedInfoMgrLoadBaseInfoTest::TEST_SCN_VAL;
const int64_t ObMViewSchedInfoMgrLoadBaseInfoTest::TEST_REFRESH_INTERVAL;
const int64_t ObMViewSchedInfoMgrLoadBaseInfoTest::TEST_REFRESH_START;
const int64_t ObMViewSchedInfoMgrLoadBaseInfoTest::TEST_MVIEW_CREATE_TIME;

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, NewNodeRestoresLastStartTimeFromScn)
{
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  const ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(ObMViewSchedInfoMgrLoadBaseInfoTest::scn_to_last_start_time_us(TEST_SCN_VAL),
            node->last_start_time_);
  EXPECT_EQ(TEST_REFRESH_INTERVAL, node->specified_refresh_interval_);
  EXPECT_EQ(TEST_MVIEW_CREATE_TIME + TEST_REFRESH_INTERVAL, node->get_refresh_start());
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "a.refresh_interval AS refresh_interval"));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "time_to_usec(a.gmt_create) AS mview_create_time"));
  EXPECT_FALSE(node->scheduler_suspended_);
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "a.refresh_flags AS refresh_flags"));
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, ReloadsSchedulerSuspendFromRefreshFlags)
{
  ASSERT_EQ(OB_SUCCESS,
            load_one_row(TEST_MVIEW_ID,
                         TEST_SCN_VAL,
                         TEST_REFRESH_INTERVAL,
                         OB_INVALID_TIMESTAMP,
                         share::schema::ObMViewInfo::REFRESH_SCHEDULER_SUSPENDED_FLAG));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_TRUE(node->scheduler_suspended_);

  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_FALSE(node->scheduler_suspended_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, NeverRefreshModeAndSuspendFlagBothBlockScheduling)
{
  struct RefreshState
  {
    RefreshState(const share::schema::ObMVRefreshMode mode, const uint64_t flags, const bool is_suspended)
      : refresh_mode_(mode), refresh_flags_(flags), is_suspended_(is_suspended)
    {}

    share::schema::ObMVRefreshMode refresh_mode_;
    uint64_t refresh_flags_;
    bool is_suspended_;
  };

  const uint64_t suspend_flag = share::schema::ObMViewInfo::REFRESH_SCHEDULER_SUSPENDED_FLAG;
  const uint64_t other_flag = 1ULL << 8;
  const RefreshState states[] = {
    {share::schema::ObMVRefreshMode::NEVER, 0, true},
    {share::schema::ObMVRefreshMode::DEMAND, 0, false},
    {share::schema::ObMVRefreshMode::NEVER, suspend_flag | other_flag, true},
    {share::schema::ObMVRefreshMode::DEMAND, suspend_flag | other_flag, true},
    {share::schema::ObMVRefreshMode::NEVER, other_flag, true},
    {share::schema::ObMVRefreshMode::DEMAND, other_flag, false},
    {share::schema::ObMVRefreshMode::NEVER, 0, true},
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(states); ++i) {
    SCOPED_TRACE(i);
    const RefreshState &state = states[i];
    ASSERT_EQ(OB_SUCCESS,
              load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL, TEST_REFRESH_INTERVAL,
                           OB_INVALID_TIMESTAMP, state.refresh_flags_, TEST_MVIEW_CREATE_TIME,
                           state.refresh_mode_));
    ASSERT_EQ(OB_SUCCESS,
              mgr_.load_mview_sched_base_info(*proxy_, 0 == i ? OB_INVALID_ID : TEST_MVIEW_ID));
    const ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
    ASSERT_NE(nullptr, node);
    EXPECT_EQ(state.is_suspended_, node->is_scheduler_suspended());
    EXPECT_EQ(TEST_REFRESH_INTERVAL, node->specified_refresh_interval_);
    EXPECT_EQ(TEST_MVIEW_CREATE_TIME + TEST_REFRESH_INTERVAL, node->get_refresh_start());
    EXPECT_EQ(1, mgr_.info_map_.size());
    EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "a.refresh_mode AS refresh_mode"));
  }
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, PeriodicRefreshStartDelaysFirstTrigger)
{
  static const int64_t EFFECTIVE_REFRESH_INTERVAL = 6LL * 1000 * 1000;
  static const int64_t GROUP_REFRESH_INTERVAL = 2LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS,
            load_one_row(TEST_MVIEW_ID,
                         TEST_SCN_VAL,
                         TEST_REFRESH_INTERVAL,
                         TEST_REFRESH_START));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(ObMViewSchedInfoMgrLoadBaseInfoTest::scn_to_last_start_time_us(TEST_SCN_VAL),
            node->last_start_time_);
  EXPECT_EQ(TEST_REFRESH_INTERVAL, node->specified_refresh_interval_);
  node->refresh_interval_ = EFFECTIVE_REFRESH_INTERVAL;
  EXPECT_EQ(TEST_REFRESH_START, node->get_refresh_start());
  int64_t node_next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, get_next_trigger_ts(node->mview_id_, node_next_trigger_ts));
  EXPECT_EQ(TEST_REFRESH_START, node_next_trigger_ts);

  ObMViewSchedInfo *other = OB_NEWx(ObMViewSchedInfo, &mgr_.alloc_, mgr_.alloc_);
  ASSERT_NE(nullptr, other);
  other->mview_id_ = TEST_MVIEW_ID + 1;
  other->refresh_interval_ = GROUP_REFRESH_INTERVAL;
  other->last_start_time_ = TEST_REFRESH_START - GROUP_REFRESH_INTERVAL / 2;
  ObMViewSchedGroupInfo group(mgr_.alloc_);
  group.refresh_interval_ = GROUP_REFRESH_INTERVAL;
  group.modified_ts_ = 1;
  ASSERT_EQ(OB_SUCCESS, group.top_mview_ids_.push_back(node->mview_id_));
  ASSERT_EQ(OB_SUCCESS, group.top_mview_ids_.push_back(other->mview_id_));
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, mgr_.info_map_.set_refactored(other->mview_id_, other));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_next_trigger_ts(group, next_trigger_ts));
  EXPECT_EQ(TEST_REFRESH_START, next_trigger_ts);
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "time_to_usec(a.refresh_start) AS refresh_start"));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "status IN (0, 1)"));
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, NonPeriodicRefreshStartIsIgnored)
{
  ASSERT_EQ(OB_SUCCESS,
            load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL, 0, TEST_REFRESH_START));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  const ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(TEST_REFRESH_START, node->get_refresh_start());
  EXPECT_EQ(ObMViewSchedInfoMgrLoadBaseInfoTest::scn_to_last_start_time_us(TEST_SCN_VAL),
            node->last_start_time_);
  EXPECT_EQ(0, node->specified_refresh_interval_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, RefreshStartSurvivesRefreshIntervalRecalc)
{
  static const int64_t FIRST_EFFECTIVE_INTERVAL = 6LL * 1000 * 1000;
  static const int64_t SECOND_EFFECTIVE_INTERVAL = 3LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS,
            load_one_row(TEST_MVIEW_ID,
                         TEST_SCN_VAL,
                         TEST_REFRESH_INTERVAL,
                         TEST_REFRESH_START));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  node->refresh_interval_ = FIRST_EFFECTIVE_INTERVAL;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, get_next_trigger_ts(node->mview_id_, next_trigger_ts));
  EXPECT_EQ(TEST_REFRESH_START, next_trigger_ts);
  node->refresh_interval_ = SECOND_EFFECTIVE_INTERVAL;
  ASSERT_EQ(OB_SUCCESS, get_next_trigger_ts(node->mview_id_, next_trigger_ts));
  EXPECT_EQ(TEST_REFRESH_START, next_trigger_ts);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, OldDataVersionDoesNotInitializeTimer)
{
  ObClusterVersion::get_instance().update_cluster_version(DATA_VERSION_5_0_0_0);
  EXPECT_EQ(OB_EAGAIN, mgr_.init_sched_info(*proxy_));
  EXPECT_TRUE(last_sql_.empty());
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, ExistingNodePreservesInMemoryLastStartTime)
{
  static const int64_t RUNTIME_LAST_START = 9999;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  node->last_start_time_ = RUNTIME_LAST_START;

  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, 90000UL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_EQ(RUNTIME_LAST_START, node->last_start_time_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, ActivateAssignsGroupToLoadedNode)
{
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_info(*proxy_, TEST_MVIEW_ID));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(OB_INVALID_ID, node->group_id_);

  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.activate_mview_sched_info(*proxy_,
                                           TEST_MVIEW_ID,
                                           updated_groups));
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(TEST_MVIEW_ID, updated_groups.at(0).group_id_);
  EXPECT_EQ(TEST_MVIEW_ID, node->group_id_);
  ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, group_info);
  EXPECT_EQ(group_info->modified_ts_, updated_groups.at(0).modified_ts_);
  ASSERT_EQ(1, group_info->top_mview_ids_.count());
  EXPECT_EQ(TEST_MVIEW_ID, group_info->top_mview_ids_.at(0));

  ObSEArray<ObMVGroupKey, 4> retry_updated_groups;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.activate_mview_sched_info(*proxy_,
                                           TEST_MVIEW_ID,
                                           retry_updated_groups));
  ASSERT_EQ(1, retry_updated_groups.count());
  EXPECT_EQ(TEST_MVIEW_ID, retry_updated_groups.at(0).group_id_);
  EXPECT_EQ(TEST_MVIEW_ID, node->group_id_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, RegisterExistingNodeIsIdempotent)
{
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_info(*proxy_, TEST_MVIEW_ID));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  node->group_id_ = TEST_MVIEW_ID;

  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_EQ(TEST_MVIEW_ID, node->group_id_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, ActivateMissingNodeReturnsHashNotExist)
{
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  EXPECT_EQ(OB_HASH_NOT_EXIST,
            mgr_.activate_mview_sched_info(*proxy_,
                                           TEST_MVIEW_ID,
                                           updated_groups));
  EXPECT_EQ(0, updated_groups.count());
  EXPECT_EQ(nullptr, mgr_.get_sched_info(TEST_MVIEW_ID));
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadFailureRemovesNodeAndRetryCanActivate)
{
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  fail_dep_read_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr_.load_mview_sched_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_EQ(nullptr, mgr_.get_sched_info(TEST_MVIEW_ID));

  fail_dep_read_ = false;
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_info(*proxy_, TEST_MVIEW_ID));
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.activate_mview_sched_info(*proxy_,
                                           TEST_MVIEW_ID,
                                           updated_groups));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(TEST_MVIEW_ID, updated_groups.at(0).group_id_);
  EXPECT_EQ(TEST_MVIEW_ID, node->group_id_);
  ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, group_info);
  ASSERT_EQ(1, group_info->top_mview_ids_.count());
  EXPECT_EQ(TEST_MVIEW_ID, group_info->top_mview_ids_.at(0));
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, ExistingNodeReloadsPersistedRefreshInterval)
{
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);

  static const int64_t NEW_REFRESH_INTERVAL = 60LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS,
            load_one_row(TEST_MVIEW_ID,
                         TEST_SCN_VAL,
                         NEW_REFRESH_INTERVAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_EQ(NEW_REFRESH_INTERVAL, node->specified_refresh_interval_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, ExistingNodeReloadsPeriodicRefreshStart)
{
  static const int64_t EFFECTIVE_REFRESH_INTERVAL = 6LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  node->last_start_time_ = 15LL * 1000 * 1000;
  node->refresh_interval_ = EFFECTIVE_REFRESH_INTERVAL;

  static const int64_t NEW_REFRESH_START = 30LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS,
            load_one_row(TEST_MVIEW_ID,
                         TEST_SCN_VAL,
                         TEST_REFRESH_INTERVAL,
                         NEW_REFRESH_START));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_EQ(15LL * 1000 * 1000, node->last_start_time_);
  EXPECT_EQ(NEW_REFRESH_START, node->get_refresh_start());
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, get_next_trigger_ts(node->mview_id_, next_trigger_ts));
  EXPECT_EQ(NEW_REFRESH_START, next_trigger_ts);

  node->last_start_time_ = 40LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_EQ(40LL * 1000 * 1000, node->last_start_time_);
  EXPECT_EQ(NEW_REFRESH_START, node->get_refresh_start());
  ASSERT_EQ(OB_SUCCESS, get_next_trigger_ts(node->mview_id_, next_trigger_ts));
  EXPECT_EQ(40LL * 1000 * 1000 + EFFECTIVE_REFRESH_INTERVAL, next_trigger_ts);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, ClearAndReloadRestoresLastStartTime)
{
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ASSERT_EQ(OB_SUCCESS, mgr_.clear_sched_info_map());
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  const ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(ObMViewSchedInfoMgrLoadBaseInfoTest::scn_to_last_start_time_us(TEST_SCN_VAL),
            node->last_start_time_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, InitSchedInfoReplacesNodeAndGroupState)
{
  const uint64_t old_mview_id = TEST_MVIEW_ID + 1;
  ObMViewSchedInfo *old_node = OB_NEWx(ObMViewSchedInfo, &mgr_.alloc_, mgr_.alloc_);
  ASSERT_NE(nullptr, old_node);
  old_node->mview_id_ = old_mview_id;
  old_node->group_id_ = old_mview_id;
  old_node->specified_refresh_interval_ = TEST_REFRESH_INTERVAL;
  ASSERT_EQ(OB_SUCCESS, mgr_.info_map_.set_refactored(old_mview_id, old_node));
  ObSEArray<uint64_t, 1> old_top_mview_ids;
  ObSEArray<uint64_t, 1> old_all_mview_ids;
  ObMVGroupKey group_key;
  ASSERT_EQ(OB_SUCCESS, old_top_mview_ids.push_back(old_mview_id));
  ASSERT_EQ(OB_SUCCESS, old_all_mview_ids.push_back(old_mview_id));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.build_and_publish_sched_group(old_mview_id,
                                                old_top_mview_ids,
                                                old_all_mview_ids,
                                                group_key));
  ASSERT_NE(nullptr, mgr_.get_sched_group_info(old_mview_id));
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));

  ASSERT_EQ(OB_SUCCESS, mgr_.init_sched_info(*proxy_));
  EXPECT_EQ(nullptr, mgr_.get_sched_info(old_mview_id));
  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(old_mview_id));
  ObMViewSchedInfo *new_node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ObMViewSchedGroupInfo *new_group = mgr_.get_sched_group_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, new_node);
  ASSERT_NE(nullptr, new_group);
  EXPECT_EQ(TEST_MVIEW_ID, new_node->group_id_);
  EXPECT_EQ(TEST_REFRESH_INTERVAL, new_node->refresh_interval_);
  EXPECT_EQ(TEST_REFRESH_INTERVAL, new_group->refresh_interval_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadBaseInfoDatabaseFilterRecordsLoadedIds)
{
  static const uint64_t DATABASE_ID = 3001UL;
  ObSEArray<uint64_t, 4> loaded_mview_ids;
  ASSERT_EQ(OB_SUCCESS, loaded_mview_ids.push_back(999UL));
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.load_mview_sched_base_info(*proxy_,
                                            OB_INVALID_ID,
                                            DATABASE_ID,
                                            &loaded_mview_ids));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), " WHERE t.database_id = 3001"));
  EXPECT_EQ(nullptr, strstr(last_sql_.ptr(), "WHERE a.mview_id"));
  EXPECT_EQ(nullptr, strstr(last_sql_.ptr(), "AND mview_id ="));
  ASSERT_EQ(1, loaded_mview_ids.count());
  EXPECT_EQ(TEST_MVIEW_ID, loaded_mview_ids.at(0));
  ASSERT_NE(nullptr, mgr_.get_sched_info(TEST_MVIEW_ID));
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadBaseInfoMviewAndDatabaseFiltersChainWithAnd)
{
  static const uint64_t DATABASE_ID = 3001UL;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_, TEST_MVIEW_ID, DATABASE_ID));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), " AND mview_id = 500001 GROUP BY mview_id"));
  EXPECT_NE(nullptr,
            strstr(last_sql_.ptr(), " WHERE a.mview_id = 500001 AND t.database_id = 3001"));
  ASSERT_NE(nullptr, mgr_.get_sched_info(TEST_MVIEW_ID));

  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), " WHERE a.mview_id = 500001"));
  EXPECT_EQ(nullptr, strstr(last_sql_.ptr(), "database_id"));
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadBaseInfoRecordsExistingNodeInLoadedIds)
{
  static const int64_t RUNTIME_LAST_START = 777LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_base_info(*proxy_));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  node->last_start_time_ = RUNTIME_LAST_START;

  ObSEArray<uint64_t, 4> loaded_mview_ids;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, 90000UL));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.load_mview_sched_base_info(*proxy_,
                                            OB_INVALID_ID,
                                            OB_INVALID_ID,
                                            &loaded_mview_ids));
  ASSERT_EQ(1, loaded_mview_ids.count());
  EXPECT_EQ(TEST_MVIEW_ID, loaded_mview_ids.at(0));
  EXPECT_EQ(node, mgr_.get_sched_info(TEST_MVIEW_ID));
  EXPECT_EQ(RUNTIME_LAST_START, node->last_start_time_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadDepInfoDatabaseFilterJoinsMviewTable)
{
  static const uint64_t DATABASE_ID = 3001UL;
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_dep_info(*proxy_, OB_INVALID_ID, DATABASE_ID));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "dep.p_obj AS dep_mview_id"));
  EXPECT_NE(nullptr,
            strstr(last_sql_.ptr(), "ON dep.mview_id = mv.table_id AND mv.database_id = 3001"));
  EXPECT_EQ(nullptr, strstr(last_sql_.ptr(), "WHERE dep.mview_id"));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), " ORDER BY dep.mview_id"));

  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_dep_info(*proxy_, TEST_MVIEW_ID, DATABASE_ID));
  EXPECT_NE(nullptr,
            strstr(last_sql_.ptr(),
                   "mv.database_id = 3001 WHERE dep.mview_id = 500001 ORDER BY dep.mview_id"));

  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_dep_info(*proxy_, TEST_MVIEW_ID));
  EXPECT_EQ(nullptr, strstr(last_sql_.ptr(), "database_id"));
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), " WHERE dep.mview_id = 500001 ORDER BY dep.mview_id"));
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadAndActivateDatabasePublishesLoadedGroup)
{
  static const uint64_t DATABASE_ID = 3001UL;
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, updated_groups.push_back(ObMVGroupKey(999UL, 1)));
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.load_and_activate_mview_sched_infos(*proxy_, DATABASE_ID, updated_groups));
  // the dep query is issued last and must carry the database filter as well
  EXPECT_NE(nullptr, strstr(last_sql_.ptr(), "mv.database_id = 3001"));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(TEST_MVIEW_ID, node->group_id_);
  EXPECT_EQ(TEST_REFRESH_INTERVAL, node->refresh_interval_);
  const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, group_info);
  ASSERT_EQ(1, group_info->top_mview_ids_.count());
  EXPECT_EQ(TEST_MVIEW_ID, group_info->top_mview_ids_.at(0));
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(TEST_MVIEW_ID, updated_groups.at(0).group_id_);
  EXPECT_EQ(group_info->modified_ts_, updated_groups.at(0).modified_ts_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadAndActivateEmptyDatabaseReturnsNoGroups)
{
  static const uint64_t DATABASE_ID = 3001UL;
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, updated_groups.push_back(ObMVGroupKey(999UL, 1)));
  fake_->clear_rows();
  ASSERT_EQ(OB_SUCCESS,
            mgr_.load_and_activate_mview_sched_infos(*proxy_, DATABASE_ID, updated_groups));
  EXPECT_EQ(0, updated_groups.count());
  EXPECT_EQ(0, mgr_.info_map_.size());
  EXPECT_EQ(0, mgr_.group_info_map_.size());
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadAndActivateMultipleRowsPublishesEachGroup)
{
  static const uint64_t DATABASE_ID = 3001UL;
  const uint64_t second_mview_id = TEST_MVIEW_ID + 1;
  const FakeSchedBaseInfoRow rows[] = {
    {TEST_MVIEW_ID, TEST_SCN_VAL, 0, TEST_REFRESH_INTERVAL, OB_INVALID_TIMESTAMP, TEST_MVIEW_CREATE_TIME},
    {second_mview_id, TEST_SCN_VAL, 0, TEST_REFRESH_INTERVAL, OB_INVALID_TIMESTAMP, TEST_MVIEW_CREATE_TIME},
  };
  ASSERT_EQ(OB_SUCCESS, fake_->load_rows(rows, 2));
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.load_and_activate_mview_sched_infos(*proxy_, DATABASE_ID, updated_groups));
  ASSERT_EQ(2, updated_groups.count());
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(updated_groups, TEST_MVIEW_ID));
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(updated_groups, second_mview_id));
  for (int64_t i = 0; i < updated_groups.count(); ++i) {
    const ObMVGroupKey &group_key = updated_groups.at(i);
    const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(group_key.group_id_);
    ASSERT_NE(nullptr, group_info);
    EXPECT_EQ(group_info->modified_ts_, group_key.modified_ts_);
  }
  ASSERT_NE(nullptr, mgr_.get_sched_info(TEST_MVIEW_ID));
  ASSERT_NE(nullptr, mgr_.get_sched_info(second_mview_id));
  EXPECT_EQ(TEST_MVIEW_ID, mgr_.get_sched_info(TEST_MVIEW_ID)->group_id_);
  EXPECT_EQ(second_mview_id, mgr_.get_sched_info(second_mview_id)->group_id_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadAndActivateReactivatesRegisteredNode)
{
  static const uint64_t DATABASE_ID = 3001UL;
  static const int64_t RUNTIME_LAST_START = 777LL * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  ASSERT_EQ(OB_SUCCESS, mgr_.load_mview_sched_info(*proxy_, TEST_MVIEW_ID));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(OB_INVALID_ID, node->group_id_);
  node->last_start_time_ = RUNTIME_LAST_START;

  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, 90000UL));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.load_and_activate_mview_sched_infos(*proxy_, DATABASE_ID, updated_groups));
  EXPECT_EQ(node, mgr_.get_sched_info(TEST_MVIEW_ID));
  EXPECT_EQ(RUNTIME_LAST_START, node->last_start_time_);
  EXPECT_EQ(TEST_MVIEW_ID, node->group_id_);
  ASSERT_NE(nullptr, mgr_.get_sched_group_info(TEST_MVIEW_ID));
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(TEST_MVIEW_ID, updated_groups.at(0).group_id_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadAndActivateFailureRemovesLoadedNodesAndRetrySucceeds)
{
  static const uint64_t DATABASE_ID = 3001UL;
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, load_one_row(TEST_MVIEW_ID, TEST_SCN_VAL));
  fail_dep_read_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            mgr_.load_and_activate_mview_sched_infos(*proxy_, DATABASE_ID, updated_groups));
  EXPECT_EQ(0, updated_groups.count());
  EXPECT_EQ(nullptr, mgr_.get_sched_info(TEST_MVIEW_ID));
  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(TEST_MVIEW_ID));

  fail_dep_read_ = false;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.load_and_activate_mview_sched_infos(*proxy_, DATABASE_ID, updated_groups));
  ObMViewSchedInfo *node = mgr_.get_sched_info(TEST_MVIEW_ID);
  ASSERT_NE(nullptr, node);
  EXPECT_EQ(TEST_MVIEW_ID, node->group_id_);
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(TEST_MVIEW_ID, updated_groups.at(0).group_id_);
}

TEST_F(ObMViewSchedInfoMgrLoadBaseInfoTest, LoadAndActivateBeforeInitReturnsNotInit)
{
  static const uint64_t DATABASE_ID = 3001UL;
  ObMViewSchedInfoMgr uninited_mgr;
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, updated_groups.push_back(ObMVGroupKey(999UL, 1)));
  EXPECT_EQ(OB_NOT_INIT,
            uninited_mgr.load_and_activate_mview_sched_infos(*proxy_, DATABASE_ID, updated_groups));
  EXPECT_EQ(0, updated_groups.count());
}

TEST(ObMVGroupKeyTest, DefaultConstructAndResetAreInvalid)
{
  ObMVGroupKey default_key;
  EXPECT_EQ(OB_INVALID_ID, default_key.group_id_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, default_key.modified_ts_);

  ObMVGroupKey group_key(10UL, 100);
  EXPECT_EQ(10UL, group_key.group_id_);
  EXPECT_EQ(100, group_key.modified_ts_);
  group_key.reset();
  EXPECT_EQ(OB_INVALID_ID, group_key.group_id_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, group_key.modified_ts_);
}

TEST(ObMVGroupKeyTest, ContainsGroupIdMatchesOnGroupIdOnly)
{
  ObSEArray<ObMVGroupKey, 4> group_keys;
  EXPECT_FALSE(ObMVGroupKey::contains_group_id(group_keys, 10UL));
  ASSERT_EQ(OB_SUCCESS, group_keys.push_back(ObMVGroupKey(10UL, 100)));
  ASSERT_EQ(OB_SUCCESS, group_keys.push_back(ObMVGroupKey(20UL, 200)));
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(group_keys, 10UL));
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(group_keys, 20UL));
  EXPECT_FALSE(ObMVGroupKey::contains_group_id(group_keys, 30UL));
  EXPECT_FALSE(ObMVGroupKey::contains_group_id(group_keys, OB_INVALID_ID));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const int64_t  REFRESH_ID = 100L;

// Build a basic task. Dependencies now live in ObMViewSchedInfoMgr; call
// ObMViewPendingTaskQueueTest::add_sched_info before pushing so fetch/deps
// checks can resolve them.
static ObMViewPendingTask make_task(uint64_t mview_id, int64_t flags = 0);

// Build a task with an explicit status (for reload-from-DB scenarios).
static ObMViewPendingTask make_task_with_status(uint64_t mview_id,
                                                ObMViewTaskStatus status,
                                                int64_t  flags = 0)
{
  ObMViewPendingTask t = make_task(mview_id, flags);
  t.status_ = status;
  return t;
}

static ObMViewPendingTask make_task(uint64_t mview_id, int64_t flags)
{
  ObMViewPendingTask t;
  t.mview_id_             = mview_id;
  t.status_               = MV_TASK_PENDING;
  t.skip_cnt_             = 0;
  t.retry_count_          = 0;
  t.next_retry_ts_        = 0;
  t.flags_                = flags;
  return t;
}

static const uint64_t TEST_TARGET_SCN = 0;

// Build a group and insert it as published (reload/restore path).
static int enqueue_tasks(ObMViewPendingTaskQueue &q,
                         int64_t refresh_id,
                         uint64_t target_scn,
                         const ObIArray<ObMViewPendingTask> &tasks)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc;
  ObMViewPendingTaskGroup *group = nullptr;
  ObMViewPendingTaskGroup::GroupInfo info;
  info.tenant_id_ = TENANT_ID;
  info.refresh_id_ = refresh_id;
  info.target_data_sync_scn_ = target_scn;
  if (OB_FAIL(ObMViewPendingTaskGroup::alloc_and_build(alloc, info, tasks, group))) {
    LOG_WARN("alloc and build pending task group failed", K(ret));
  } else if (OB_FAIL(q.restore_pending_task_group(*group))) {
    LOG_WARN("restore pending task group failed", K(ret));
  }
  return ret;
}

// Insert an unpublished reservation. Unit tests cannot call reserve_pending_task_group()
// because it needs MTL(ObTransService) for SCN; private enqueue(publish=false) covers the
// barrier semantics of an in-memory reservation.
static int reserve_tasks(ObMViewPendingTaskQueue &q,
                         int64_t refresh_id,
                         uint64_t target_scn,
                         const ObIArray<ObMViewPendingTask> &tasks)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc;
  ObMViewPendingTaskGroup *group = nullptr;
  ObMViewPendingTaskGroup::GroupInfo info;
  info.tenant_id_ = TENANT_ID;
  info.refresh_id_ = refresh_id;
  info.target_data_sync_scn_ = target_scn;
  if (OB_FAIL(ObMViewPendingTaskGroup::alloc_and_build(alloc, info, tasks, group))) {
    LOG_WARN("alloc and build pending task group failed", K(ret));
  } else {
    ObLatchWGuard guard(q.rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
    if (OB_FAIL(q.enqueue_pending_task_group(*group, false /*publish_group*/))) {
      LOG_WARN("reserve pending task group failed", K(ret));
    }
  }
  return ret;
}

static int push_one(ObMViewPendingTaskQueue &q,
                    ObMViewPendingTask &t,
                    uint64_t target_scn = TEST_TARGET_SCN,
                    int64_t refresh_id = REFRESH_ID)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMViewPendingTask, 1> tasks;
  if (OB_FAIL(tasks.push_back(t))) {
    LOG_WARN("push back failed", K(ret));
  } else {
    ret = enqueue_tasks(q, refresh_id, target_scn, tasks);
  }
  return ret;
}

template <typename... Tasks>
static int push_many(ObMViewPendingTaskQueue &q, Tasks&... tasks)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMViewPendingTask, 8> arr;
  ObMViewPendingTask *ptrs[] = {&tasks...};
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(sizeof...(tasks)); ++i) {
    if (OB_FAIL(arr.push_back(*ptrs[i]))) {
      LOG_WARN("push back failed", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    ret = enqueue_tasks(q, REFRESH_ID, TEST_TARGET_SCN, arr);
  }
  return ret;
}

template <typename... Tasks>
static int push_many_with_group(ObMViewPendingTaskQueue &q,
                                uint64_t target_scn,
                                Tasks&... tasks)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMViewPendingTask, 8> arr;
  ObMViewPendingTask *ptrs[] = {&tasks...};
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(sizeof...(tasks)); ++i) {
    if (OB_FAIL(arr.push_back(*ptrs[i]))) {
      LOG_WARN("push back failed", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    ret = enqueue_tasks(q, REFRESH_ID, target_scn, arr);
  }
  return ret;
}

// Compose candidate discovery and the subsequent queue state transition for
// lifecycle tests that exercise the scheduler-side sequence.
static int test_fetch_next_task(ObMViewPendingTaskQueue &q,
                                ObMViewPendingTaskGroup::GroupInfo &info,
                                ObMViewPendingTask &out);

static int test_fetch_next_task(ObMViewPendingTaskQueue &q, ObMViewPendingTask &out)
{
  int ret = OB_SUCCESS;
  ObMViewPendingTaskGroup::GroupInfo info;
  ret = test_fetch_next_task(q, info, out);
  return ret;
}

static int test_fetch_next_task(ObMViewPendingTaskQueue &q,
                                ObMViewPendingTaskGroup::GroupInfo &info,
                                ObMViewPendingTask &out)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> refresh_ids;
  if (OB_FAIL(q.find_next_ready_task(refresh_ids, info, out))) {
  } else if (!out.is_valid()) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(q.set_task_running(info.tenant_id_, info.refresh_id_, out.mview_id_))) {
  } else {
    out.status_ = MV_TASK_RUNNING;
  }
  return ret;
}

static bool has_running_in_group(const ObMViewPendingTaskGroup &group)
{
  for (int64_t i = 0; i < group.tasks_.count(); ++i) {
    const ObMViewPendingTask &task = group.tasks_.at(i);
    if (MV_TASK_RUNNING == task.status_) {
      return true;
    }
  }
  return false;
}

static bool has_failed_in_group(const ObMViewPendingTaskGroup &group)
{
  for (int64_t i = 0; i < group.tasks_.count(); ++i) {
    const ObMViewPendingTask &task = group.tasks_.at(i);
    if (MV_TASK_FAILED == task.status_) {
      return true;
    }
  }
  return false;
}

static int get_group(const ObMViewPendingTaskQueue &queue,
                               uint64_t tenant_id,
                               int64_t refresh_id,
                               const ObMViewPendingTaskGroup *&group)
{
  int ret = OB_SUCCESS;
  ObMViewPendingTaskGroup *tmp = nullptr;
  const ObMViewRefreshKey refresh_key(tenant_id, refresh_id);
  if (OB_FAIL(const_cast<ObMViewPendingTaskQueue &>(queue)
                  .group_map_.get_refactored(refresh_key, tmp))) {
  } else {
    group = tmp;
  }
  return ret;
}

struct RecycleCountRecorder
{
  int64_t *count_;
  uint64_t *first_mview_id_;
  explicit RecycleCountRecorder(int64_t *count, uint64_t *first_mview_id = NULL)
    : count_(count), first_mview_id_(first_mview_id) {}
  int operator()(const ObMViewPendingTask *task)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL != first_mview_id_ && (NULL == count_ || 0 == *count_)) {
      *first_mview_id_ = task->mview_id_;
    }
    if (OB_SUCC(ret) && NULL != count_) {
      ++(*count_);
    }
    return ret;
  }
};

// Derive the refresh-level aggregated status from the group snapshot. Rank
// order: RUNNING > FAILED > SUCCESS (all done, no failure) > PENDING (still
// non-terminal work). Kept in the test file only — production no longer needs
// a refresh-wide rollup.
static int get_refresh_status(const ObMViewPendingTaskQueue &queue,
                                       uint64_t tenant_id,
                                       int64_t refresh_id,
                                       ObMViewTaskStatus &status)
{
  int ret = OB_SUCCESS;
  const ObMViewPendingTaskGroup *group = nullptr;
  if (OB_FAIL(get_group(queue, tenant_id, refresh_id, group))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_ISNULL(group)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool has_running = false;
    bool has_failed = false;
    for (int64_t i = 0; i < group->tasks_.count(); ++i) {
      const ObMViewPendingTask &task = group->tasks_.at(i);
      if (MV_TASK_RUNNING == task.status_) {
        has_running = true;
      } else if (MV_TASK_FAILED == task.status_) {
        has_failed = true;
      }
    }
    if (has_running) {
      status = MV_TASK_RUNNING;
    } else if (has_failed) {
      status = MV_TASK_FAILED;
    } else if (group->is_refresh_finished()) {
      status = MV_TASK_SUCCESS;
    } else {
      status = MV_TASK_PENDING;
    }
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
  static constexpr uint64_t ROOT_MV = 1003UL; // root mview, depends on MID_MV

  ObMViewPendingTaskQueueTest() : sched_info_mgr_(), queue_(sched_info_mgr_) {}

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.init(TENANT_ID, 64L));
    ASSERT_EQ(OB_SUCCESS, queue_.init(TENANT_ID));
  }

  void TearDown() override
  {
    // destroy is idempotent for the purposes of cleanup
    queue_.destroy();
    sched_info_mgr_.destroy();
  }

  // Seed ObMViewSchedInfoMgr with a synthetic node for `mv`. Every mview
  // pushed into the queue in a test must have a matching entry here so
  // find_next_ready_task -> check_mview_deps_status can resolve its deps. Deps must
  // themselves be pre-registered (two-phase build invariant).
  int add_sched_info(uint64_t mv,
                     const uint64_t *dep_arr = nullptr,
                     int64_t dep_cnt = 0)
  {
    int ret = OB_SUCCESS;
    ObMViewSchedInfo *node = OB_NEWx(ObMViewSchedInfo, &sched_info_mgr_.alloc_,
                                     sched_info_mgr_.alloc_);
    if (OB_ISNULL(node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      node->mview_id_ = mv;
      node->group_id_ = mv;
      node->last_start_time_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < dep_cnt; ++i) {
        ObMViewSchedInfo *dep_node = sched_info_mgr_.get_sched_info(dep_arr[i]);
        if (OB_ISNULL(dep_node)) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          ret = node->dep_mview_ids_.push_back(dep_arr[i]);
        }
      }
      if (OB_SUCC(ret)) {
        ret = sched_info_mgr_.info_map_.set_refactored(mv, node);
      }
    }
    return ret;
  }

  // Snapshot the group stats for (tenant, refresh_id) into `ctx`. Only
  // scalar/stat fields are copied (tasks_ is intentionally skipped by the
  // test-only accessor).
  void get_ctx(ObMViewPendingTaskGroup &ctx, int64_t refresh_id = REFRESH_ID)
  {
    const ObMViewPendingTaskGroup *ptr = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, refresh_id, ptr));
    ASSERT_TRUE(OB_NOT_NULL(ptr));
    ctx.info_                  = ptr->info_;
    ctx.first_unfinished_pos_  = ptr->first_unfinished_pos_;
  }

  // Transition helper: seed sched_info for mview_id if not yet present, push
  // a task, then mark it RUNNING.
  int push_and_run(uint64_t mview_id, int64_t flags = 0)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(sched_info_mgr_.get_sched_info(mview_id))
        && OB_FAIL(add_sched_info(mview_id))) {
    } else {
      ObMViewPendingTask t = make_task(mview_id, flags);
      if (OB_FAIL(push_one(queue_, t))) {
      } else if (OB_FAIL(queue_.set_task_running(TENANT_ID, REFRESH_ID, mview_id))) {
      }
    }
    return ret;
  }

  ObMViewSchedInfoMgr sched_info_mgr_;
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
// 2. Push / Fetch
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, FindNextReadyTaskEmptyQueueReturnsInvalidOutput)
{
  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_FALSE(info.is_valid());
  EXPECT_FALSE(out.is_valid());
  EXPECT_TRUE(refresh_ids.empty());
}

TEST_F(ObMViewPendingTaskQueueTest, FindNextReadyTaskDoesNotClaimTask)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_EQ(TENANT_ID,  info.tenant_id_);
  EXPECT_EQ(REFRESH_ID, info.refresh_id_);
  EXPECT_EQ(LEAF_MV,    out.mview_id_);
  EXPECT_EQ(MV_TASK_PENDING, out.status_);
  EXPECT_EQ(0, queue_.get_total_running_cnt());
  EXPECT_TRUE(refresh_ids.empty());

  ASSERT_EQ(OB_SUCCESS,
            queue_.find_next_ready_task(refresh_ids, info, out));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  EXPECT_EQ(1, queue_.get_total_running_cnt());
  EXPECT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_FALSE(info.is_valid());
  EXPECT_FALSE(out.is_valid());
}

TEST_F(ObMViewPendingTaskQueueTest, CancelAfterFindMarksPendingTaskCancelled)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask pending_task = make_task(LEAF_MV, 0);
  ObMViewPendingTask found_task;
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, pending_task));
  ObMViewPendingTaskGroup::GroupInfo info;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, found_task));

  bool refresh_finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.cancel_tasks_by_refresh_id(TENANT_ID, REFRESH_ID, refresh_finished));
  EXPECT_TRUE(refresh_finished);
  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_CANCELLED, status);
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskQueueTest, FindReportsFinishedGroupAlongsideReadyTask)
{
  const int64_t next_refresh_id = REFRESH_ID + 1;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask first_task = make_task(LEAF_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, first_task, TEST_TARGET_SCN, REFRESH_ID));
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  bool refresh_finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, refresh_finished));
  ASSERT_TRUE(refresh_finished);

  ObMViewPendingTask next_task = make_task(LEAF_MV);
  ASSERT_EQ(OB_SUCCESS,
            push_one(queue_, next_task, TEST_TARGET_SCN, next_refresh_id));

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  ASSERT_EQ(1, refresh_ids.count());
  EXPECT_EQ(REFRESH_ID, refresh_ids.at(0));
  EXPECT_EQ(next_refresh_id, info.refresh_id_);
  EXPECT_EQ(LEAF_MV, out.mview_id_);
  EXPECT_EQ(MV_TASK_PENDING, out.status_);
}

TEST_F(ObMViewPendingTaskQueueTest, FetchWithoutSchedInfoFails)
{
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_FALSE(info.is_valid());
  EXPECT_FALSE(out.is_valid());
  ASSERT_EQ(1, refresh_ids.count());
  EXPECT_EQ(REFRESH_ID, refresh_ids.at(0));
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskQueueTest, MissingSchedInfoDoesNotBlockLaterRefresh)
{
  const int64_t next_refresh_id = REFRESH_ID + 1;
  ObMViewPendingTask missing = make_task_with_status(LEAF_MV, MV_TASK_RETRY_WAIT);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, missing));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV));
  ObMViewPendingTask ready = make_task(ROOT_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, ready, TEST_TARGET_SCN, next_refresh_id));

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  ASSERT_EQ(1, refresh_ids.count());
  EXPECT_EQ(REFRESH_ID, refresh_ids.at(0));
  EXPECT_EQ(next_refresh_id, info.refresh_id_);
  EXPECT_EQ(ROOT_MV, out.mview_id_);
  EXPECT_EQ(MV_TASK_PENDING, out.status_);
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskQueueTest, GroupLookupErrorDoesNotBlockLaterRefresh)
{
  const int64_t failed_refresh_id = REFRESH_ID + 1;
  const int64_t next_refresh_id = REFRESH_ID + 2;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV));
  ObMViewPendingTask previous = make_task_with_status(LEAF_MV, MV_TASK_SUCCESS);
  ObMViewPendingTask missing = make_task_with_status(MID_MV, MV_TASK_RUNNING);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, previous, missing));
  ObMViewPendingTask blocked = make_task(LEAF_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, blocked, TEST_TARGET_SCN, failed_refresh_id));
  ObMViewPendingTask ready = make_task(ROOT_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, ready, TEST_TARGET_SCN, next_refresh_id));

  const ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, failed_refresh_id, group));
  ASSERT_NE(nullptr, group);
  ObMViewPendingTask out;
  bool need_cancel_group = false;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
            queue_.find_first_ready_task_in_group(*group, out, need_cancel_group));

  ObMViewPendingTaskGroup::GroupInfo info;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  ASSERT_EQ(1, refresh_ids.count());
  EXPECT_EQ(failed_refresh_id, refresh_ids.at(0));
  EXPECT_EQ(next_refresh_id, info.refresh_id_);
  EXPECT_EQ(ROOT_MV, out.mview_id_);
  EXPECT_EQ(1, queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskQueueTest, FetchSkipsNonPendingTasks)
{
  // Push one task and put it into RUNNING state – it should no longer be fetchable.
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));
}

TEST_F(ObMViewPendingTaskQueueTest, CollectRunningJobsOnlyReturnsRunningTasks)
{
  static const uint64_t RETRY_MV = 1101UL;
  static const uint64_t SUCCESS_MV = 1102UL;
  static const uint64_t PENDING_MV = 1103UL;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(RETRY_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(SUCCESS_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(PENDING_MV));
  const uint64_t target_scn = 123456UL;
  const int64_t refresh_parallel = 8;
  bool finished = false;
  ObMViewPendingTask running_task = make_task(LEAF_MV);
  ObMViewPendingTask retry_task = make_task(RETRY_MV);
  ObMViewPendingTask success_task = make_task(SUCCESS_MV);
  ObMViewPendingTask pending_task = make_task(PENDING_MV, 0);
  running_task.refresh_method_ = share::schema::ObMVRefreshMethod::FAST;
  running_task.refresh_parallel_ = refresh_parallel;
  ASSERT_EQ(OB_SUCCESS,
            push_many_with_group(queue_, target_scn,
                                 running_task, retry_task, success_task, pending_task));
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
  EXPECT_GT(running_job_infos.at(0).start_time_, 0);
}

// ---------------------------------------------------------------------------
// 3. Refresh status
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RefreshStatusPendingAfterPush)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
}

TEST_F(ObMViewPendingTaskQueueTest, RefreshStatusRunningWhenTaskRunning)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
}

TEST_F(ObMViewPendingTaskQueueTest, RefreshStatusSuccessAfterSingleTaskSucceeds)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_TRUE(finished);

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_SUCCESS, status);
}

// ---------------------------------------------------------------------------
// 4. Dependency blocking in fetch
// ---------------------------------------------------------------------------

// Topology: LEAF (no deps) --> ROOT (depends on LEAF)
// Only LEAF should be fetchable until LEAF is SUCCESS.
TEST_F(ObMViewPendingTaskQueueTest, DependencyBlocksFetch)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  const uint64_t root_deps[] = { LEAF_MV };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));
  ObMViewPendingTask leaf = make_task(LEAF_MV);
  ObMViewPendingTask root = make_task(ROOT_MV, 0);

  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, root));

  // Fetch must return LEAF (its deps are empty, so it is ready) and mark it RUNNING.
  ObMViewPendingTask out;
  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(LEAF_MV, out.mview_id_);

  // Root is still not fetchable while LEAF is RUNNING (dep not yet SUCCESS).
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));

  // LEAF succeeds; now ROOT should become fetchable
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished); // root still pending

  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(ROOT_MV, out.mview_id_);
}

// ---------------------------------------------------------------------------
// 5. Task failure does not auto-cascade; cancel_tasks_by_refresh_id is explicit
// ---------------------------------------------------------------------------

// Topology (deepest dep first):
//   LEAF (no deps) -> MID (depends on LEAF) -> ROOT (depends on MID, root)
// When LEAF fails (after running), MID and ROOT stay PENDING until
// cancel_tasks_by_refresh_id marks them CANCELLED; then refresh_finished is true.
TEST_F(ObMViewPendingTaskQueueTest, CancelAllAfterFailureCancelsDependents)
{
  const uint64_t mid_deps[]  = { LEAF_MV };
  const uint64_t root_deps[] = { MID_MV  };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(MID_MV, mid_deps, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));

  ObMViewPendingTask leaf = make_task(LEAF_MV);
  ObMViewPendingTask mid  = make_task(MID_MV, 0);
  ObMViewPendingTask root = make_task(ROOT_MV, 0);

  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, mid, root));

  // Run LEAF then fail it — mid and root are still PENDING so refresh is not done yet
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_failed(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  // Cascade-cancel the remaining PENDING tasks (MID, ROOT)
  ASSERT_EQ(OB_SUCCESS,
            queue_.cancel_tasks_by_refresh_id(TENANT_ID, REFRESH_ID, finished));
  // All three tasks are now terminal (LEAF=failed, MID=cancelled, ROOT=cancelled)
  EXPECT_TRUE(finished);

  // Overall refresh status reflects failure
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_FAILED, status);

  // No more tasks are dispatchable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));
}

// ---------------------------------------------------------------------------
// 6. Retry-wait: task goes back to PENDING-equivalent (running_cnt drops)
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RetryWaitDecrementsRunningCount)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  // Status is RUNNING
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  // no running tasks, first unfinished task at pos 0 -> PENDING
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
}

// ---------------------------------------------------------------------------
// 8. recycle_refresh cleans up all tasks
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RecycleRefreshCleansUp)
{
  const uint64_t root_deps[] = { LEAF_MV };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));
  ObMViewPendingTask leaf_task = make_task(LEAF_MV);
  ObMViewPendingTask root_task = make_task(ROOT_MV, 0);
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
  EXPECT_NE(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));

  // Queue is now empty
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));
}

TEST_F(ObMViewPendingTaskQueueTest, RecycleRefreshDecrementsRunningCount)
{
  // Two RUNNING tasks and one PENDING task in the same refresh group.
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(MID_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV));
  ObMViewPendingTask leaf_task = make_task(LEAF_MV, 0);
  ObMViewPendingTask mid_task = make_task(MID_MV, 0);
  ObMViewPendingTask root_task = make_task(ROOT_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, mid_task, root_task));
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, MID_MV));
  EXPECT_EQ(2, queue_.get_total_running_cnt());

  // recycle_refresh frees the group directly; RUNNING tasks inside must not
  // leak into total_running_cnt_.
  ASSERT_EQ(OB_SUCCESS, queue_.recycle_refresh(TENANT_ID, REFRESH_ID));
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

// ---------------------------------------------------------------------------
// 9. Two independent refreshes do not interfere
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, PatchSessionIdsForRunningTasks)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  const ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
  ASSERT_EQ(1, group->tasks_.count());
  EXPECT_EQ(MV_TASK_RUNNING, group->tasks_.at(0).status_);
  EXPECT_EQ(0u, group->tasks_.at(0).session_id_);

  const ObMViewPendingTaskKey key(TENANT_ID, REFRESH_ID, LEAF_MV);
  ObMViewPendingTaskSessionIdEntry entry(key, 0, group->tasks_.at(0).seq_);
  entry.session_id_ = 42;
  ObSEArray<ObMViewPendingTaskSessionIdEntry, 1> entries;
  ASSERT_EQ(OB_SUCCESS, entries.push_back(entry));
  ASSERT_EQ(OB_SUCCESS, queue_.patch_session_ids_for_running_tasks(entries));

  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
  EXPECT_EQ(42u, group->tasks_.at(0).session_id_);
}

TEST_F(ObMViewPendingTaskQueueTest, FetchBlockedWhenPrevGroupHasNonTerminalDependent)
{
  const int64_t REFRESH_A = 100L;
  const int64_t REFRESH_B = 101L;
  const uint64_t root_deps[] = { LEAF_MV };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));

  ObMViewPendingTask leaf_a = make_task(LEAF_MV);
  ObMViewPendingTask root_a = make_task(ROOT_MV, 0);
  ObSEArray<ObMViewPendingTask, 2> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(leaf_a));
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(root_a));
  ASSERT_EQ(OB_SUCCESS,
            enqueue_tasks(queue_, REFRESH_A, TEST_TARGET_SCN, tasks_a));

  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_A, LEAF_MV));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_A, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  ObMViewPendingTask leaf_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(leaf_b));
  ASSERT_EQ(OB_SUCCESS,
            enqueue_tasks(queue_, REFRESH_B, TEST_TARGET_SCN, tasks_b));

  const ObMViewPendingTaskGroup *group_b = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_B, group_b));
  ASSERT_TRUE(OB_NOT_NULL(group_b));

  ObMViewPendingTask out;
  bool need_cancel_group = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.find_first_ready_task_in_group(*group_b, out, need_cancel_group));
  EXPECT_FALSE(out.is_valid());
  EXPECT_FALSE(need_cancel_group);
}

TEST_F(ObMViewPendingTaskQueueTest, FetchUnblockedWhenPrevGroupAllTerminal)
{
  const int64_t REFRESH_A = 102L;
  const int64_t REFRESH_B = 103L;
  const uint64_t root_deps[] = { LEAF_MV };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));

  ObMViewPendingTask leaf_a = make_task(LEAF_MV);
  ObMViewPendingTask root_a = make_task(ROOT_MV, 0);
  ObSEArray<ObMViewPendingTask, 2> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(leaf_a));
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(root_a));
  ASSERT_EQ(OB_SUCCESS,
            enqueue_tasks(queue_, REFRESH_A, TEST_TARGET_SCN, tasks_a));

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_A, LEAF_MV));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_A, LEAF_MV, finished));
  EXPECT_FALSE(finished);
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_A, ROOT_MV));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_A, ROOT_MV, finished));
  EXPECT_TRUE(finished);

  ObMViewPendingTask leaf_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(leaf_b));
  ASSERT_EQ(OB_SUCCESS,
            enqueue_tasks(queue_, REFRESH_B, TEST_TARGET_SCN, tasks_b));

  ObMViewPendingTask out;
  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(LEAF_MV, out.mview_id_);
}

// ---------------------------------------------------------------------------
// Publish barrier: unpublished reservation stops scanning later published groups
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, UnpublishedHeadBlocksLaterPublishedGroup)
{
  const int64_t REFRESH_A = 110L;
  const int64_t REFRESH_B = 111L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask task_a = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(task_a));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_A, 1, tasks_a));

  ObMViewPendingTask task_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(task_b));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_B, 2, tasks_b));

  const ObMViewPendingTaskGroup *group_a = nullptr;
  const ObMViewPendingTaskGroup *group_b = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_A, group_a));
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_B, group_b));
  ASSERT_TRUE(OB_NOT_NULL(group_a));
  ASSERT_TRUE(OB_NOT_NULL(group_b));
  EXPECT_FALSE(group_a->is_published());
  EXPECT_TRUE(group_b->is_published());

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_FALSE(info.is_valid());
  EXPECT_FALSE(out.is_valid());
  EXPECT_TRUE(refresh_ids.empty());
}

TEST_F(ObMViewPendingTaskQueueTest, PublishUnpublishedHeadUnblocksScan)
{
  const int64_t REFRESH_A = 112L;
  const int64_t REFRESH_B = 113L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask task_a = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(task_a));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_A, 1, tasks_a));

  ObMViewPendingTask task_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(task_b));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_B, 2, tasks_b));

  ASSERT_EQ(OB_SUCCESS, queue_.publish_pending_task_group(TENANT_ID, REFRESH_A));

  const ObMViewPendingTaskGroup *group_a = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_A, group_a));
  ASSERT_TRUE(OB_NOT_NULL(group_a));
  EXPECT_TRUE(group_a->is_published());

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_EQ(REFRESH_A, info.refresh_id_);
  EXPECT_EQ(LEAF_MV, out.mview_id_);
  EXPECT_EQ(MV_TASK_PENDING, out.status_);
}

TEST_F(ObMViewPendingTaskQueueTest, RecycleUnpublishedReservationUnblocksLaterGroup)
{
  const int64_t REFRESH_A = 114L;
  const int64_t REFRESH_B = 115L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask task_a = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(task_a));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_A, 1, tasks_a));

  ObMViewPendingTask task_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(task_b));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_B, 2, tasks_b));

  ASSERT_EQ(OB_SUCCESS, queue_.recycle_refresh(TENANT_ID, REFRESH_A));

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_EQ(REFRESH_B, info.refresh_id_);
  EXPECT_EQ(LEAF_MV, out.mview_id_);
  EXPECT_EQ(MV_TASK_PENDING, out.status_);
}

TEST_F(ObMViewPendingTaskQueueTest, RestoreMarksGroupPublished)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask task = make_task(LEAF_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, task));

  const ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
  EXPECT_TRUE(group->is_published());
}

TEST_F(ObMViewPendingTaskQueueTest, ReserveWithoutTransServiceDoesNotEnqueueGroup)
{
  ObArenaAllocator allocator;
  ObMViewPendingTask task = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks;
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
  ObMViewPendingTaskGroup::GroupInfo info;
  info.tenant_id_ = TENANT_ID;
  info.refresh_id_ = REFRESH_ID;
  ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObMViewPendingTaskGroup::alloc_and_build(allocator, info, tasks, group));
  ASSERT_TRUE(OB_NOT_NULL(group));

  EXPECT_EQ(OB_ERR_SYS, queue_.reserve_pending_task_group(*group));
  EXPECT_EQ(share::OB_INVALID_SCN_VAL, group->info_.target_data_sync_scn_);

  const ObMViewPendingTaskGroup *queued_group = nullptr;
  EXPECT_NE(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, queued_group));
}

TEST_F(ObMViewPendingTaskQueueTest, GroupResetClearsPublishedFlag)
{
  ObMViewPendingTaskGroup group;
  group.set_published();
  ASSERT_TRUE(group.is_published());

  group.reset();
  EXPECT_FALSE(group.is_published());
}

TEST_F(ObMViewPendingTaskQueueTest, StatusApisRejectUnpublishedReservation)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask task = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks;
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_ID, TEST_TARGET_SCN, tasks));

  bool finished = false;
  int64_t status = -1;
  int64_t retry_count = -1;
  bool cancelled = true;
  ObSEArray<uint64_t, 1> mview_ids;
  ASSERT_EQ(OB_SUCCESS, mview_ids.push_back(LEAF_MV));
  ObSEArray<uint64_t, 1> dep_ids;
  EXPECT_EQ(OB_EAGAIN, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  EXPECT_EQ(OB_EAGAIN, queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_EQ(OB_EAGAIN, queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_EQ(OB_EAGAIN, queue_.set_task_failed(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_EQ(OB_EAGAIN, queue_.set_task_pending(TENANT_ID, REFRESH_ID, LEAF_MV));
  EXPECT_EQ(OB_EAGAIN,
            queue_.set_task_running_to_pending(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_EQ(OB_EAGAIN, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(OB_EAGAIN, queue_.get_task_retry_count(TENANT_ID, REFRESH_ID, LEAF_MV, retry_count));
  EXPECT_EQ(OB_EAGAIN,
            queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID, LEAF_MV, cancelled));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            queue_.cancel_tasks_by_refresh_id(TENANT_ID, REFRESH_ID, finished));
  EXPECT_EQ(OB_EAGAIN,
            queue_.collect_task_and_dependents(TENANT_ID, REFRESH_ID, LEAF_MV, dep_ids));
  EXPECT_EQ(OB_EAGAIN,
            queue_.add_task_flags(TENANT_ID, REFRESH_ID, mview_ids,
                                  ObMViewPendingTask::FORCE_COMPLETE_REFRESH_FLAG));
  EXPECT_FALSE(finished);
  EXPECT_EQ(-1, status);
  EXPECT_EQ(-1, retry_count);
  EXPECT_FALSE(cancelled);
  EXPECT_TRUE(dep_ids.empty());

  bool memory_absent = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_CANCELLED, memory_absent, finished));
  EXPECT_TRUE(memory_absent);
  EXPECT_FALSE(finished);

  const ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
  EXPECT_FALSE(group->is_published());
  EXPECT_FALSE(group->cancelled_);
  EXPECT_EQ(MV_TASK_PENDING, group->tasks_.at(0).status_);
  EXPECT_EQ(0, group->tasks_.at(0).flags_);
  EXPECT_EQ(0, queue_.get_total_running_cnt());

  const ObMViewPendingTaskKey key(TENANT_ID, REFRESH_ID, LEAF_MV);
  ObMViewPendingTaskSessionIdEntry entry(key, 0, group->tasks_.at(0).seq_);
  entry.session_id_ = 42;
  ObSEArray<ObMViewPendingTaskSessionIdEntry, 1> entries;
  ASSERT_EQ(OB_SUCCESS, entries.push_back(entry));
  ASSERT_EQ(OB_SUCCESS, queue_.patch_session_ids_for_running_tasks(entries));
  EXPECT_EQ(0u, group->tasks_.at(0).session_id_);
}

TEST_F(ObMViewPendingTaskQueueTest, StatusApisWorkAfterUnpublishedGroupIsPublished)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask task = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks;
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_ID, TEST_TARGET_SCN, tasks));
  ASSERT_EQ(OB_SUCCESS, queue_.publish_pending_task_group(TENANT_ID, REFRESH_ID));

  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
  EXPECT_EQ(1, queue_.get_total_running_cnt());

  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_SUCCESS, memory_absent, finished));
  EXPECT_FALSE(memory_absent);
  EXPECT_TRUE(finished);
}

TEST_F(ObMViewPendingTaskQueueTest, UnpublishedFinishedHeadStillBlocksLaterGroup)
{
  const int64_t REFRESH_A = 116L;
  const int64_t REFRESH_B = 117L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask task_a = make_task(LEAF_MV);
  task_a.status_ = MV_TASK_CANCELLED;
  ObSEArray<ObMViewPendingTask, 1> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(task_a));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_A, 1, tasks_a));
  const ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_A, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
  EXPECT_TRUE(group->is_refresh_finished());

  ObMViewPendingTask task_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(task_b));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_B, 2, tasks_b));

  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  ObSEArray<int64_t, 8> refresh_ids;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, out));
  EXPECT_FALSE(info.is_valid());
  EXPECT_FALSE(out.is_valid());
  EXPECT_TRUE(refresh_ids.empty());
}

TEST_F(ObMViewPendingTaskQueueTest, CancelByMviewIdAffectsUnpublishedReservation)
{
  const int64_t REFRESH_A = 118L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask task_a = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(task_a));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_A, 1, tasks_a));
  ASSERT_EQ(OB_SUCCESS, queue_.cancel_tasks_by_mview_id(TENANT_ID, LEAF_MV));

  const ObMViewPendingTaskGroup *group_a = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_A, group_a));
  ASSERT_TRUE(OB_NOT_NULL(group_a));
  EXPECT_FALSE(group_a->is_published());
  EXPECT_EQ(MV_TASK_CANCELLED, group_a->tasks_.at(0).status_);
}

TEST_F(ObMViewPendingTaskQueueTest, PublishMissingGroupReturnsCanceled)
{
  EXPECT_EQ(OB_CANCELED, queue_.publish_pending_task_group(TENANT_ID, 999L));
}

TEST_F(ObMViewPendingTaskQueueTest, PublishCanceledReservationReturnsCanceled)
{
  const int64_t REFRESH_A = 119L;
  const int64_t REFRESH_B = 120L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask task_a = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(task_a));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_A, 1, tasks_a));

  ObMViewPendingTask task_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(task_b));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_B, 2, tasks_b));

  ObSEArray<int64_t, 1> published_ids;
  ASSERT_EQ(OB_SUCCESS,
            queue_.prepare_preceding_cancel(TENANT_ID, LEAF_MV, REFRESH_B, published_ids));
  EXPECT_TRUE(published_ids.empty());

  ASSERT_EQ(OB_CANCELED, queue_.publish_pending_task_group(TENANT_ID, REFRESH_A));

  const ObMViewPendingTaskGroup *group_a = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_A, group_a));
  ASSERT_TRUE(OB_NOT_NULL(group_a));
  EXPECT_FALSE(group_a->is_published());
  EXPECT_TRUE(group_a->cancelled_);
  ASSERT_EQ(OB_SUCCESS, queue_.recycle_refresh(TENANT_ID, REFRESH_A));
  EXPECT_EQ(OB_CANCELED, queue_.publish_pending_task_group(TENANT_ID, REFRESH_A));
}

TEST_F(ObMViewPendingTaskQueueTest, PreparePrecedingCancelHandlesPublishedAndUnpublishedGroups)
{
  const int64_t REFRESH_A = 201L;
  const int64_t REFRESH_B = 202L;
  const int64_t REFRESH_C = 203L;
  const int64_t REFRESH_D = 204L;
  const int64_t REFRESH_E = 205L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(MID_MV));

  ObMViewPendingTask task_a = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_a;
  ASSERT_EQ(OB_SUCCESS, tasks_a.push_back(task_a));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_A, 1, tasks_a));

  ObMViewPendingTask task_b = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_b;
  ASSERT_EQ(OB_SUCCESS, tasks_b.push_back(task_b));
  ASSERT_EQ(OB_SUCCESS, reserve_tasks(queue_, REFRESH_B, 2, tasks_b));

  ObMViewPendingTask task_c = make_task(MID_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_c;
  ASSERT_EQ(OB_SUCCESS, tasks_c.push_back(task_c));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_C, 3, tasks_c));

  ObMViewPendingTask task_d = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_d;
  ASSERT_EQ(OB_SUCCESS, tasks_d.push_back(task_d));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_D, 4, tasks_d));

  ObMViewPendingTask task_e = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_e;
  ASSERT_EQ(OB_SUCCESS, tasks_e.push_back(task_e));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_E, 5, tasks_e));

  ObSEArray<int64_t, 4> published_ids;
  ASSERT_EQ(OB_SUCCESS,
            queue_.prepare_preceding_cancel(TENANT_ID, LEAF_MV, REFRESH_D, published_ids));
  ASSERT_EQ(1, published_ids.count());
  EXPECT_EQ(REFRESH_A, published_ids.at(0));

  const ObMViewPendingTaskGroup *group_a = nullptr;
  const ObMViewPendingTaskGroup *group_b = nullptr;
  const ObMViewPendingTaskGroup *group_c = nullptr;
  const ObMViewPendingTaskGroup *group_d = nullptr;
  const ObMViewPendingTaskGroup *group_e = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_A, group_a));
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_B, group_b));
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_C, group_c));
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_D, group_d));
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_E, group_e));
  ASSERT_TRUE(OB_NOT_NULL(group_a));
  ASSERT_TRUE(OB_NOT_NULL(group_b));
  ASSERT_TRUE(OB_NOT_NULL(group_c));
  ASSERT_TRUE(OB_NOT_NULL(group_d));
  ASSERT_TRUE(OB_NOT_NULL(group_e));
  EXPECT_FALSE(group_a->cancelled_);
  EXPECT_TRUE(group_b->cancelled_);
  EXPECT_EQ(MV_TASK_CANCELLED, group_b->tasks_.at(0).status_);
  EXPECT_EQ(MV_TASK_PENDING, group_c->tasks_.at(0).status_);
  EXPECT_EQ(MV_TASK_PENDING, group_d->tasks_.at(0).status_);
  EXPECT_EQ(MV_TASK_PENDING, group_e->tasks_.at(0).status_);
}

TEST_F(ObMViewPendingTaskQueueTest, PreparePrecedingCancelSkipsHigherScn)
{
  const int64_t REFRESH_HIGH_SCN = 231L;
  const int64_t REFRESH_TARGET = 232L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask task_high = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_high;
  ASSERT_EQ(OB_SUCCESS, tasks_high.push_back(task_high));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_HIGH_SCN, 10, tasks_high));

  ObMViewPendingTask task_target = make_task(LEAF_MV);
  ObSEArray<ObMViewPendingTask, 1> tasks_target;
  ASSERT_EQ(OB_SUCCESS, tasks_target.push_back(task_target));
  ASSERT_EQ(OB_SUCCESS, enqueue_tasks(queue_, REFRESH_TARGET, 1, tasks_target));

  ObSEArray<int64_t, 4> published_ids;
  ASSERT_EQ(OB_SUCCESS,
            queue_.prepare_preceding_cancel(TENANT_ID, LEAF_MV, REFRESH_TARGET, published_ids));
  EXPECT_TRUE(published_ids.empty());
}

TEST_F(ObMViewPendingTaskQueueTest, PreparePrecedingCancelMissingTargetReturnsEmpty)
{
  ObSEArray<int64_t, 4> published_ids;
  ASSERT_EQ(OB_SUCCESS,
            queue_.prepare_preceding_cancel(TENANT_ID, LEAF_MV, 999L, published_ids));
  EXPECT_TRUE(published_ids.empty());
}

TEST_F(ObMViewPendingTaskQueueTest, RecycleRefreshRejectsNegativeCallbackCount)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask task = make_task(LEAF_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, task));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            queue_.recycle_refresh(TENANT_ID,
                                   REFRESH_ID,
                                   ObMViewPendingTaskQueue::RecycleCb(),
                                   -1));

  const ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
}

TEST_F(ObMViewPendingTaskQueueTest, RecycleRefreshCallbackCountLimitsPrefix)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(MID_MV));
  ObMViewPendingTask leaf_task = make_task(LEAF_MV);
  ObMViewPendingTask mid_task = make_task(MID_MV);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, mid_task));

  int64_t callback_count = 0;
  uint64_t first_mview_id = OB_INVALID_ID;
  RecycleCountRecorder recorder(&callback_count, &first_mview_id);
  ObMViewPendingTaskQueue::RecycleCb cb(recorder);
  ASSERT_TRUE(cb.is_valid());
  ASSERT_EQ(OB_SUCCESS, queue_.recycle_refresh(TENANT_ID, REFRESH_ID, cb, 1));
  EXPECT_EQ(1, callback_count);
  EXPECT_EQ(LEAF_MV, first_mview_id);

  const ObMViewPendingTaskGroup *group = nullptr;
  EXPECT_NE(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
}

TEST_F(ObMViewPendingTaskQueueTest, RecycleRefreshZeroCallbackCountStillRemovesGroup)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask task = make_task(LEAF_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, task));

  int64_t callback_count = 0;
  RecycleCountRecorder recorder(&callback_count);
  ObMViewPendingTaskQueue::RecycleCb cb(recorder);
  ASSERT_EQ(OB_SUCCESS, queue_.recycle_refresh(TENANT_ID, REFRESH_ID, cb, 0));
  EXPECT_EQ(0, callback_count);

  const ObMViewPendingTaskGroup *group = nullptr;
  EXPECT_NE(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
}

TEST_F(ObMViewPendingTaskQueueTest, MultipleRefreshesAreIndependent)
{
  const int64_t REFRESH_A = 200L;
  const int64_t REFRESH_B = 201L;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));

  ObMViewPendingTask ta = make_task(LEAF_MV);
  ObMViewPendingTask tb = make_task(LEAF_MV);

  ASSERT_EQ(OB_SUCCESS, push_one(queue_, ta, TEST_TARGET_SCN, REFRESH_A));
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, tb, TEST_TARGET_SCN, REFRESH_B));

  // Mark REFRESH_A's task as running
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_A, LEAF_MV));

  ObMViewTaskStatus sa, sb;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_A, sa));
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_B, sb));
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
// 11. ObMViewPendingTaskGroup::is_refresh_finished
// ---------------------------------------------------------------------------

TEST(ObMViewPendingTaskGroupTest, CancellationRespectsTargetAndTaskState)
{
  const ObMViewTaskStatus statuses[] = {
    MV_TASK_PENDING, MV_TASK_RETRY_WAIT, MV_TASK_RUNNING,
    MV_TASK_SUCCESS, MV_TASK_FAILED, MV_TASK_CANCELLED
  };
  const uint64_t target_ids[] = {1001, 1002, 1003, 1004, 1005, 1006, OB_INVALID_ID, 9999};
  const int64_t flags = ObMViewPendingTask::NESTED_REFRESH_FLAG
                      | ObMViewPendingTask::FORCE_COMPLETE_REFRESH_FLAG;
  for (const uint64_t target_id : target_ids) {
    SCOPED_TRACE(target_id);
    ObArenaAllocator alloc;
    ObMViewPendingTaskGroup group;
    ASSERT_EQ(OB_SUCCESS, group.init(alloc, ARRAYSIZEOF(statuses)));
    for (int64_t i = 0; i < ARRAYSIZEOF(statuses); ++i) {
      ObMViewPendingTask task = make_task_with_status(1001 + i, statuses[i], flags);
      ASSERT_EQ(OB_SUCCESS, group.tasks_.push_back(task));
    }

    group.cancel_task(target_id);

    for (int64_t i = 0; i < ARRAYSIZEOF(statuses); ++i) {
      SCOPED_TRACE(i);
      const ObMViewPendingTask &task = group.tasks_.at(i);
      const bool selected = OB_INVALID_ID == target_id || target_id == task.mview_id_;
      const bool cancel_pending = selected
          && (MV_TASK_PENDING == statuses[i] || MV_TASK_RETRY_WAIT == statuses[i]);
      EXPECT_EQ(cancel_pending ? MV_TASK_CANCELLED : statuses[i], task.status_);
      EXPECT_EQ(selected && MV_TASK_RUNNING == statuses[i], task.is_cancelled());
      EXPECT_TRUE(task.is_nested_refresh());
      EXPECT_TRUE(task.is_force_complete_refresh());
    }
    EXPECT_EQ(1, group.get_running_cnt());
    EXPECT_FALSE(group.is_refresh_finished());
  }
}

TEST(ObMViewPendingTaskGroupTest, IsRefreshFinished)
{
  ObMViewPendingTaskGroup ctx;

  // Empty tasks_: first_unfinished_pos_ (0) >= tasks_.count() (0) -> finished.
  EXPECT_TRUE(ctx.is_refresh_finished());

  // Allocate one task slot and set first_unfinished_pos_ < count.
  ObArenaAllocator alloc;
  ASSERT_EQ(OB_SUCCESS, ctx.init(alloc, 1));
  ObMViewPendingTask t;
  t.status_ = MV_TASK_PENDING;
  ASSERT_EQ(OB_SUCCESS, ctx.tasks_.push_back(t));
  ctx.first_unfinished_pos_ = 0;
  EXPECT_FALSE(ctx.is_refresh_finished());
  ctx.first_unfinished_pos_ = 1;
  EXPECT_TRUE(ctx.is_refresh_finished());
}

// ---------------------------------------------------------------------------
// 12. Concurrent push from multiple threads (different refresh_ids)
//
// Each thread pushes tasks for a different refresh_id.
// All pushes must succeed and every task must be fetchable.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ConcurrentPushDifferentRefreshes)
{
  static const int      THREAD_COUNT    = 8;
  static const uint64_t BASE_MV_ID      = 3000UL;
  static const int64_t  BASE_REFRESH_ID = 500L;

  // Seed sched_info entries on the main thread; the drain step below fetches tasks.
  for (int i = 0; i < THREAD_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_sched_info(BASE_MV_ID + static_cast<uint64_t>(i)));
  }

  // Concurrent pushes — each thread creates its own refresh context.
  std::vector<std::thread> threads;
  std::vector<int> results(THREAD_COUNT, OB_SUCCESS);
  for (int i = 0; i < THREAD_COUNT; ++i) {
    threads.emplace_back([this, i, &results]() {
      ObMViewPendingTask t = make_task(BASE_MV_ID + static_cast<uint64_t>(i));
      results[i] = push_one(queue_, t, TEST_TARGET_SCN,
                            BASE_REFRESH_ID + i);
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
  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask out;
  while (OB_SUCCESS == test_fetch_next_task(queue_, info, out)) {
    bool fin = false;
    ASSERT_EQ(OB_SUCCESS,
              queue_.set_task_success(info.tenant_id_, info.refresh_id_, out.mview_id_, fin));
    ++consumed;
  }
  EXPECT_EQ(THREAD_COUNT, consumed);
}

// ---------------------------------------------------------------------------
// 13. Concurrent push (producers) and fetch (consumer)
//
// A single consumer thread fetches each task (which marks it running) then marks
// it successful. THREAD_COUNT producer threads each push one task. After all
// producers finish and the consumer drains the queue, every task must have
// been consumed exactly once.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ConcurrentPushAndFetch)
{
  static const int      THREAD_COUNT    = 8;
  static const uint64_t BASE_MV_ID      = 4000UL;
  static const int64_t  BASE_REFRESH_ID = 600L;

  for (int i = 0; i < THREAD_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_sched_info(BASE_MV_ID + static_cast<uint64_t>(i)));
  }

  std::atomic<int> consumed{0};
  std::atomic<bool> producers_done{false};

  // Consumer: spins until producers are done and queue is empty.
  std::thread consumer([&]() {
    while (true) {
      ObMViewPendingTaskGroup::GroupInfo info;
      ObMViewPendingTask out;
      int ret = test_fetch_next_task(queue_, info, out);
      if (OB_SUCCESS == ret) {
        bool fin = false;
        if (OB_SUCCESS == queue_.set_task_success(
                              info.tenant_id_, info.refresh_id_, out.mview_id_, fin)) {
          ++consumed;
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
      ObMViewPendingTask t = make_task(BASE_MV_ID + static_cast<uint64_t>(i));
      EXPECT_EQ(OB_SUCCESS,
                push_one(queue_, t, TEST_TARGET_SCN,
                         BASE_REFRESH_ID + i));
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
  const uint64_t root_deps[] = { LEAF_MV };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));
  ObMViewPendingTask leaf_task = make_task(LEAF_MV);
  ObMViewPendingTask root_task = make_task(ROOT_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, root_task));

  ASSERT_EQ(OB_SUCCESS, queue_.clear());

  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));

  ObMViewTaskStatus status;
  EXPECT_NE(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
}

// ---------------------------------------------------------------------------
// 18. Pushing a task with the same (tenant, refresh, mview) key twice fails
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, DuplicatePushFails)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));
  // group_map_.set_refactored returns OB_HASH_EXIST when (tenant, refresh)
  // already has a group.
  EXPECT_NE(OB_SUCCESS, push_one(queue_, t));
}

// ---------------------------------------------------------------------------
// 19. set_task_running on an already-RUNNING task returns OB_EAGAIN
//     (from_status=PENDING does not match current=RUNNING)
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, SetRunningOnAlreadyRunningReturnsEAGAIN)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));
  EXPECT_EQ(OB_EAGAIN,
            queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
}

// ---------------------------------------------------------------------------
// 20. set_task_success from PENDING (not RUNNING) returns OB_EAGAIN
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, SuccessFromNonRunningStateReturnsEAGAIN)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask leaf_task = make_task(LEAF_MV, 0);
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
            get_refresh_status(queue_, TENANT_ID, 9999L, status));
}

// ---------------------------------------------------------------------------
// 22. RETRY_WAIT task is NOT visible via fetch_next_task (status != PENDING)
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, RetryWaitTaskIsNotFetchable)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  // RETRY_WAIT is not PENDING -> fetch must skip it
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));

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
  const uint64_t root_deps[] = { LEAF_MV };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));
  ObMViewPendingTask leaf_task = make_task(LEAF_MV);
  ObMViewPendingTask root_task = make_task(ROOT_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, root_task));

  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished); // ROOT still unfinished

  // running_cnt==0, unfinished_cnt==1 -> PENDING
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
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
  const uint64_t b_deps[] = { A };
  const uint64_t c_deps[] = { A };
  const uint64_t d_deps[] = { B, C };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(A));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(B, b_deps, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(C, c_deps, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(D, d_deps, 2));

  ObMViewPendingTask task_a = make_task(A);
  ObMViewPendingTask task_b = make_task(B, 0);
  ObMViewPendingTask task_c = make_task(C, 0);
  ObMViewPendingTask task_d = make_task(D, 0);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, task_a, task_b, task_c, task_d));
  // Only A is ready initially
  ObMViewPendingTask out;
  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(A, out.mview_id_);
  // B and C are blocked while A is running
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));

  // A succeeds; B and C both become dispatchable
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_success(TENANT_ID, REFRESH_ID, A, finished));
  EXPECT_FALSE(finished);

  // Drain B and C in whatever order the queue offers them.
  // After A succeeds both B and C are independently dispatchable; they can run
  // concurrently so there is no OB_EAGAIN between dispatching them.
  for (int step = 0; step < 2; ++step) {
    ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
    EXPECT_TRUE(out.mview_id_ == B || out.mview_id_ == C)
        << "unexpected task: " << out.mview_id_;
    uint64_t mid = out.mview_id_;
    ASSERT_EQ(OB_SUCCESS,
              queue_.set_task_success(TENANT_ID, REFRESH_ID, mid, finished));
    EXPECT_FALSE(finished); // D still has at least one unsatisfied dependency
  }

  // Both B and C succeeded; D is now ready
  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(D, out.mview_id_);
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

  const uint64_t dep1[] = { IDS[0] };
  const uint64_t dep2[] = { IDS[1] };
  const uint64_t dep3[] = { IDS[2] };
  const uint64_t dep4[] = { IDS[3] };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(IDS[0]));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(IDS[1], dep1, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(IDS[2], dep2, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(IDS[3], dep3, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(IDS[4], dep4, 1));

  ObMViewPendingTask t0 = make_task(IDS[0]);
  ObMViewPendingTask t1 = make_task(IDS[1], 0);
  ObMViewPendingTask t2 = make_task(IDS[2], 0);
  ObMViewPendingTask t3 = make_task(IDS[3], 0);
  ObMViewPendingTask t4 = make_task(IDS[4], 0);

  ASSERT_EQ(OB_SUCCESS, push_many(queue_, t0, t1, t2, t3, t4));

  for (int i = 0; i < DEPTH; ++i) {
    ObMViewPendingTask out;
    ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
    EXPECT_EQ(IDS[i], out.mview_id_) << "wrong task at depth " << i;
    bool finished = false;
    ASSERT_EQ(OB_SUCCESS,
              queue_.set_task_success(TENANT_ID, REFRESH_ID, IDS[i], finished));
    EXPECT_EQ(i == DEPTH - 1, finished);
  }
}

TEST_F(ObMViewPendingTaskQueueTest, InspectReportsAbortedDependency)
{
  const uint64_t dep_mview_id = 4001;
  const uint64_t root_mview_id = 4002;
  const uint64_t root_deps[] = { dep_mview_id };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(dep_mview_id));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(root_mview_id, root_deps, 1));

  ObMViewPendingTask dep_task = make_task_with_status(dep_mview_id, MV_TASK_FAILED);
  ObMViewPendingTask root_task = make_task(root_mview_id);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, dep_task, root_task));

  ObSEArray<int64_t, 4> refresh_ids;
  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask task;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, task));
  EXPECT_FALSE(info.is_valid());
  EXPECT_FALSE(task.is_valid());
  ASSERT_EQ(1, refresh_ids.count());
  EXPECT_EQ(REFRESH_ID, refresh_ids.at(0));
}

TEST_F(ObMViewPendingTaskQueueTest, AbortedDependencyPropagatesTransitively)
{
  const uint64_t leaf_mview_id = 4101;
  const uint64_t mid_mview_id = 4102;
  const uint64_t root_mview_id = 4103;
  const uint64_t mid_deps[] = { leaf_mview_id };
  const uint64_t root_deps[] = { mid_mview_id };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(leaf_mview_id));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(mid_mview_id, mid_deps, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(root_mview_id, root_deps, 1));

  ObMViewPendingTask leaf_task = make_task_with_status(leaf_mview_id, MV_TASK_FAILED);
  ObMViewPendingTask mid_task = make_task(mid_mview_id);
  ObMViewPendingTask root_task = make_task(root_mview_id);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf_task, mid_task, root_task));

  ObSEArray<int64_t, 4> refresh_ids;
  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask task;
  ASSERT_EQ(OB_SUCCESS, queue_.find_next_ready_task(refresh_ids, info, task));
  EXPECT_FALSE(info.is_valid());
  EXPECT_FALSE(task.is_valid());
  ASSERT_EQ(1, refresh_ids.count());
  EXPECT_EQ(REFRESH_ID, refresh_ids.at(0));
}

// ---------------------------------------------------------------------------
// 26. Reload batch: root was RUNNING when RS crashed
//
// After reload the group has a RUNNING task, so get_refresh_status returns
// RUNNING and total_running_cnt_ is updated. When the inspect-task call reports
// set_task_success, the refresh finishes.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithRunningTask)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV));
  ObMViewPendingTask leaf = make_task_with_status(LEAF_MV, MV_TASK_SUCCESS);
  ObMViewPendingTask root = make_task_with_status(ROOT_MV, MV_TASK_RUNNING);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, root));

  // Directly verify ctx fields set by ObMViewPendingTaskGroup::alloc_and_build
  {
    ObMViewPendingTaskGroup ctx;
    get_ctx(ctx);
    const ObMViewPendingTaskGroup *ptr = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, ptr));
    ASSERT_TRUE(OB_NOT_NULL(ptr));
    EXPECT_TRUE(has_running_in_group(*ptr));
    EXPECT_EQ(1, ctx.first_unfinished_pos_);
    EXPECT_FALSE(has_failed_in_group(*ptr));
  }

  // has a RUNNING task -> RUNNING
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);

  // total_running_cnt_ must account for the reloaded RUNNING task
  EXPECT_EQ(1, queue_.get_total_running_cnt());

  // RUNNING task is not fetchable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));

  // Inspect-task call completes the root task
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, ROOT_MV, finished));
  EXPECT_TRUE(finished);

  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_SUCCESS, status);
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

// ---------------------------------------------------------------------------
// 27. Reload batch: root was RETRY_WAIT when RS crashed
//
// first unfinished task at pos 0, no running tasks -> PENDING.
// RETRY_WAIT tasks are not fetchable; set_task_pending re-enables dispatch.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithRetryWaitRoot)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV));
  ObMViewPendingTask root = make_task_with_status(ROOT_MV, MV_TASK_RETRY_WAIT);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, root));

  // Directly verify ctx: unfinished=1, no running (RETRY_WAIT counts as unfinished)
  {
    ObMViewPendingTaskGroup ctx;
    get_ctx(ctx);
    const ObMViewPendingTaskGroup *ptr = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, ptr));
    ASSERT_TRUE(OB_NOT_NULL(ptr));
    EXPECT_FALSE(has_running_in_group(*ptr));
    EXPECT_EQ(0, ctx.first_unfinished_pos_);
    EXPECT_FALSE(has_failed_in_group(*ptr));
  }

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);

  // RETRY_WAIT is not fetchable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));

  // set_task_pending moves it back to PENDING so it can be dispatched again
  ASSERT_EQ(OB_SUCCESS, queue_.set_task_pending(TENANT_ID, REFRESH_ID, ROOT_MV));
  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(ROOT_MV, out.mview_id_);
}

// ---------------------------------------------------------------------------
// 28. Reload batch: all tasks already in terminal-failure state
//
// FAILED leaf + CANCELLED root -> has FAILED task, unfinished == 0
// -> get_refresh_status returns FAILED immediately after push.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithAllTerminalFailure)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV));
  ObMViewPendingTask leaf = make_task_with_status(LEAF_MV, MV_TASK_FAILED);
  ObMViewPendingTask root = make_task_with_status(ROOT_MV, MV_TASK_CANCELLED);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, root));

  // FAILED leaf is detected by has_failed_in_group, CANCELLED root does not increment
  // unfinished, so unfinished == 0 and no running tasks
  {
    ObMViewPendingTaskGroup ctx;
    get_ctx(ctx);
    EXPECT_EQ(2,       ctx.first_unfinished_pos_);
    const ObMViewPendingTaskGroup *ptr = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, ptr));
    ASSERT_TRUE(OB_NOT_NULL(ptr));
    EXPECT_TRUE(has_failed_in_group(*ptr));
  }

  // has FAILED task, unfinished == 0 -> FAILED
  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_FAILED, status);

  // Nothing to dispatch
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

// ---------------------------------------------------------------------------
// 29. Reload batch: root already SUCCESS
//
// unfinished == 0, no terminal failure -> SUCCESS immediately.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadBatchWithRootAlreadySucceeded)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV));
  ObMViewPendingTask root = make_task_with_status(ROOT_MV, MV_TASK_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, root));

  {
    ObMViewPendingTaskGroup ctx;
    get_ctx(ctx);
    EXPECT_EQ(1,       ctx.first_unfinished_pos_);
    const ObMViewPendingTaskGroup *ptr = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, ptr));
    ASSERT_TRUE(OB_NOT_NULL(ptr));
    EXPECT_FALSE(has_failed_in_group(*ptr));
  }

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_SUCCESS, status);

  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));
}

// ---------------------------------------------------------------------------
// 30. Reload mixed-status batch: leaf=SUCCESS, mid=RUNNING, root=PENDING
//
// Simulates a crash after leaf finished but while mid was still running.
// - mid=RUNNING, unfinished_cnt=2 (mid+root) -> RUNNING initially
// - root is PENDING but its dep (mid) is RUNNING, not SUCCESS -> not fetchable
// - After mid is marked success, root becomes fetchable
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, ReloadMixedStatusBatch)
{
  const uint64_t mid_deps[]  = { LEAF_MV };
  const uint64_t root_deps[] = { MID_MV  };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(MID_MV, mid_deps, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));

  ObMViewPendingTask leaf = make_task_with_status(LEAF_MV, MV_TASK_SUCCESS);
  ObMViewPendingTask mid  = make_task_with_status(MID_MV,  MV_TASK_RUNNING);
  ObMViewPendingTask root = make_task_with_status(ROOT_MV, MV_TASK_PENDING);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, mid, root));

  // leaf=SUCCESS contributes nothing; mid=RUNNING counts as unfinished+running;
  // root=PENDING counts as unfinished only -> has running, unfinished=2
  {
    ObMViewPendingTaskGroup ctx;
    get_ctx(ctx);
    const ObMViewPendingTaskGroup *ptr = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, ptr));
    ASSERT_TRUE(OB_NOT_NULL(ptr));
    EXPECT_TRUE(has_running_in_group(*ptr));
    EXPECT_EQ(1, ctx.first_unfinished_pos_);
    EXPECT_FALSE(has_failed_in_group(*ptr));
  }

  ObMViewTaskStatus status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
  EXPECT_EQ(1, queue_.get_total_running_cnt());

  // root is PENDING but mid (its dep) is not SUCCESS yet -> not fetchable
  ObMViewPendingTask out;
  EXPECT_EQ(OB_EAGAIN, test_fetch_next_task(queue_, out));

  // Inspect-task marks mid as success
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, MID_MV, finished));
  EXPECT_FALSE(finished); // root still pending

  // Now root's dep is satisfied -> fetchable
  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(ROOT_MV, out.mview_id_);
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, ROOT_MV, finished));
  EXPECT_TRUE(finished);
}

// ---------------------------------------------------------------------------
// 31. Push a task with an invalid mview_id fails is_valid() → deep_copy →
//     alloc_and_build propagates OB_ERR_UNEXPECTED.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, PushInvalidTaskFails)
{
  ObMViewPendingTask root = make_task(ROOT_MV, 0);
  root.mview_id_ = OB_INVALID_ID; // fails ObMViewPendingTask::is_valid()
  EXPECT_NE(OB_SUCCESS, push_one(queue_, root));
}

// ---------------------------------------------------------------------------
// 32. set_task_running_to_pending: RUNNING -> PENDING bumps retry_count
//     and drops running counters; task is dispatchable again via fetch.
//
// Covers the new no-delay retry path (mark_task_retry_wait fast-track) added
// by the retry_wait sync fix.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, SetTaskRunningToPendingResetsForRetry)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));
  EXPECT_EQ(1, queue_.get_total_running_cnt());

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_running_to_pending(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  // running counters decrement, unfinished still includes the task
  EXPECT_EQ(0, queue_.get_total_running_cnt());
  ObMViewPendingTaskGroup ctx;
  get_ctx(ctx);
  const ObMViewPendingTaskGroup *ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, ptr));
  ASSERT_TRUE(OB_NOT_NULL(ptr));
  EXPECT_FALSE(has_running_in_group(*ptr));
  EXPECT_EQ(0, ctx.first_unfinished_pos_);

  // status is back to PENDING and the task is fetchable again
  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_PENDING, status);

  int64_t retry_count = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_retry_count(TENANT_ID, REFRESH_ID, LEAF_MV, retry_count));
  EXPECT_EQ(1, retry_count);

  ObMViewPendingTask out;
  ASSERT_EQ(OB_SUCCESS, test_fetch_next_task(queue_, out));
  EXPECT_EQ(LEAF_MV, out.mview_id_);
}

// set_task_running_to_pending from a non-RUNNING state must fail (status CAS)
TEST_F(ObMViewPendingTaskQueueTest, SetTaskRunningToPendingFromPendingFails)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  bool finished = false;
  EXPECT_EQ(OB_EAGAIN,
              queue_.set_task_running_to_pending(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
}

TEST_F(ObMViewPendingTaskQueueTest, RetryRunningPreservesStartTime)
{
  const int64_t first_start_time = 1234567;
  ObMViewPendingTask task = make_task(LEAF_MV, 0);
  task.retry_count_ = 1;
  task.start_time_ = first_start_time;

  ASSERT_EQ(OB_SUCCESS,
            queue_.inner_set_task_status(task, MV_TASK_PENDING, MV_TASK_RUNNING, NULL));

  EXPECT_EQ(first_start_time, task.start_time_);
}

// ---------------------------------------------------------------------------
// 33. cancel_tasks_by_refresh_id now walks every task in the group directly:
//     PENDING/RETRY_WAIT are flipped to CANCELLED; RUNNING tasks retain
//     their status and carry a cancellation flag. Terminal tasks are unchanged.
//
// Topology: LEAF -> MID -> ROOT. LEAF is RUNNING; MID and ROOT are PENDING.
// After cancel_tasks_by_refresh_id, only LEAF stays RUNNING; MID/ROOT are
// CANCELLED.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, CancelAllSkipsRunningLeavesPendingCancelled)
{
  const uint64_t mid_deps[]  = { LEAF_MV };
  const uint64_t root_deps[] = { MID_MV  };
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(MID_MV, mid_deps, 1));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(ROOT_MV, root_deps, 1));

  ObMViewPendingTask leaf = make_task(LEAF_MV);
  ObMViewPendingTask mid  = make_task(MID_MV, 0);
  ObMViewPendingTask root = make_task(ROOT_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, mid, root));

  ASSERT_EQ(OB_SUCCESS, queue_.set_task_running(TENANT_ID, REFRESH_ID, LEAF_MV));

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.cancel_tasks_by_refresh_id(TENANT_ID, REFRESH_ID, finished));
  EXPECT_FALSE(finished); // LEAF still RUNNING; refresh not yet done

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, ROOT_MV, status));
  EXPECT_EQ(MV_TASK_CANCELLED, status);
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, MID_MV, status));
  EXPECT_EQ(MV_TASK_CANCELLED, status);
  // LEAF remains RUNNING until its completion callback arrives.
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
  EXPECT_EQ(1, queue_.get_total_running_cnt());
  bool cancelled = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID, LEAF_MV, cancelled));
  EXPECT_TRUE(cancelled);
}

TEST_F(ObMViewPendingTaskQueueTest, CancelTasksByMviewIdSkipsRunningTasks)
{
  const int64_t second_refresh_id = REFRESH_ID + 1;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask first_task = make_task(LEAF_MV, 0);
  ObMViewPendingTask second_task = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, first_task));
  ASSERT_EQ(OB_SUCCESS,
            push_one(queue_, second_task, TEST_TARGET_SCN, second_refresh_id));
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_running(TENANT_ID, second_refresh_id, LEAF_MV));
  const int64_t task_count = queue_.task_count_;

  ASSERT_EQ(OB_SUCCESS, queue_.cancel_tasks_by_mview_id(TENANT_ID, LEAF_MV));

  EXPECT_EQ(task_count, queue_.task_count_);
  EXPECT_EQ(1, queue_.get_total_running_cnt());
  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_CANCELLED, status);
  ASSERT_EQ(OB_SUCCESS,
            queue_.get_task_status(TENANT_ID, second_refresh_id, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);

  const ObMViewPendingTaskGroup *first_group = NULL;
  const ObMViewPendingTaskGroup *second_group = NULL;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, first_group));
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, second_refresh_id, second_group));
  ASSERT_NE(nullptr, first_group);
  ASSERT_NE(nullptr, second_group);
  EXPECT_TRUE(first_group->is_refresh_finished());
  EXPECT_FALSE(second_group->is_refresh_finished());
  EXPECT_FALSE(first_group->cancelled_);
  EXPECT_FALSE(second_group->cancelled_);
  bool cancelled = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.is_refresh_cancelled(TENANT_ID, second_refresh_id, LEAF_MV, cancelled));
  EXPECT_TRUE(cancelled);
}

TEST_F(ObMViewPendingTaskQueueTest, RefreshCancellationChecksTaskAndGroupFlags)
{
  ObMViewPendingTask leaf = make_task_with_status(LEAF_MV, MV_TASK_RUNNING);
  ObMViewPendingTask mid = make_task_with_status(MID_MV, MV_TASK_RUNNING);
  mid.set_cancelled();
  ASSERT_EQ(OB_SUCCESS, push_many(queue_, leaf, mid));

  bool cancelled = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID, MID_MV, cancelled));
  EXPECT_TRUE(cancelled);
  ASSERT_EQ(OB_SUCCESS,
            queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID, LEAF_MV, cancelled));
  EXPECT_FALSE(cancelled);

  bool finished = false;
  ASSERT_EQ(OB_SUCCESS, queue_.cancel_tasks_by_refresh_id(TENANT_ID, REFRESH_ID, finished));
  EXPECT_FALSE(finished);
  ASSERT_EQ(OB_SUCCESS,
            queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID, LEAF_MV, cancelled));
  EXPECT_TRUE(cancelled);
  EXPECT_EQ(2, queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskQueueTest, RefreshCancellationPropagatesLookupErrors)
{
  bool cancelled = true;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID, LEAF_MV, cancelled));
  EXPECT_FALSE(cancelled);

  ObMViewPendingTask task = make_task(LEAF_MV);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, task));
  cancelled = true;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID, ROOT_MV, cancelled));
  EXPECT_FALSE(cancelled);
}

// ---------------------------------------------------------------------------
// 34. set_task_failed requires RUNNING -> FAILED (strict status CAS).
//     Manager-layer finalize_task calls this while the task is still RUNNING;
//     direct calls from PENDING or RETRY_WAIT must return OB_EAGAIN.
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskQueueTest, SetTaskFailedFromPendingReturnsEagain)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  bool finished = false;
  EXPECT_EQ(OB_EAGAIN,
            queue_.set_task_failed(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskQueueTest, SetTaskFailedFromRetryWaitReturnsEagain)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);
  EXPECT_EQ(0, queue_.get_total_running_cnt());

  EXPECT_EQ(OB_EAGAIN,
            queue_.set_task_failed(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_FALSE(finished);

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_RETRY_WAIT, status);
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

// ---------------------------------------------------------------------------
// 35. align_task_to_status: full disk->memory matrix coverage
// ---------------------------------------------------------------------------

// memory_absent when refresh ctx does not exist
TEST_F(ObMViewPendingTaskQueueTest, AlignMissingRefreshReportsAbsent)
{
  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, 9999L, LEAF_MV,
                                        MV_TASK_RUNNING, memory_absent, finished));
  EXPECT_TRUE(memory_absent);
  EXPECT_FALSE(finished);
}

// memory_absent when refresh ctx exists but task is missing
TEST_F(ObMViewPendingTaskQueueTest, AlignMissingTaskReportsAbsent)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, 9999UL /*unknown mview*/,
                                        MV_TASK_RUNNING, memory_absent, finished));
  EXPECT_TRUE(memory_absent);
  EXPECT_FALSE(finished);
}

// Same-state no-op: target == memory; nothing changes
TEST_F(ObMViewPendingTaskQueueTest, AlignSameStateIsNoop)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));
  int64_t prev_running = queue_.get_total_running_cnt();

  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_RUNNING, memory_absent, finished));
  EXPECT_FALSE(memory_absent);
  EXPECT_FALSE(finished);
  EXPECT_EQ(prev_running, queue_.get_total_running_cnt());

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
}

// Same-state RUNNING: svr_addr is still patched
TEST_F(ObMViewPendingTaskQueueTest, AlignSameStateRunningPatchesSvrAddr)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  ObAddr svr;
  ASSERT_TRUE(svr.set_ip_addr("10.0.0.5", 2881));
  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_RUNNING, memory_absent, finished, &svr));
  EXPECT_FALSE(memory_absent);
  EXPECT_FALSE(finished);

  const ObMViewPendingTaskGroup *group = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_group(queue_, TENANT_ID, REFRESH_ID, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
  bool found = false;
  for (int64_t i = 0; i < group->tasks_.count(); ++i) {
    if (LEAF_MV == group->tasks_.at(i).mview_id_) {
      EXPECT_EQ(svr, group->tasks_.at(i).svr_addr_);
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

// Memory PENDING, disk RUNNING -> align mutates state and bumps running counters
TEST_F(ObMViewPendingTaskQueueTest, AlignPendingToRunningUpdatesCounters)
{
  const int64_t disk_start_time = 1234567;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));

  ObAddr svr;
  ASSERT_TRUE(svr.set_ip_addr("10.0.0.6", 2882));
  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_RUNNING, memory_absent, finished,
                                        &svr, disk_start_time));
  EXPECT_FALSE(memory_absent);
  EXPECT_FALSE(finished);

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
  EXPECT_EQ(1, queue_.get_total_running_cnt());
  ObSEArray<ObMViewPendingRunningJobInfo, 1> running_job_infos;
  ASSERT_EQ(OB_SUCCESS, queue_.collect_running_jobs(running_job_infos));
  ASSERT_EQ(1, running_job_infos.count());
  EXPECT_EQ(disk_start_time, running_job_infos.at(0).start_time_);
}

// Memory RUNNING, disk SUCCESS -> finishes refresh
TEST_F(ObMViewPendingTaskQueueTest, AlignRunningToSuccessFinishesRefresh)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));

  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_SUCCESS, memory_absent, finished));
  EXPECT_FALSE(memory_absent);
  EXPECT_TRUE(finished);

  EXPECT_EQ(0, queue_.get_total_running_cnt());
  ObMViewTaskStatus refresh_status;
  ASSERT_EQ(OB_SUCCESS, get_refresh_status(queue_, TENANT_ID, REFRESH_ID, refresh_status));
  EXPECT_EQ(MV_TASK_SUCCESS, refresh_status);
}

// Memory PENDING, disk SUCCESS -> ok via align (counters do not underflow)
// This is the documented align path: SUCCESS reached without going RUNNING.
TEST_F(ObMViewPendingTaskQueueTest, AlignPendingToSuccessSkipsRunningCounters)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(LEAF_MV));
  ObMViewPendingTask t = make_task(LEAF_MV, 0);
  ASSERT_EQ(OB_SUCCESS, push_one(queue_, t));
  EXPECT_EQ(0, queue_.get_total_running_cnt());

  bool memory_absent = false;
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_SUCCESS, memory_absent, finished));
  EXPECT_FALSE(memory_absent);
  EXPECT_TRUE(finished);
  EXPECT_EQ(0, queue_.get_total_running_cnt()); // no underflow
}

// Memory RETRY_WAIT, disk PENDING -> counters untouched (no running delta)
TEST_F(ObMViewPendingTaskQueueTest, AlignRetryWaitToPendingNoRunningDelta)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_retry_wait(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_EQ(0, queue_.get_total_running_cnt());

  bool memory_absent = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_PENDING, memory_absent, finished));
  EXPECT_FALSE(memory_absent);
  EXPECT_FALSE(finished);
  EXPECT_EQ(0, queue_.get_total_running_cnt());

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
}

// Memory terminal, disk reports a different state -> WARN, no mutation
TEST_F(ObMViewPendingTaskQueueTest, AlignTerminalMemoryNotRevived)
{
  ASSERT_EQ(OB_SUCCESS, push_and_run(LEAF_MV, 0));
  bool finished = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.set_task_success(TENANT_ID, REFRESH_ID, LEAF_MV, finished));
  EXPECT_TRUE(finished);

  // Try to align SUCCESS->RUNNING; align must refuse to revive terminal memory.
  bool memory_absent = false;
  ASSERT_EQ(OB_SUCCESS,
            queue_.align_task_to_status(TENANT_ID, REFRESH_ID, LEAF_MV,
                                        MV_TASK_RUNNING, memory_absent, finished));
  EXPECT_FALSE(memory_absent);
  // refresh was already finished before the call; the helper still reports it
  // as finished because nothing about ctx changed.
  EXPECT_TRUE(finished);

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS, queue_.get_task_status(TENANT_ID, REFRESH_ID, LEAF_MV, status));
  EXPECT_EQ(MV_TASK_SUCCESS, status); // untouched
  EXPECT_EQ(0, queue_.get_total_running_cnt());
}

// ---------------------------------------------------------------------------
// 36. check_prev_task_satisfied: a CANCELLED previous version with
//     retry_count > 0 still blocks dispatch.
//
// The queue stores tasks under group_list_ (per-refresh groups, tasks_ per
// group) in submission order. When a task is
// CANCELLED with retry_count == 0, fetch treats it as non-existent (a fresh
// task may run). When CANCELLED but retry_count > 0, fetch treats it as a
// real ancestor of the same mview_id whose run finished, so a subsequent
// duplicate push for the same mview_id should NOT be fetchable.
//
// Direct unit-testing of this edge is awkward because push_tasks rejects
// duplicate keys; instead we verify the supporting helper indirectly through
// is_task_status_non_terminal and document the contract here.
// ---------------------------------------------------------------------------

TEST(ObMViewPendingTaskQueueStaticTest, IsTaskStatusNonTerminal)
{
  EXPECT_TRUE(ObMViewPendingTask::is_task_status_non_terminal(MV_TASK_PENDING));
  EXPECT_TRUE(ObMViewPendingTask::is_task_status_non_terminal(MV_TASK_RUNNING));
  EXPECT_TRUE(ObMViewPendingTask::is_task_status_non_terminal(MV_TASK_RETRY_WAIT));
  EXPECT_FALSE(ObMViewPendingTask::is_task_status_non_terminal(MV_TASK_SUCCESS));
  EXPECT_FALSE(ObMViewPendingTask::is_task_status_non_terminal(MV_TASK_FAILED));
  EXPECT_FALSE(ObMViewPendingTask::is_task_status_non_terminal(MV_TASK_CANCELLED));
}

TEST(ObMViewPendingTaskStaticTest, CancelledFlagPreservesRefreshFlags)
{
  const int64_t flags = ObMViewPendingTask::NESTED_REFRESH_FLAG
                      | ObMViewPendingTask::FORCE_COMPLETE_REFRESH_FLAG;
  ObMViewPendingTask task = make_task_with_status(1001UL, MV_TASK_RUNNING, flags);
  EXPECT_FALSE(task.is_cancelled());
  task.set_cancelled();
  task.set_cancelled();
  EXPECT_TRUE(task.is_cancelled());
  EXPECT_EQ(flags, task.flags_);
  EXPECT_EQ(MV_TASK_RUNNING, task.status_);

  ObMViewPendingTask copy;
  ASSERT_EQ(OB_SUCCESS, copy.assign(task));
  EXPECT_TRUE(copy.is_cancelled());
  EXPECT_TRUE(copy.is_nested_refresh());
  EXPECT_TRUE(copy.is_force_complete_refresh());
  EXPECT_EQ(MV_TASK_RUNNING, copy.status_);
  copy.reset();
  EXPECT_FALSE(copy.is_cancelled());
  EXPECT_EQ(0, copy.flags_);
  EXPECT_TRUE(task.is_cancelled());
}

TEST(ObMViewPendingTaskStaticTest, NestedRefreshFlagRecognized)
{
  ObMViewPendingTask t = make_task(1001UL);
  EXPECT_FALSE(t.is_nested_refresh());

  t.flags_ = ObMViewPendingTask::NESTED_REFRESH_FLAG;
  EXPECT_TRUE(t.is_nested_refresh());

  t.flags_ = ObMViewPendingTask::NESTED_REFRESH_FLAG | 1;
  EXPECT_TRUE(t.is_nested_refresh());

  t.flags_ = 0;
  EXPECT_FALSE(t.is_nested_refresh());
}

// ---------------------------------------------------------------------------
// 37. ObMViewSchedInfoMgr group_id calculation (full UF + incremental)
//
// Verifies that regroup_sched_infos (full/scoped) maintains
// scheduling groups:
//   - group_info_map_ keys and UF roots use max(mview_id) per component
//   - scheduler suspension splits connected components at blocked boundaries
// Test-only helpers live here (not on the production class). Private members
// are accessed via `#define private public` at the top of this file.
// ---------------------------------------------------------------------------

class ObMViewSchedInfoMgrGroupIdsTest : public ::testing::Test
{
public:
  struct AlwaysFailingMemberFn
  {
    AlwaysFailingMemberFn() : call_count_(0) {}
    int operator()(const ObMViewSchedInfo &, const ObMViewSchedGroupInfo &)
    {
      ++call_count_;
      return OB_ERR_UNEXPECTED;
    }
    int64_t call_count_;
  };

  static constexpr int64_t DEFAULT_SPECIFIED_INTERVAL =
      ObMViewTieredIntervalCalc::BASE_UNIT_US;

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, mgr_.init(TENANT_ID, 64L));
  }

  void TearDown() override
  {
    ASSERT_EQ(OB_SUCCESS, mgr_.destroy());
  }

  // Inserts a synthetic node directly into the mgr's info_map_,
  // bypassing SQL. group_id_ is seeded to mview_id_ (matches production
  // load_mview_sched_base_info). specified_refresh_interval_ must be > 0 so
  // group rebuild can calculate a periodic refresh interval.
  int add_node(uint64_t mv, const uint64_t *dep_arr, int64_t dep_cnt)
  {
    int ret = OB_SUCCESS;
    ObMViewSchedInfo *node = OB_NEWx(ObMViewSchedInfo, &mgr_.alloc_, mgr_.alloc_);
    if (OB_ISNULL(node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      node->mview_id_ = mv;
      node->group_id_ = mv;
      node->last_start_time_ = 0;
      node->specified_refresh_interval_ = DEFAULT_SPECIFIED_INTERVAL;
      // Two-phase build invariant: caller must add dep nodes before dependents.
      for (int64_t i = 0; OB_SUCC(ret) && i < dep_cnt; ++i) {
        ObMViewSchedInfo *dep_node = mgr_.get_sched_info(dep_arr[i]);
        if (OB_ISNULL(dep_node)) {
          // dep must be pre-inserted; propagate failure to the test assertion
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          ret = node->dep_mview_ids_.push_back(dep_arr[i]);
        }
      }
      if (OB_SUCC(ret)) {
        ret = mgr_.info_map_.set_refactored(mv, node);
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(node)) {
      OB_DELETEx(ObMViewSchedInfo, &mgr_.alloc_, node);
    }
    return ret;
  }

  // Wire a dep edge after both endpoints already exist.
  int add_dep(uint64_t mv, uint64_t dep_mv)
  {
    int ret = OB_SUCCESS;
    ObMViewSchedInfo *node = mgr_.get_sched_info(mv);
    ObMViewSchedInfo *dep_node = mgr_.get_sched_info(dep_mv);
    if (OB_ISNULL(node) || OB_ISNULL(dep_node)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ret = node->dep_mview_ids_.push_back(dep_mv);
    }
    return ret;
  }

  int recompute_full_with_intervals()
  {
    return mgr_.regroup_all_sched_infos();
  }

  bool is_top(uint64_t mv)
  {
    const ObMViewSchedInfo *node = mgr_.get_sched_info(mv);
    EXPECT_TRUE(nullptr != node);
    if (nullptr == node || OB_INVALID_ID == node->group_id_) {
      return false;
    }
    ObSEArray<uint64_t, 4> tops;
    if (OB_SUCCESS != get_group_tops(node->group_id_, tops)) {
      return false;
    }
    return has_exist_in_array(tops, mv);
  }

  int get_group_tops(uint64_t group_id, ObIArray<uint64_t> &tops)
  {
    tops.reset();
    ObMViewSchedGroupInfo *group_info = nullptr;
    int ret = mgr_.group_info_map_.get_refactored(group_id, group_info);
    if (OB_SUCC(ret)) {
      const sql::ObSqlArray<uint64_t> &arr = group_info->top_mview_ids_;
      for (int64_t i = 0; OB_SUCC(ret) && i < arr.count(); ++i) {
        ret = tops.push_back(arr.at(i));
      }
    }
    return ret;
  }

  int recompute_full()
  {
    return mgr_.regroup_all_sched_infos();
  }

  int recompute_scope(uint64_t scope_group_id)
  {
    int ret = OB_SUCCESS;
    ObSEArray<uint64_t, 8> scope_mview_ids;
    ObSEArray<ObMVGroupKey, 4> updated_groups;
    for (MViewSchedInfoMap::const_iterator it = mgr_.info_map_.begin();
         OB_SUCC(ret) && it != mgr_.info_map_.end();
         ++it) {
      const ObMViewSchedInfo *sched_info = it->second;
      if (OB_ISNULL(sched_info)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (scope_group_id != sched_info->group_id_) {
        // do nothing
      } else if (OB_FAIL(scope_mview_ids.push_back(sched_info->mview_id_))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mgr_.regroup_sched_infos(scope_mview_ids, updated_groups))) {
    }
    return ret;
  }

  // Expand dependency-related groups, then pass their MV IDs to the scoped regroup API.
  int recompute_scope_with_intervals(uint64_t scope_group_id)
  {
    int ret = OB_SUCCESS;
    ObSEArray<uint64_t, 8> scope_mview_ids;
    ObSEArray<uint64_t, 4> scope_group_ids;
    ObSEArray<ObMVGroupKey, 4> new_group_ids;
    if (OB_FAIL(scope_group_ids.push_back(scope_group_id))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < scope_group_ids.count(); ++i) {
      const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(scope_group_ids.at(i));
      const int64_t old_count = scope_mview_ids.count();
      if (OB_ISNULL(group_info)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order(
                     mgr_.info_map_, group_info->top_mview_ids_, scope_mview_ids))) {
      }
      for (int64_t j = old_count; OB_SUCC(ret) && j < scope_mview_ids.count(); ++j) {
        const ObMViewSchedInfo *sched_info = mgr_.get_sched_info(scope_mview_ids.at(j));
        if (OB_ISNULL(sched_info)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(add_var_to_array_no_dup(scope_group_ids, sched_info->group_id_))) {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mgr_.regroup_sched_infos(scope_mview_ids, new_group_ids))) {
    }
    return ret;
  }

  int recompute_on_insert(uint64_t mv)
  {
    int ret = OB_SUCCESS;
    ObSEArray<uint64_t, 8> scope_mview_ids;
    ObSEArray<ObMVGroupKey, 4> updated_groups;
    if (OB_FAIL(ObMViewSchedInfoUtils::collect_related_mview_ids(TENANT_ID,
                                                                 mgr_.info_map_,
                                                                 mv,
                                                                 scope_mview_ids))) {
    } else if (OB_FAIL(mgr_.regroup_sched_infos(scope_mview_ids, updated_groups))) {
    }
    return ret;
  }

  uint64_t gid(uint64_t mv)
  {
    const ObMViewSchedInfo *node = mgr_.get_sched_info(mv);
    EXPECT_TRUE(nullptr != node);
    return nullptr == node ? OB_INVALID_ID : node->group_id_;
  }

  bool is_group_schedulable(uint64_t group_id)
  {
    const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(group_id);
    EXPECT_NE(nullptr, group_info);
    return nullptr != group_info && group_info->is_schedulable_;
  }

  void reload_scheduler_suspended(const uint64_t *suspended_mview_ids,
                                  const int64_t suspended_mview_count)
  {
    for (ObMViewSchedInfoMgr::SchedInfoMapIter it = mgr_.info_map_.begin();
         it != mgr_.info_map_.end(); ++it) {
      ASSERT_NE(nullptr, it->second);
      it->second->scheduler_suspended_ = false;
    }
    for (int64_t i = 0; i < suspended_mview_count; ++i) {
      ObMViewSchedInfo *node = mgr_.get_sched_info(suspended_mview_ids[i]);
      ASSERT_NE(nullptr, node);
      node->scheduler_suspended_ = true;
    }
  }

  bool scheduler_suspended(const uint64_t mv)
  {
    const ObMViewSchedInfo *node = mgr_.get_sched_info(mv);
    EXPECT_NE(nullptr, node);
    return nullptr != node && node->scheduler_suspended_;
  }

  ObMViewSchedInfoMgr mgr_;
};

// Baseline: a single node with no deps stays in its own group.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, IsolatedNode)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_EQ(10UL, gid(10));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, SchedulerSuspendCascadesThroughDependencies)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  const uint64_t first_suspended[] = {10};
  reload_scheduler_suspended(first_suspended, ARRAYSIZEOF(first_suspended));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_TRUE(scheduler_suspended(10));
  EXPECT_FALSE(scheduler_suspended(20));
  EXPECT_FALSE(scheduler_suspended(30));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops));
  EXPECT_FALSE(is_group_schedulable(30));

  const uint64_t second_suspended[] = {20};
  reload_scheduler_suspended(second_suspended, ARRAYSIZEOF(second_suspended));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_FALSE(scheduler_suspended(10));
  EXPECT_TRUE(scheduler_suspended(20));
  EXPECT_FALSE(scheduler_suspended(30));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(10UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops));
  EXPECT_FALSE(is_group_schedulable(30));

  reload_scheduler_suspended(nullptr, 0);
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_FALSE(scheduler_suspended(10));
  EXPECT_FALSE(scheduler_suspended(20));
  EXPECT_FALSE(scheduler_suspended(30));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(30UL, tops.at(0));
  EXPECT_TRUE(is_group_schedulable(30));

}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, SchedulerSuspendWaitsForAllUpstreamsToResume)
{
  const uint64_t deps_30[] = {10, 20};
  const uint64_t deps_40[] = {30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 2));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  const uint64_t both_suspended[] = {10, 20};
  reload_scheduler_suspended(both_suspended, ARRAYSIZEOF(both_suspended));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_FALSE(scheduler_suspended(30));
  EXPECT_FALSE(scheduler_suspended(40));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(40, tops));
  EXPECT_FALSE(is_group_schedulable(40));

  const uint64_t one_suspended[] = {20};
  reload_scheduler_suspended(one_suspended, ARRAYSIZEOF(one_suspended));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_FALSE(scheduler_suspended(10));
  EXPECT_TRUE(scheduler_suspended(20));
  EXPECT_FALSE(scheduler_suspended(30));
  EXPECT_FALSE(scheduler_suspended(40));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(10UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(40, tops));
  EXPECT_FALSE(is_group_schedulable(40));

  reload_scheduler_suspended(nullptr, 0);
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_FALSE(scheduler_suspended(30));
  EXPECT_FALSE(scheduler_suspended(40));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(40, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(40UL, tops.at(0));
  EXPECT_TRUE(is_group_schedulable(40));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, SchedulerStateChangeRebuildsGroups)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_EQ(30UL, gid(10));
  EXPECT_EQ(30UL, gid(20));
  EXPECT_EQ(30UL, gid(30));

  const uint64_t suspended_mview_ids[] = {20};
  reload_scheduler_suspended(suspended_mview_ids, ARRAYSIZEOF(suspended_mview_ids));
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(30));
  EXPECT_EQ(10UL, gid(10));
  EXPECT_EQ(30UL, gid(20));
  EXPECT_EQ(30UL, gid(30));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(10UL, tops.at(0));
  EXPECT_TRUE(is_group_schedulable(10));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(30UL, tops.at(0));
  EXPECT_FALSE(is_group_schedulable(30));
  int64_t next_trigger_ts = 0;
  const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(30);
  ASSERT_NE(nullptr, group_info);
  const ObMVGroupKey group_key(30, group_info->modified_ts_);
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_sched_group_timer_info(group_key, next_trigger_ts));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, next_trigger_ts);
  bool is_valid = true;
  ASSERT_EQ(OB_SUCCESS, mgr_.check_sched_group_valid(30, group_key.modified_ts_, is_valid));
  EXPECT_FALSE(is_valid);

  reload_scheduler_suspended(nullptr, 0);
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(30));
  EXPECT_EQ(30UL, gid(10));
  EXPECT_EQ(30UL, gid(20));
  EXPECT_EQ(30UL, gid(30));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(30UL, tops.at(0));
  EXPECT_TRUE(is_group_schedulable(30));

  reload_scheduler_suspended(suspended_mview_ids, ARRAYSIZEOF(suspended_mview_ids));
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(30));
  EXPECT_EQ(10UL, gid(10));
  EXPECT_EQ(30UL, gid(20));
  EXPECT_EQ(30UL, gid(30));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, SchedulerTopsRemoveSharedRunnableAncestor)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  const uint64_t suspended_mview_ids[] = {30};
  reload_scheduler_suspended(suspended_mview_ids, ARRAYSIZEOF(suspended_mview_ids));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(20, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(20UL, tops.at(0));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, SchedulerBlockedMapSplitsGroups)
{
  const uint64_t deps_a[] = {20, 30};
  const uint64_t deps_d[] = {30, 50};
  const uint64_t deps_f[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0)); // B
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0)); // C
  ASSERT_EQ(OB_SUCCESS, add_node(50, nullptr, 0)); // E
  ASSERT_EQ(OB_SUCCESS, add_node(10, deps_a, 2));  // A -> {B, C}
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_d, 2));  // D -> {C, E}
  ASSERT_EQ(OB_SUCCESS, add_node(60, deps_f, 1));  // F -> A
  mgr_.get_sched_info(30)->scheduler_suspended_ = true;

  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_EQ(60UL, gid(10));
  EXPECT_EQ(20UL, gid(20));
  EXPECT_EQ(60UL, gid(30));
  EXPECT_EQ(60UL, gid(40));
  EXPECT_EQ(50UL, gid(50));
  EXPECT_EQ(60UL, gid(60));
  EXPECT_FALSE(is_group_schedulable(60));
  EXPECT_TRUE(is_group_schedulable(20));
  EXPECT_TRUE(is_group_schedulable(50));

  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(60, tops));
  ASSERT_EQ(2, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 40UL));
  EXPECT_TRUE(has_exist_in_array(tops, 60UL));
  ObSEArray<ObMVGroupKey, 4> group_ids;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(3, group_ids.count());
}

// Two nodes with a single edge share the group keyed by the maximum mview id.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TwoNodesSingleEdge)
{
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_EQ(20UL, gid(10));
  EXPECT_EQ(20UL, gid(20));
}

// Three-node chain A(10) -> B(20) -> C(30).
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, ChainNodesShareRootGroupId)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_EQ(30UL, gid(10));
  EXPECT_EQ(30UL, gid(20));
  EXPECT_EQ(30UL, gid(30));
}

// ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order walks deps first, then appends self.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, GetMviewIdsInRefreshOrderChain)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());

  ObSEArray<uint64_t, 4> ids;
  const int64_t current_time = 1000;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_mview_ids_in_refresh_order(30, OB_INVALID_ID, current_time, ids));
  ASSERT_EQ(3, ids.count());
  EXPECT_EQ(10UL, ids.at(0));
  EXPECT_EQ(20UL, ids.at(1));
  EXPECT_EQ(30UL, ids.at(2));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, ManualRefreshKeepsSuspendedDependencyOrder)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  const uint64_t suspended_mview_ids[] = {10};
  reload_scheduler_suspended(suspended_mview_ids, ARRAYSIZEOF(suspended_mview_ids));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  ObSEArray<uint64_t, 4> ids;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_mview_ids_in_refresh_order(30, OB_INVALID_ID, 1000, ids));
  ASSERT_EQ(3, ids.count());
  EXPECT_EQ(10UL, ids.at(0));
  EXPECT_EQ(20UL, ids.at(1));
  EXPECT_EQ(30UL, ids.at(2));

  int64_t next_trigger_ts = 0;
  const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(30);
  ASSERT_NE(nullptr, group_info);
  const ObMVGroupKey group_key(30, group_info->modified_ts_);
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_sched_group_timer_info(group_key, next_trigger_ts));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, next_trigger_ts);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, AutomaticGroupSkipsSuspendedTop)
{
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  const uint64_t suspended_mview_ids[] = {20};
  reload_scheduler_suspended(suspended_mview_ids, ARRAYSIZEOF(suspended_mview_ids));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  ObSEArray<uint64_t, 2> ids;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_mview_ids_in_refresh_order(OB_INVALID_ID, 10, 1000000, ids));
  ASSERT_EQ(1, ids.count());
  EXPECT_EQ(10UL, ids.at(0));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, GetMviewIdsInRefreshOrderByGroupId)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  ObSEArray<uint64_t, 4> ids;
  const int64_t current_time = 2000000;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_mview_ids_in_refresh_order(OB_INVALID_ID, 30, current_time, ids));
  ASSERT_GE(ids.count(), 3);
  EXPECT_EQ(10UL, ids.at(0));
  EXPECT_EQ(20UL, ids.at(1));
  EXPECT_EQ(30UL, ids.at(2));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, GetMviewIdsInRefreshOrderDoesNotUpdateLastStartTime)
{
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  ObMViewSchedInfo *dep = mgr_.get_sched_info(10);
  ObMViewSchedInfo *top = mgr_.get_sched_info(20);
  ASSERT_NE(nullptr, dep);
  ASSERT_NE(nullptr, top);
  dep->last_start_time_ = 100;
  top->last_start_time_ = 200;

  ObSEArray<uint64_t, 4> ids;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_mview_ids_in_refresh_order(20, OB_INVALID_ID, 300, ids));
  ASSERT_EQ(2, ids.count());
  EXPECT_EQ(10UL, ids.at(0));
  EXPECT_EQ(20UL, ids.at(1));
  EXPECT_EQ(100, dep->last_start_time_);
  EXPECT_EQ(200, top->last_start_time_);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, ManagerUsesLcmIntervalCalc)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ObMViewSchedInfo *dep = mgr_.get_sched_info(10);
  ObMViewSchedInfo *child = mgr_.get_sched_info(20);
  ASSERT_NE(nullptr, dep);
  ASSERT_NE(nullptr, child);
  dep->specified_refresh_interval_ = 95 * base_unit;
  child->specified_refresh_interval_ = 100 * base_unit;

  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  EXPECT_EQ(95 * base_unit, dep->refresh_interval_);
  EXPECT_EQ(95 * base_unit, child->refresh_interval_);
  ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(20);
  ASSERT_NE(nullptr, group_info);
  EXPECT_EQ(95 * base_unit, group_info->refresh_interval_);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CheckGroupHasNonTerminalDependentDirect)
{
  static constexpr uint64_t LEAF = 1001UL;
  static constexpr uint64_t ROOT = 1003UL;
  const uint64_t root_deps[] = { LEAF };
  ASSERT_EQ(OB_SUCCESS, add_node(LEAF, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(ROOT, root_deps, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());

  ObArenaAllocator alloc;
  ObMViewPendingTaskGroup *group = nullptr;
  ObMViewPendingTaskGroup::GroupInfo info;
  info.tenant_id_ = TENANT_ID;
  info.refresh_id_ = 100;
  ObMViewPendingTask leaf = make_task(LEAF);
  leaf.status_ = MV_TASK_SUCCESS;
  ObMViewPendingTask root = make_task(ROOT, 0);
  root.status_ = MV_TASK_PENDING;
  ObSEArray<ObMViewPendingTask, 2> tasks;
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(leaf));
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(root));
  ASSERT_EQ(OB_SUCCESS, ObMViewPendingTaskGroup::alloc_and_build(alloc, info, tasks, group));

  bool has_dependent = false;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_group_has_non_terminal_dependent(*group, LEAF, has_dependent));
  EXPECT_TRUE(has_dependent);
}

// Regression: once a non-terminal direct dependent is found, later tasks in the
// backward scan must not reset has_dependent via check_mview_has_dep.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CheckGroupHasNonTerminalDependentPreservesFoundDep)
{
  static constexpr uint64_t LEAF = 1001UL;
  static constexpr uint64_t MID  = 1002UL;
  static constexpr uint64_t ROOT = 1003UL;
  const uint64_t root_deps[] = { LEAF };
  ASSERT_EQ(OB_SUCCESS, add_node(LEAF, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(MID, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(ROOT, root_deps, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());

  ObArenaAllocator alloc;
  ObMViewPendingTaskGroup *group = nullptr;
  ObMViewPendingTaskGroup::GroupInfo info;
  info.tenant_id_ = TENANT_ID;
  info.refresh_id_ = 100;
  ObMViewPendingTask leaf = make_task(LEAF);
  leaf.status_ = MV_TASK_SUCCESS;
  ObMViewPendingTask mid = make_task(MID, 0);
  mid.status_ = MV_TASK_PENDING;
  ObMViewPendingTask root = make_task(ROOT, 0);
  root.status_ = MV_TASK_PENDING;
  ObSEArray<ObMViewPendingTask, 3> tasks;
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(leaf));
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(mid));
  ASSERT_EQ(OB_SUCCESS, tasks.push_back(root));
  ASSERT_EQ(OB_SUCCESS, ObMViewPendingTaskGroup::alloc_and_build(alloc, info, tasks, group));

  bool has_dependent = false;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_group_has_non_terminal_dependent(*group, LEAF, has_dependent));
  EXPECT_TRUE(has_dependent);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CheckMviewHasDepDirect)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());

  bool has_dep = false;
  ASSERT_EQ(OB_SUCCESS, mgr_.check_mview_has_dep(20, 10, has_dep));
  EXPECT_TRUE(has_dep);

  has_dep = true;
  ASSERT_EQ(OB_SUCCESS, mgr_.check_mview_has_dep(20, 30, has_dep));
  EXPECT_FALSE(has_dep);

  has_dep = true;
  ASSERT_EQ(OB_SUCCESS, mgr_.check_mview_has_dep(10, 20, has_dep));
  EXPECT_FALSE(has_dep);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CheckMviewDepsStatusDirect)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());

  ObSEArray<uint64_t, 4> succ_ids;
  ObSEArray<uint64_t, 4> aborted_ids;
  bool deps_satisfied = true;
  bool has_aborted_dep = true;
  bool mview_exists = false;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(10, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_TRUE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);

  succ_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(20, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_FALSE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);

  ASSERT_EQ(OB_SUCCESS, succ_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(20, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_TRUE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);

  succ_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(30, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_FALSE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);

  ASSERT_EQ(OB_SUCCESS, succ_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(30, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_FALSE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);

  ASSERT_EQ(OB_SUCCESS, succ_ids.push_back(20));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(30, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_TRUE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);

  succ_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, aborted_ids.push_back(20));
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(30, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_FALSE(deps_satisfied);
  EXPECT_TRUE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);

  deps_satisfied = true;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(9999, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_FALSE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_FALSE(mview_exists);

  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_mview_deps_status(10, succ_ids, aborted_ids,
                                         deps_satisfied, has_aborted_dep, mview_exists));
  EXPECT_TRUE(deps_satisfied);
  EXPECT_FALSE(has_aborted_dep);
  EXPECT_TRUE(mview_exists);
}

// Diamond A(10) -> B(20), A(10) -> C(30), B(20) -> D(40), C(30) -> D(40).
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, DiamondSingleGroup)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {10};
  const uint64_t deps_40[] = {20, 30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_EQ(40UL, gid(10));
  EXPECT_EQ(40UL, gid(20));
  EXPECT_EQ(40UL, gid(30));
  EXPECT_EQ(40UL, gid(40));
}

// Two disjoint subgraphs stay separate.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, DisjointSubgraphs)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_40[] = {30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_EQ(20UL, gid(10));
  EXPECT_EQ(20UL, gid(20));
  EXPECT_EQ(40UL, gid(30));
  EXPECT_EQ(40UL, gid(40));
}

// Cycle A(10) -> B(20), B(20) -> A(10) is rejected without hanging.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CycleMergesWithoutHang)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_dep(10, 20));
  ASSERT_EQ(OB_SUCCESS, add_dep(20, 10));
  EXPECT_EQ(OB_ERR_UNEXPECTED, recompute_full());
}

// A dep pointing to a non-existent mview is silently skipped (non-mview table).
// Two-phase build invariant: all deps must be pre-registered mview nodes.
// A dep_id that is not in info_map_ (e.g. a non-mview base table id or a
// mview not yet loaded) is a build-invariant violation and add_node fails.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, DepOutsideMapRejectedByAddNode)
{
  const uint64_t deps_10[] = {999};
  ASSERT_NE(OB_SUCCESS, add_node(10, deps_10, 1));
}

// Duplicate edges are idempotent under UF.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, DuplicateDepEdgesIdempotent)
{
  const uint64_t deps_20[] = {10, 10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_EQ(20UL, gid(10));
  EXPECT_EQ(20UL, gid(20));
}

// insert path: two disjoint singletons collapse into one group when a new
// node depends on both.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, InsertMergesDisjointGroups)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_EQ(10UL, gid(10));
  ASSERT_EQ(20UL, gid(20));

  // New node 30 depends on both 10 and 20.
  const uint64_t deps_30[] = {10, 20};
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_on_insert(30));
  EXPECT_EQ(30UL, gid(10));
  EXPECT_EQ(30UL, gid(20));
  EXPECT_EQ(30UL, gid(30));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, ActivateMergeReturnsUpdatedGroups)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  const uint64_t deps_30[] = {10, 20};
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 2));

  MockMySQLProxy proxy;
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, mgr_.activate_mview_sched_info(proxy, 30, updated_groups));
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(30UL, updated_groups.at(0).group_id_);
  EXPECT_EQ(30UL, gid(10));
  EXPECT_EQ(30UL, gid(20));
  EXPECT_EQ(30UL, gid(30));
  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(10));
  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(20));
  const ObMViewSchedGroupInfo *merged_group = mgr_.get_sched_group_info(30);
  ASSERT_NE(nullptr, merged_group);
  EXPECT_EQ(merged_group->modified_ts_, updated_groups.at(0).modified_ts_);
}

// Every key returned by a scoped regroup identifies the group incarnation that
// was just published, so it must match the modified_ts_ stored in the map.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RemoveSplitReturnsKeysMatchingRepublishedGroups)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  const uint64_t deps_30[] = {10, 20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_EQ(30UL, gid(10));
  ASSERT_EQ(30UL, gid(20));

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(30, updated_groups));
  ASSERT_EQ(2, updated_groups.count());
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(updated_groups, 10UL));
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(updated_groups, 20UL));
  EXPECT_FALSE(ObMVGroupKey::contains_group_id(updated_groups, 30UL));
  for (int64_t i = 0; i < updated_groups.count(); ++i) {
    const ObMVGroupKey &group_key = updated_groups.at(i);
    const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(group_key.group_id_);
    ASSERT_NE(nullptr, group_info);
    EXPECT_NE(OB_INVALID_TIMESTAMP, group_key.modified_ts_);
    EXPECT_EQ(group_info->modified_ts_, group_key.modified_ts_);
  }
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, GetSchedGroupKeysReturnsEveryPublishedGroup)
{
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  ObSEArray<ObMVGroupKey, 4> group_keys;
  ASSERT_EQ(OB_SUCCESS, group_keys.push_back(ObMVGroupKey(999UL, 1)));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_keys));
  ASSERT_EQ(2, group_keys.count());
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(group_keys, 20UL));
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(group_keys, 30UL));
  EXPECT_FALSE(ObMVGroupKey::contains_group_id(group_keys, 10UL));
  EXPECT_FALSE(ObMVGroupKey::contains_group_id(group_keys, 999UL));
  for (int64_t i = 0; i < group_keys.count(); ++i) {
    const ObMVGroupKey &group_key = group_keys.at(i);
    const ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(group_key.group_id_);
    ASSERT_NE(nullptr, group_info);
    EXPECT_EQ(group_info->modified_ts_, group_key.modified_ts_);
    int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
    ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_timer_info(group_key, next_trigger_ts));
    EXPECT_EQ(DEFAULT_SPECIFIED_INTERVAL, next_trigger_ts);
  }
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, GetSchedGroupTimerInfoMissingGroupReturnsEntryNotExist)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  int64_t next_trigger_ts = 123;
  const ObMVGroupKey missing_group_key(999UL, 1);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            mgr_.get_sched_group_timer_info(missing_group_key, next_trigger_ts));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, next_trigger_ts);

  const ObMVGroupKey invalid_group_key;
  next_trigger_ts = 123;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            mgr_.get_sched_group_timer_info(invalid_group_key, next_trigger_ts));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, next_trigger_ts);
}

// A key from an older incarnation stays stale even though the group still
// exists, while the key of the current incarnation keeps scheduling.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, GetSchedGroupTimerInfoRepublishInvalidatesOldKey)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  ObSEArray<ObMVGroupKey, 4> old_group_keys;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(old_group_keys));
  ASSERT_EQ(1, old_group_keys.count());
  const ObMVGroupKey old_group_key = old_group_keys.at(0);

  ObMViewSchedGroupInfo *group_info = mgr_.get_sched_group_info(10);
  ASSERT_NE(nullptr, group_info);
  group_info->modified_ts_ = old_group_key.modified_ts_ + 1;
  const ObMVGroupKey new_group_key(10UL, group_info->modified_ts_);

  int64_t next_trigger_ts = 123;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_timer_info(old_group_key, next_trigger_ts));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, next_trigger_ts);
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_timer_info(new_group_key, next_trigger_ts));
  EXPECT_EQ(DEFAULT_SPECIFIED_INTERVAL, next_trigger_ts);

  bool is_valid = true;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_sched_group_valid(10, old_group_key.modified_ts_, is_valid));
  EXPECT_FALSE(is_valid);
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_sched_group_valid(10, new_group_key.modified_ts_, is_valid));
  EXPECT_TRUE(is_valid);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, SharedDependencyGraphSuspendResumeAndDelete)
{
  const uint64_t deps_c[] = {10, 20};
  const uint64_t deps_d[] = {20, 50};
  const uint64_t deps_f[] = {30};
  const uint64_t deps_g[] = {50};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0)); // A
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0)); // B
  ASSERT_EQ(OB_SUCCESS, add_node(50, nullptr, 0)); // E
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_c, 2));  // C -> {A, B}
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_d, 2));  // D -> {B, E}
  ASSERT_EQ(OB_SUCCESS, add_node(60, deps_f, 1));  // F -> C
  ASSERT_EQ(OB_SUCCESS, add_node(70, deps_g, 1));  // G -> E

  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  EXPECT_EQ(70UL, gid(10));
  EXPECT_EQ(70UL, gid(20));
  EXPECT_EQ(70UL, gid(30));
  EXPECT_EQ(70UL, gid(40));
  EXPECT_EQ(70UL, gid(50));
  EXPECT_EQ(70UL, gid(60));
  EXPECT_EQ(70UL, gid(70));
  EXPECT_TRUE(is_group_schedulable(70));
  ObSEArray<uint64_t, 8> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(70, tops));
  EXPECT_EQ(3, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 40UL));
  EXPECT_TRUE(has_exist_in_array(tops, 60UL));
  EXPECT_TRUE(has_exist_in_array(tops, 70UL));
  ObSEArray<ObMVGroupKey, 8> group_ids;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(1, group_ids.count());

  const uint64_t suspended_e[] = {50};
  reload_scheduler_suspended(suspended_e, ARRAYSIZEOF(suspended_e));
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(70));
  EXPECT_EQ(60UL, gid(10));
  EXPECT_EQ(60UL, gid(20));
  EXPECT_EQ(60UL, gid(30));
  EXPECT_EQ(70UL, gid(40));
  EXPECT_EQ(70UL, gid(50));
  EXPECT_EQ(60UL, gid(60));
  EXPECT_EQ(70UL, gid(70));
  EXPECT_TRUE(is_group_schedulable(60));
  EXPECT_FALSE(is_group_schedulable(70));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(60, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(60UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(70, tops));
  EXPECT_EQ(2, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 40UL));
  EXPECT_TRUE(has_exist_in_array(tops, 70UL));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(2, group_ids.count());

  const uint64_t suspended_e_and_b[] = {50, 20};
  reload_scheduler_suspended(suspended_e_and_b, ARRAYSIZEOF(suspended_e_and_b));
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(60));
  EXPECT_EQ(10UL, gid(10));
  EXPECT_EQ(60UL, gid(20));
  EXPECT_EQ(60UL, gid(30));
  EXPECT_EQ(70UL, gid(40));
  EXPECT_EQ(70UL, gid(50));
  EXPECT_EQ(60UL, gid(60));
  EXPECT_EQ(70UL, gid(70));
  EXPECT_TRUE(is_group_schedulable(10));
  EXPECT_FALSE(is_group_schedulable(60));
  EXPECT_FALSE(is_group_schedulable(70));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(10UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(60, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(60UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(70, tops));
  EXPECT_EQ(2, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 40UL));
  EXPECT_TRUE(has_exist_in_array(tops, 70UL));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(3, group_ids.count());
  const uint64_t suspended_b[] = {20};
  reload_scheduler_suspended(suspended_b, ARRAYSIZEOF(suspended_b));
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(70));
  EXPECT_EQ(10UL, gid(10));
  EXPECT_EQ(60UL, gid(20));
  EXPECT_EQ(60UL, gid(30));
  EXPECT_EQ(60UL, gid(40));
  EXPECT_EQ(70UL, gid(50));
  EXPECT_EQ(60UL, gid(60));
  EXPECT_EQ(70UL, gid(70));
  EXPECT_TRUE(is_group_schedulable(10));
  EXPECT_FALSE(is_group_schedulable(60));
  EXPECT_TRUE(is_group_schedulable(70));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(10UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(60, tops));
  EXPECT_EQ(2, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 40UL));
  EXPECT_TRUE(has_exist_in_array(tops, 60UL));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(70, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(70UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(3, group_ids.count());

  reload_scheduler_suspended(nullptr, 0);
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(60));
  EXPECT_EQ(70UL, gid(10));
  EXPECT_EQ(70UL, gid(20));
  EXPECT_EQ(70UL, gid(30));
  EXPECT_EQ(70UL, gid(40));
  EXPECT_EQ(70UL, gid(50));
  EXPECT_EQ(70UL, gid(60));
  EXPECT_EQ(70UL, gid(70));
  EXPECT_TRUE(is_group_schedulable(70));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(70, tops));
  EXPECT_EQ(3, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 40UL));
  EXPECT_TRUE(has_exist_in_array(tops, 60UL));
  EXPECT_TRUE(has_exist_in_array(tops, 70UL));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(1, group_ids.count());

  ObSEArray<ObMVGroupKey, 8> update_group_ids;
  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(40, update_group_ids));
  EXPECT_EQ(nullptr, mgr_.get_sched_info(40));
  EXPECT_EQ(2, update_group_ids.count());
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(update_group_ids, 60UL));
  EXPECT_TRUE(ObMVGroupKey::contains_group_id(update_group_ids, 70UL));
  EXPECT_EQ(60UL, gid(10));
  EXPECT_EQ(60UL, gid(20));
  EXPECT_EQ(60UL, gid(30));
  EXPECT_EQ(70UL, gid(50));
  EXPECT_EQ(60UL, gid(60));
  EXPECT_EQ(70UL, gid(70));
  EXPECT_TRUE(is_group_schedulable(60));
  EXPECT_TRUE(is_group_schedulable(70));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(60, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(60UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(70, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(70UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(2, group_ids.count());

  reload_scheduler_suspended(suspended_b, ARRAYSIZEOF(suspended_b));
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(60));
  EXPECT_FALSE(is_group_schedulable(gid(20)));
  EXPECT_TRUE(is_group_schedulable(gid(50)));
  ASSERT_EQ(OB_SUCCESS, add_node(80, deps_d, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_on_insert(80));
  EXPECT_EQ(10UL, gid(10));
  EXPECT_EQ(80UL, gid(20));
  EXPECT_EQ(80UL, gid(30));
  EXPECT_EQ(70UL, gid(50));
  EXPECT_EQ(80UL, gid(60));
  EXPECT_EQ(70UL, gid(70));
  EXPECT_EQ(80UL, gid(80));
  EXPECT_TRUE(is_group_schedulable(10));
  EXPECT_FALSE(is_group_schedulable(80));
  EXPECT_TRUE(is_group_schedulable(70));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(80, tops));
  EXPECT_EQ(2, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 60UL));
  EXPECT_TRUE(has_exist_in_array(tops, 80UL));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(3, group_ids.count());

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(80, update_group_ids));
  EXPECT_EQ(nullptr, mgr_.get_sched_info(80));
  reload_scheduler_suspended(nullptr, 0);
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(60));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(2, group_ids.count());

  reload_scheduler_suspended(suspended_e, ARRAYSIZEOF(suspended_e));
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(70));
  EXPECT_TRUE(is_group_schedulable(gid(20)));
  EXPECT_FALSE(is_group_schedulable(gid(50)));
  ASSERT_EQ(OB_SUCCESS, add_node(90, deps_d, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_on_insert(90));
  EXPECT_EQ(60UL, gid(10));
  EXPECT_EQ(60UL, gid(20));
  EXPECT_EQ(60UL, gid(30));
  EXPECT_EQ(90UL, gid(50));
  EXPECT_EQ(60UL, gid(60));
  EXPECT_EQ(90UL, gid(70));
  EXPECT_EQ(90UL, gid(90));
  EXPECT_TRUE(is_group_schedulable(60));
  EXPECT_FALSE(is_group_schedulable(90));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(60, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(60UL, tops.at(0));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(90, tops));
  EXPECT_EQ(2, tops.count());
  EXPECT_TRUE(has_exist_in_array(tops, 70UL));
  EXPECT_TRUE(has_exist_in_array(tops, 90UL));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(2, group_ids.count());

  reload_scheduler_suspended(nullptr, 0);
  ASSERT_EQ(OB_SUCCESS, recompute_scope_with_intervals(90));
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_ids));
  EXPECT_EQ(1, group_ids.count());
  EXPECT_EQ(90UL, gid(10));
  EXPECT_EQ(90UL, gid(20));
  EXPECT_EQ(90UL, gid(30));
  EXPECT_EQ(90UL, gid(50));
  EXPECT_EQ(90UL, gid(60));
  EXPECT_EQ(90UL, gid(70));
  EXPECT_EQ(90UL, gid(90));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RepeatedLargeFullAndScopedRebuildKeepsStableTraversal)
{
  static const int64_t NODE_COUNT = 256;
  for (uint64_t mview_id = 1; mview_id <= NODE_COUNT; ++mview_id) {
    const uint64_t dep_mview_id = mview_id - 1;
    const uint64_t *dep_mview_ids = (1 == mview_id) ? nullptr : &dep_mview_id;
    const int64_t dep_count = (1 == mview_id) ? 0 : 1;
    ASSERT_EQ(OB_SUCCESS, add_node(mview_id, dep_mview_ids, dep_count));
  }

  for (int64_t i = 0; i < 8; ++i) {
    ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  }
  for (uint64_t mview_id = 1; mview_id <= NODE_COUNT; ++mview_id) {
    ObMViewSchedInfo *sched_info = mgr_.get_sched_info(mview_id);
    ASSERT_NE(nullptr, sched_info);
    sched_info->group_id_ = NODE_COUNT;
  }
  for (int64_t i = 0; i < 8; ++i) {
    ASSERT_EQ(OB_SUCCESS, recompute_scope(NODE_COUNT));
  }
  EXPECT_TRUE(is_top(NODE_COUNT));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(NODE_COUNT, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(static_cast<uint64_t>(NODE_COUNT), tops.at(0));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, InsertIntervalFailureSkipsFailedGroup)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ObMViewSchedInfo *old_node = mgr_.get_sched_info(10);
  ASSERT_NE(nullptr, old_node);
  old_node->specified_refresh_interval_ = 4 * base_unit;
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  ASSERT_NE(nullptr, mgr_.get_sched_group_info(10));
  ASSERT_EQ(4 * base_unit, old_node->refresh_interval_);

  const uint64_t invalid_deps[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(20, invalid_deps, 1));
  ObMViewSchedInfo *invalid_node = mgr_.get_sched_info(20);
  ASSERT_NE(nullptr, invalid_node);
  invalid_node->specified_refresh_interval_ = base_unit;
  EXPECT_EQ(OB_SUCCESS, recompute_on_insert(20));

  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(10));
  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(20));
  EXPECT_FALSE(is_top(10));
  EXPECT_EQ(4 * base_unit, old_node->refresh_interval_);
  EXPECT_EQ(static_cast<int64_t>(ObMViewSchedInfo::NO_PERIODIC_SCHEDULE_INTERVAL),
            invalid_node->refresh_interval_);

  mgr_.remove_sched_info_from_map(20);
  ASSERT_EQ(nullptr, mgr_.get_sched_info(20));
  const uint64_t valid_deps[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(30, valid_deps, 1));
  ObMViewSchedInfo *valid_node = mgr_.get_sched_info(30);
  ASSERT_NE(nullptr, valid_node);
  valid_node->specified_refresh_interval_ = 8 * base_unit;
  ASSERT_EQ(OB_SUCCESS, recompute_on_insert(30));
  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(10));
  EXPECT_NE(nullptr, mgr_.get_sched_group_info(30));
  EXPECT_TRUE(is_top(30));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, BuildAndPublishFailureKeepsOldPublishedState)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  ObMViewSchedInfo *node = mgr_.get_sched_info(10);
  ObMViewSchedGroupInfo *old_group = mgr_.get_sched_group_info(10);
  ASSERT_NE(nullptr, node);
  ASSERT_NE(nullptr, old_group);
  const int64_t old_refresh_interval = node->refresh_interval_;
  const int64_t old_group_refresh_interval = old_group->refresh_interval_;
  const int64_t old_modified_ts = old_group->modified_ts_;
  ObSEArray<uint64_t, 1> top_mview_ids;
  ObSEArray<uint64_t, 2> all_mview_ids;
  ObMVGroupKey group_key(10, 100);
  ASSERT_EQ(OB_SUCCESS, top_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, all_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, all_mview_ids.push_back(999));

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            mgr_.build_and_publish_sched_group(10, top_mview_ids, all_mview_ids, group_key));
  EXPECT_EQ(OB_INVALID_ID, group_key.group_id_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, group_key.modified_ts_);
  EXPECT_EQ(old_group, mgr_.get_sched_group_info(10));
  EXPECT_EQ(10UL, node->group_id_);
  EXPECT_EQ(old_refresh_interval, node->refresh_interval_);
  EXPECT_EQ(old_group_refresh_interval, old_group->refresh_interval_);
  EXPECT_EQ(old_modified_ts, old_group->modified_ts_);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, FullRegroupContinuesAfterInvalidGroup)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_dep(10, 20));
  ASSERT_EQ(OB_SUCCESS, add_dep(20, 10));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));

  ASSERT_EQ(OB_ERR_UNEXPECTED, recompute_full_with_intervals());
}

// Removing a mid-chain node (has a dependent → not top) is forbidden:
// dep_mview_ids_ pointers held by the upstream node would dangle.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RemoveMidChainRejected)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_FALSE(is_top(20));

  EXPECT_EQ(OB_EAGAIN, mgr_.remove_sched_info(20, updated_groups));
}

// remove path: A(10) -> B(20). Removing B leaves A as an isolated singleton.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RemoveTriggersGroupIdDownshift)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_EQ(20UL, gid(10));

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(20, updated_groups));
  EXPECT_EQ(10UL, gid(10));
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(10UL, updated_groups.at(0).group_id_);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RemoveTopPromotesDependencyAndIsIdempotent)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  // Production-loaded nodes keep the invalid group id until the first full calculation.
  mgr_.get_sched_info(10)->group_id_ = OB_INVALID_ID;
  mgr_.get_sched_info(20)->group_id_ = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  ASSERT_EQ(20UL, gid(10));
  ASSERT_EQ(20UL, gid(20));
  ASSERT_FALSE(is_top(10));
  ASSERT_TRUE(is_top(20));

  EXPECT_EQ(OB_EAGAIN, mgr_.remove_sched_info(10, updated_groups));
  EXPECT_EQ(20UL, gid(10));
  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(20, updated_groups));
  ASSERT_TRUE(is_top(10));
  EXPECT_EQ(10UL, gid(10));
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(10UL, updated_groups.at(0).group_id_);

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(10, updated_groups));
  EXPECT_EQ(nullptr, mgr_.get_sched_info(10));
  EXPECT_EQ(0, updated_groups.count());

  ASSERT_EQ(OB_SUCCESS,
            updated_groups.push_back(ObMVGroupKey()));
  EXPECT_EQ(OB_SUCCESS, mgr_.remove_sched_info(10, updated_groups));
  EXPECT_EQ(0, updated_groups.count());

  ASSERT_EQ(OB_SUCCESS,
            updated_groups.push_back(ObMVGroupKey()));
  EXPECT_EQ(OB_SUCCESS, mgr_.remove_sched_info(OB_INVALID_ID, updated_groups));
  EXPECT_EQ(0, updated_groups.count());
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RemoveTopSplitReturnsAllUpdatedGroups)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  const uint64_t deps_30[] = {10, 20};
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());
  ASSERT_EQ(30UL, gid(10));
  ASSERT_EQ(30UL, gid(20));
  ASSERT_EQ(30UL, gid(30));

  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(30, updated_groups));
  ASSERT_EQ(2, updated_groups.count());
  bool has_group_10 = false;
  bool has_group_20 = false;
  for (int64_t i = 0; i < updated_groups.count(); ++i) {
    has_group_10 = has_group_10 || 10UL == updated_groups.at(i).group_id_;
    has_group_20 = has_group_20 || 20UL == updated_groups.at(i).group_id_;
  }
  EXPECT_TRUE(has_group_10);
  EXPECT_TRUE(has_group_20);
  EXPECT_EQ(nullptr, mgr_.get_sched_info(30));
  EXPECT_EQ(nullptr, mgr_.get_sched_group_info(30));
  EXPECT_EQ(10UL, gid(10));
  EXPECT_EQ(20UL, gid(20));
  ASSERT_NE(nullptr, mgr_.get_sched_group_info(10));
  ASSERT_NE(nullptr, mgr_.get_sched_group_info(20));
}

// remove path: removing an isolated node leaves other groups untouched.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RemoveIsolatedNodeNoop)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  const uint64_t deps_40[] = {30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_EQ(10UL, gid(10));
  ASSERT_EQ(40UL, gid(30));
  ASSERT_EQ(40UL, gid(40));

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(10, updated_groups));
  EXPECT_EQ(40UL, gid(30));
  EXPECT_EQ(40UL, gid(40));
  EXPECT_EQ(0, updated_groups.count());
}

// Removing a leaf that other nodes still depend on is forbidden: leaf = has
// no deps, but "leaf" here means graph leaf, not top. Top is defined as
// "nothing depends on me", which is the max mview_id of the group here.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, RemoveLeafDependedOnRejected)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_FALSE(is_top(10));

  EXPECT_EQ(OB_EAGAIN, mgr_.remove_sched_info(10, updated_groups));
}

// Scoped recompute rebuilds one complete existing group without changing other groups.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, ScopedRecomputeRespectsScope)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_40[] = {30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_EQ(20UL, gid(10));
  ASSERT_EQ(20UL, gid(20));
  ASSERT_EQ(40UL, gid(30));
  ASSERT_EQ(40UL, gid(40));
  ObMViewSchedGroupInfo *unrelated_group = mgr_.get_sched_group_info(40);
  ASSERT_NE(nullptr, unrelated_group);
  unrelated_group->modified_ts_ = 12345;

  EXPECT_EQ(OB_SUCCESS, recompute_scope(20));
  EXPECT_EQ(20UL, gid(10));
  EXPECT_EQ(20UL, gid(20));
  EXPECT_EQ(40UL, gid(30));
  EXPECT_EQ(40UL, gid(40));
  unrelated_group = mgr_.get_sched_group_info(40);
  ASSERT_NE(nullptr, unrelated_group);
  EXPECT_EQ(12345, unrelated_group->modified_ts_);
}

// ---------------------------------------------------------------------------
// 38. ObMViewSchedGroupInfo top vertex maintenance
//
// Verifies group_info_map_.top_mview_ids_ stay consistent across the
// full/scope/insert paths. Tops are the group members that no other in-group
// member depends on (i.e. no in-group child points to them via dep_mview_ids_).
// ---------------------------------------------------------------------------

// Singleton with no deps -> group_info_map_ has {itself} as top.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsIsolatedNode)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_TRUE(is_top(10));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(10UL, tops.at(0));
}

// Chain A(10) -> B(20) -> C(30). Only C is a top (chain leaf; nothing in-group
// depends on it).
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsChainOnlyLeafIsTop)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_FALSE(is_top(10));
  EXPECT_FALSE(is_top(20));
  EXPECT_TRUE(is_top(30));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(30UL, tops.at(0));
}

// Diamond A(10) -> B(20), A(10) -> C(30), B(20) -> D(40), C(30) -> D(40).
// D is the only top.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsDiamondSingleTop)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {10};
  const uint64_t deps_40[] = {20, 30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_FALSE(is_top(10));
  EXPECT_FALSE(is_top(20));
  EXPECT_FALSE(is_top(30));
  EXPECT_TRUE(is_top(40));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(40, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(40UL, tops.at(0));
}

// Two independent singletons + a leaf pointing at only one of them. In the
// {30, 40} group, C(30) has no in-group deps pointing at it and D(40) points
// at C(30) → only D(40) is a top of that group.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsDisjointGroupsEachHasOwnTop)
{
  const uint64_t deps_40[] = {30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  EXPECT_TRUE(is_top(10));
  EXPECT_FALSE(is_top(30));
  EXPECT_TRUE(is_top(40));
  ObSEArray<uint64_t, 4> tops_10;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops_10));
  ASSERT_EQ(1, tops_10.count());
  EXPECT_EQ(10UL, tops_10.at(0));
  ObSEArray<uint64_t, 4> tops_40;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(40, tops_40));
  ASSERT_EQ(1, tops_40.count());
  EXPECT_EQ(40UL, tops_40.at(0));
}

// Union-find output: each group carries top ids and the full member-id set.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, UnionFindMviewIdsPerGroupChain)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ObSEArray<uint64_t, 4> scoped_mview_ids;
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(20));
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(30));
  ObMViewSchedInfoUnionFind union_find(TENANT_ID, mgr_.info_map_);
  ASSERT_EQ(OB_SUCCESS, union_find.collect_tops_by_group(scoped_mview_ids));
  ASSERT_EQ(1, union_find.get_group_count());
  uint64_t group_id = OB_INVALID_ID;
  const ObIArray<uint64_t> *tops = NULL;
  const ObIArray<uint64_t> *mview_ids = NULL;
  ASSERT_EQ(OB_SUCCESS, union_find.get_group_at(0, group_id, tops, mview_ids));
  EXPECT_EQ(30UL, group_id);
  ASSERT_EQ(1, tops->count());
  EXPECT_EQ(30UL, tops->at(0));
  ASSERT_EQ(3, mview_ids->count());
  EXPECT_TRUE(has_exist_in_array(*mview_ids, 10UL));
  EXPECT_TRUE(has_exist_in_array(*mview_ids, 20UL));
  EXPECT_TRUE(has_exist_in_array(*mview_ids, 30UL));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, UnionFindMviewIdsPerGroupDisjoint)
{
  const uint64_t deps_40[] = {30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 1));
  ObSEArray<uint64_t, 4> scoped_mview_ids;
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(30));
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(40));
  ObMViewSchedInfoUnionFind union_find(TENANT_ID, mgr_.info_map_);
  ASSERT_EQ(OB_SUCCESS, union_find.collect_tops_by_group(scoped_mview_ids));
  ASSERT_EQ(2, union_find.get_group_count());
  int64_t idx_10 = -1;
  int64_t idx_40 = -1;
  for (int64_t i = 0; i < union_find.get_group_count(); ++i) {
    uint64_t group_id = OB_INVALID_ID;
    const ObIArray<uint64_t> *tops = NULL;
    const ObIArray<uint64_t> *mview_ids = NULL;
    ASSERT_EQ(OB_SUCCESS, union_find.get_group_at(i, group_id, tops, mview_ids));
    if (10UL == group_id) {
      idx_10 = i;
    } else if (40UL == group_id) {
      idx_40 = i;
    }
  }
  ASSERT_NE(-1, idx_10);
  ASSERT_NE(-1, idx_40);
  const ObIArray<uint64_t> *ids_10 = NULL;
  const ObIArray<uint64_t> *ids_40 = NULL;
  uint64_t group_id = OB_INVALID_ID;
  const ObIArray<uint64_t> *tops = NULL;
  ASSERT_EQ(OB_SUCCESS, union_find.get_group_at(idx_10, group_id, tops, ids_10));
  ASSERT_EQ(1, ids_10->count());
  EXPECT_EQ(10UL, ids_10->at(0));
  ASSERT_EQ(OB_SUCCESS, union_find.get_group_at(idx_40, group_id, tops, ids_40));
  ASSERT_EQ(2, ids_40->count());
  EXPECT_TRUE(has_exist_in_array(*ids_40, 30UL));
  EXPECT_TRUE(has_exist_in_array(*ids_40, 40UL));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, UnionFindMviewIdsPerGroupCycle)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_dep(10, 20));
  ASSERT_EQ(OB_SUCCESS, add_dep(20, 10));
  ObSEArray<uint64_t, 4> scoped_mview_ids;
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(20));
  ObMViewSchedInfoUnionFind union_find(TENANT_ID, mgr_.info_map_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, union_find.collect_tops_by_group(scoped_mview_ids));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CollectRelatedIdsReturnsCompleteUndirectedComponent)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(40, nullptr, 0));

  ObSEArray<uint64_t, 4> related_mview_ids;
  ASSERT_EQ(OB_SUCCESS,
            ObMViewSchedInfoUtils::collect_related_mview_ids(TENANT_ID,
                                                              mgr_.info_map_,
                                                              20,
                                                              related_mview_ids));
  ASSERT_EQ(3, related_mview_ids.count());
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 10UL));
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 20UL));
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 30UL));
  EXPECT_FALSE(has_exist_in_array(related_mview_ids, 40UL));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CollectRelatedIdsForMultipleTargets)
{
  const uint64_t deps_20[] = {10};
  const uint64_t deps_40[] = {30};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(40, deps_40, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(50, nullptr, 0));

  ObSEArray<uint64_t, 3> target_mview_ids;
  ObSEArray<uint64_t, 8> related_mview_ids;
  ASSERT_EQ(OB_SUCCESS, target_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, target_mview_ids.push_back(30));
  ASSERT_EQ(OB_SUCCESS, target_mview_ids.push_back(30));
  ASSERT_EQ(OB_SUCCESS,
            ObMViewSchedInfoUtils::collect_related_mview_ids(TENANT_ID,
                                                              mgr_.info_map_,
                                                              target_mview_ids,
                                                              related_mview_ids));
  ASSERT_EQ(4, related_mview_ids.count());
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 10UL));
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 20UL));
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 30UL));
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 40UL));
  EXPECT_FALSE(has_exist_in_array(related_mview_ids, 50UL));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CollectRelatedIdsForSameComponentTargetsDeduplicates)
{
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));

  ObSEArray<uint64_t, 2> target_mview_ids;
  ObSEArray<uint64_t, 8> related_mview_ids;
  ASSERT_EQ(OB_SUCCESS, target_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, target_mview_ids.push_back(20));
  ASSERT_EQ(OB_SUCCESS,
            ObMViewSchedInfoUtils::collect_related_mview_ids(TENANT_ID,
                                                              mgr_.info_map_,
                                                              target_mview_ids,
                                                              related_mview_ids));
  ASSERT_EQ(2, related_mview_ids.count());
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 10UL));
  EXPECT_TRUE(has_exist_in_array(related_mview_ids, 20UL));
  EXPECT_FALSE(has_exist_in_array(related_mview_ids, 30UL));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CollectRelatedIdsForEmptyTargetsReturnsEmpty)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));

  ObSEArray<uint64_t, 1> target_mview_ids;
  ObSEArray<uint64_t, 4> related_mview_ids;
  ASSERT_EQ(OB_SUCCESS, related_mview_ids.push_back(77));
  ASSERT_EQ(OB_SUCCESS,
            ObMViewSchedInfoUtils::collect_related_mview_ids(TENANT_ID,
                                                              mgr_.info_map_,
                                                              target_mview_ids,
                                                              related_mview_ids));
  EXPECT_EQ(0, related_mview_ids.count());
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, CollectRelatedIdsForUnknownTargetReturnsHashNotExist)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));

  ObSEArray<uint64_t, 2> target_mview_ids;
  ObSEArray<uint64_t, 4> related_mview_ids;
  ASSERT_EQ(OB_SUCCESS, target_mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, target_mview_ids.push_back(999));
  EXPECT_EQ(OB_HASH_NOT_EXIST,
            ObMViewSchedInfoUtils::collect_related_mview_ids(TENANT_ID,
                                                              mgr_.info_map_,
                                                              target_mview_ids,
                                                              related_mview_ids));
  EXPECT_EQ(0, related_mview_ids.count());
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, UnionFindGetGroupAtRejectsInvalidIndex)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ObSEArray<uint64_t, 1> scoped_mview_ids;
  ASSERT_EQ(OB_SUCCESS, scoped_mview_ids.push_back(10));
  ObMViewSchedInfoUnionFind union_find(TENANT_ID, mgr_.info_map_);
  ASSERT_EQ(OB_SUCCESS, union_find.collect_tops_by_group(scoped_mview_ids));
  uint64_t group_id = OB_INVALID_ID;
  const ObIArray<uint64_t> *top_mview_ids = NULL;
  const ObIArray<uint64_t> *mview_ids = NULL;

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            union_find.get_group_at(-1, group_id, top_mview_ids, mview_ids));
  EXPECT_EQ(OB_INVALID_ID, group_id);
  EXPECT_EQ(nullptr, top_mview_ids);
  EXPECT_EQ(nullptr, mview_ids);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            union_find.get_group_at(1, group_id, top_mview_ids, mview_ids));
}

// Insert path: pre-existing tops of merged groups are combined, D_i's are
// demoted, new_node becomes the sole surviving top.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsInsertMergeDemotesDeps)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_TRUE(is_top(10));
  ASSERT_TRUE(is_top(20));

  const uint64_t deps_30[] = {10, 20};
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 2));
  ASSERT_EQ(OB_SUCCESS, recompute_on_insert(30));
  EXPECT_FALSE(is_top(10));
  EXPECT_FALSE(is_top(20));
  EXPECT_TRUE(is_top(30));
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(30UL, tops.at(0));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsInsertSuspendedNodeKeepsRunnableDependency)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  mgr_.get_sched_info(20)->scheduler_suspended_ = true;
  ASSERT_EQ(OB_SUCCESS, recompute_on_insert(20));

  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(20, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(20UL, tops.at(0));
  EXPECT_FALSE(is_group_schedulable(20));
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());
  EXPECT_EQ(10UL, tops.at(0));
  EXPECT_TRUE(is_group_schedulable(10));
  EXPECT_TRUE(is_top(10));
}

// Insert path: X depends on one leaf of an existing chain. That leaf loses top
// status; the pre-existing top of the other chain stays a top.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsInsertPreservesUnrelatedTops)
{
  // Group {10, 20}: chain 10 -> 20; top = 20.
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  // Standalone node 25 (its own group, top = 25).
  ASSERT_EQ(OB_SUCCESS, add_node(25, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_TRUE(is_top(20));
  ASSERT_FALSE(is_top(10));
  ASSERT_TRUE(is_top(25));

  // Insert 30 depending on 20 only. Groups {10,20} and {25} both untouched by
  // the merge — 25 stays its own group; {10,20,30} share group 30 with top 30
  // (20 demoted).
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_on_insert(30));
  EXPECT_FALSE(is_top(10));
  EXPECT_FALSE(is_top(20));
  EXPECT_TRUE(is_top(25));
  EXPECT_TRUE(is_top(30));
  ObSEArray<uint64_t, 4> tops_25;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(25, tops_25));
  ASSERT_EQ(1, tops_25.count());
  EXPECT_EQ(25UL, tops_25.at(0));
  ObSEArray<uint64_t, 4> tops_30;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(30, tops_30));
  ASSERT_EQ(1, tops_30.count());
  EXPECT_EQ(30UL, tops_30.at(0));
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsRemoveTopShrinksGroup)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  const uint64_t deps_20[] = {10};
  const uint64_t deps_30[] = {20};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, add_node(30, deps_30, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ASSERT_TRUE(is_top(30));

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(30, updated_groups));
  EXPECT_FALSE(is_top(10));
  EXPECT_TRUE(is_top(20));
  ObSEArray<uint64_t, 4> tops_20;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(20, tops_20));
  ASSERT_EQ(1, tops_20.count());
  EXPECT_EQ(20UL, tops_20.at(0));
  ObSEArray<uint64_t, 4> tops_30;
  EXPECT_EQ(OB_HASH_NOT_EXIST, get_group_tops(30, tops_30));
  ASSERT_EQ(1, updated_groups.count());
  EXPECT_EQ(20UL, updated_groups.at(0).group_id_);
}

// Remove path: removing an isolated node removes its group_info_map_ entry.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TopsRemoveIsolatedDropsGroupEntry)
{
  ObSEArray<ObMVGroupKey, 4> updated_groups;
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  ObSEArray<uint64_t, 4> tops;
  ASSERT_EQ(OB_SUCCESS, get_group_tops(10, tops));
  ASSERT_EQ(1, tops.count());

  ASSERT_EQ(OB_SUCCESS, mgr_.remove_sched_info(10, updated_groups));
  ObSEArray<uint64_t, 4> tops_after;
  EXPECT_EQ(OB_HASH_NOT_EXIST, get_group_tops(10, tops_after));
  EXPECT_EQ(0, updated_groups.count());
}

// Public timer helpers snapshot group tokens / next_trigger_ts.
TEST_F(ObMViewSchedInfoMgrGroupIdsTest, TimerHelpersSnapshotGroupState)
{
  const uint64_t deps_20[] = {10};
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, deps_20, 1));
  ASSERT_EQ(OB_SUCCESS, recompute_full_with_intervals());

  ObSEArray<ObMVGroupKey, 4> group_keys;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_sched_group_keys(group_keys));
  ASSERT_EQ(1, group_keys.count());
  EXPECT_EQ(20UL, group_keys.at(0).group_id_);
  EXPECT_NE(OB_INVALID_TIMESTAMP, group_keys.at(0).modified_ts_);

  int64_t refresh_interval = 0;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_group_refresh_interval(20, refresh_interval));
  EXPECT_EQ(DEFAULT_SPECIFIED_INTERVAL, refresh_interval);

  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_sched_group_timer_info(group_keys.at(0), next_trigger_ts));
  EXPECT_EQ(DEFAULT_SPECIFIED_INTERVAL, next_trigger_ts);

  const ObMVGroupKey stale_group_key(20, group_keys.at(0).modified_ts_ - 1);
  ASSERT_EQ(OB_SUCCESS,
            mgr_.get_sched_group_timer_info(stale_group_key, next_trigger_ts));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, next_trigger_ts);

  bool is_valid = false;
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_sched_group_valid(20, group_keys.at(0).modified_ts_, is_valid));
  EXPECT_TRUE(is_valid);
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_sched_group_valid(20, group_keys.at(0).modified_ts_ + 1, is_valid));
  EXPECT_FALSE(is_valid);
  ASSERT_EQ(OB_SUCCESS,
            mgr_.check_sched_group_valid(999, group_keys.at(0).modified_ts_, is_valid));
  EXPECT_FALSE(is_valid);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, UpdateLastStartTimeSkipsMissingEntry)
{
  static const int64_t CURRENT_TIME = 123456;
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ObSEArray<uint64_t, 3> mview_ids;
  ASSERT_EQ(OB_SUCCESS, mview_ids.push_back(10));
  ASSERT_EQ(OB_SUCCESS, mview_ids.push_back(999));
  ASSERT_EQ(OB_SUCCESS, mview_ids.push_back(20));

  mgr_.update_last_start_time_for_mviews(mview_ids, CURRENT_TIME);

  ASSERT_NE(nullptr, mgr_.get_sched_info(10));
  ASSERT_NE(nullptr, mgr_.get_sched_info(20));
  EXPECT_EQ(CURRENT_TIME, mgr_.get_sched_info(10)->last_start_time_);
  EXPECT_EQ(CURRENT_TIME, mgr_.get_sched_info(20)->last_start_time_);
}

TEST_F(ObMViewSchedInfoMgrGroupIdsTest, ForeachMemberContinuesAfterMemberErrors)
{
  ASSERT_EQ(OB_SUCCESS, add_node(10, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(20, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, add_node(30, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, recompute_full());
  mgr_.remove_group_info(20);
  AlwaysFailingMemberFn fn;

  EXPECT_EQ(OB_SUCCESS, mgr_.foreach_sched_group_member(fn));
  EXPECT_EQ(2, fn.call_count_);
}

// ---------------------------------------------------------------------------
// 39. ObMViewSchedInfoMgr::collect_group_schedule_mviews antichain dedup
//
// Verifies that the group-level DAG walk returns a set with no ancestor-
// descendant pair. Two problem cases are covered:
//   A) Cross-top shared descendant: T1 (not needed) + T2 (needed) both point
//      to A (needed). A must not appear alongside T2, since refreshing T2
//      already covers A via ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order.
//   B) Single-top diamond: T (not needed) -> X, Y; X -> Y; X and Y both
//      needed. Y must not appear alongside X for the same reason.
// ---------------------------------------------------------------------------

class ObMViewSchedGroupCollectTest : public ::testing::Test
{
public:
  static constexpr int64_t CURRENT_TIME = 1000;
  static constexpr int64_t DAG_INTERVAL = 10;
  static constexpr int64_t NEEDED_INTERVAL = 1;
  static constexpr int64_t NOT_NEEDED_INTERVAL = 100000;
  static constexpr int64_t LAST_START_TIME = 500;
  static constexpr uint64_t GROUP_ID = 999;

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.init(TENANT_ID, 64));
  }

  void TearDown() override
  {
    ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.destroy());
  }

  ObMViewSchedInfo *make_node(uint64_t mv, bool needed)
  {
    ObMViewSchedInfo *node = OB_NEWx(ObMViewSchedInfo,
                                    &sched_info_mgr_.alloc_,
                                    sched_info_mgr_.alloc_);
    EXPECT_TRUE(nullptr != node);
    if (nullptr != node) {
      node->mview_id_ = mv;
      node->group_id_ = GROUP_ID;
      node->last_start_time_ = LAST_START_TIME;
      node->refresh_interval_ = needed ? NEEDED_INTERVAL : NOT_NEEDED_INTERVAL;
      EXPECT_EQ(OB_SUCCESS, sched_info_mgr_.info_map_.set_refactored(mv, node));
    }
    return node;
  }

  ObMViewSchedGroupInfo *make_group(const ObIArray<ObMViewSchedInfo *> &tops)
  {
    ObMViewSchedGroupInfo *g = OB_NEWx(ObMViewSchedGroupInfo,
                                      &sched_info_mgr_.alloc_,
                                      sched_info_mgr_.alloc_);
    EXPECT_TRUE(nullptr != g);
    if (nullptr != g) {
      ObSEArray<uint64_t, 4> top_mview_ids;
      for (int64_t i = 0; i < tops.count(); ++i) {
        EXPECT_TRUE(nullptr != tops.at(i));
        if (nullptr != tops.at(i)) {
          EXPECT_EQ(OB_SUCCESS, top_mview_ids.push_back(tops.at(i)->mview_id_));
        }
      }
      g->refresh_interval_ = DAG_INTERVAL;
      g->modified_ts_ = CURRENT_TIME;
      EXPECT_EQ(OB_SUCCESS, g->top_mview_ids_.assign(top_mview_ids));
      EXPECT_EQ(OB_SUCCESS, sched_info_mgr_.group_info_map_.set_refactored(GROUP_ID, g));
    }
    return g;
  }

  int collect_group(ObIArray<uint64_t> &out)
  {
    return sched_info_mgr_.collect_group_schedule_mviews(GROUP_ID, CURRENT_TIME, out);
  }

  static bool contains_id(const ObIArray<uint64_t> &arr, uint64_t mv)
  {
    return has_exist_in_array(arr, mv);
  }

  ObMViewSchedInfoMgr sched_info_mgr_;
};

// Baseline: a single top that itself needs refresh returns just itself.
TEST_F(ObMViewSchedGroupCollectTest, SingleTopNeededReturnsSelf)
{
  ObMViewSchedInfo *t = make_node(100, true);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t));
  make_group(tops);
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(100UL, out.at(0));
}

// Baseline: a single top that does not need refresh, with no deps, returns empty.
TEST_F(ObMViewSchedGroupCollectTest, SingleTopNotNeededReturnsEmpty)
{
  ObMViewSchedInfo *t = make_node(100, false);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t));
  make_group(tops);
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  EXPECT_EQ(0, out.count());
}

// Two independent tops that both need refresh: both stay in the result.
TEST_F(ObMViewSchedGroupCollectTest, TwoIndependentTopsBothNeeded)
{
  ObMViewSchedInfo *t1 = make_node(100, true);
  ObMViewSchedInfo *t2 = make_node(200, true);
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t1));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t2));
  make_group(tops);
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  ASSERT_EQ(2, out.count());
  EXPECT_TRUE(contains_id(out, 100));
  EXPECT_TRUE(contains_id(out, 200));
}

// Case A: cross-top shared descendant.
// T1(not needed) -> A(needed); T2(needed) -> A(needed). Result is [A, T2].
TEST_F(ObMViewSchedGroupCollectTest, CrossTopSharedDescendantDedup)
{
  ObMViewSchedInfo *a = make_node(50, true);
  ObMViewSchedInfo *t1 = make_node(100, false);
  ObMViewSchedInfo *t2 = make_node(200, true);
  ASSERT_EQ(OB_SUCCESS, t1->dep_mview_ids_.push_back(50));
  ASSERT_EQ(OB_SUCCESS, t2->dep_mview_ids_.push_back(50));
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t1));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t2));
  make_group(tops);
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  ASSERT_EQ(2, out.count());
  EXPECT_TRUE(contains_id(out, 50));
  EXPECT_TRUE(contains_id(out, 200));
}

// Case B: single-top diamond.
// T(not needed) -> X(needed), T -> Y(needed), X -> Y. Result is [X, Y].
TEST_F(ObMViewSchedGroupCollectTest, SingleTopDiamondDedup)
{
  ObMViewSchedInfo *y = make_node(50, true);
  ObMViewSchedInfo *x = make_node(100, true);
  ObMViewSchedInfo *t = make_node(200, false);
  ASSERT_EQ(OB_SUCCESS, x->dep_mview_ids_.push_back(50));
  ASSERT_EQ(OB_SUCCESS, t->dep_mview_ids_.push_back(100));
  ASSERT_EQ(OB_SUCCESS, t->dep_mview_ids_.push_back(50));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t));
  make_group(tops);
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  ASSERT_EQ(2, out.count());
  EXPECT_TRUE(contains_id(out, 100));
  EXPECT_TRUE(contains_id(out, 50));
}

// Nothing in the group needs refresh: result is empty.
TEST_F(ObMViewSchedGroupCollectTest, NoNodeNeededReturnsEmpty)
{
  ObMViewSchedInfo *a = make_node(50, false);
  ObMViewSchedInfo *t = make_node(100, false);
  ASSERT_EQ(OB_SUCCESS, t->dep_mview_ids_.push_back(50));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t));
  make_group(tops);
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  EXPECT_EQ(0, out.count());
}

TEST_F(ObMViewSchedGroupCollectTest, PendingRefreshStartDoesNotBlockOtherTop)
{
  ObMViewSchedInfo *pending = make_node(100, true);
  ObMViewSchedInfo *needed = make_node(200, true);
  pending->set_refresh_start(CURRENT_TIME + 100);
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(pending));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(needed));
  make_group(tops);
  ObSEArray<uint64_t, 2> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(200UL, out.at(0));
}

TEST_F(ObMViewSchedGroupCollectTest, PassedRefreshStartDoesNotBlockSelf)
{
  ObMViewSchedInfo *node = make_node(100, true);
  node->set_refresh_start(LAST_START_TIME - 1);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(node));
  make_group(tops);
  ObSEArray<uint64_t, 2> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(100UL, out.at(0));
}

// Chain T(not needed) -> M(not needed) -> L(needed). Result must be [L].
TEST_F(ObMViewSchedGroupCollectTest, DeepChainOnlyLeafNeeded)
{
  ObMViewSchedInfo *l = make_node(50, true);
  ObMViewSchedInfo *m = make_node(100, false);
  ObMViewSchedInfo *t = make_node(200, false);
  ASSERT_EQ(OB_SUCCESS, m->dep_mview_ids_.push_back(50));
  ASSERT_EQ(OB_SUCCESS, t->dep_mview_ids_.push_back(100));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t));
  make_group(tops);
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS, collect_group(out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(50UL, out.at(0));
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerUsesMinMemberNextTrigger)
{
  ObMViewSchedInfo *dep = make_node(50, true);
  ObMViewSchedInfo *t1 = make_node(100, true);
  ObMViewSchedInfo *t2 = make_node(200, true);
  dep->last_start_time_ = 700;
  dep->refresh_interval_ = 100;
  t1->last_start_time_ = 500;
  t1->refresh_interval_ = 300;
  t2->last_start_time_ = 900;
  t2->refresh_interval_ = 600;
  ASSERT_EQ(OB_SUCCESS, t1->dep_mview_ids_.push_back(50));
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t1));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(t2));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 100;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(800, next_trigger_ts);
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerUsesPendingRefreshStart)
{
  ObMViewSchedInfo *dep = make_node(50, true);
  ObMViewSchedInfo *top = make_node(100, true);
  dep->last_start_time_ = 700;
  dep->refresh_interval_ = 300;
  dep->set_refresh_start(950);
  top->last_start_time_ = 900;
  top->refresh_interval_ = 300;
  ASSERT_EQ(OB_SUCCESS, top->dep_mview_ids_.push_back(50));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 300;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(950, next_trigger_ts);
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerIgnoresPassedRefreshStart)
{
  ObMViewSchedInfo *top = make_node(100, true);
  top->last_start_time_ = 900;
  top->refresh_interval_ = 300;
  top->set_refresh_start(800);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 300;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(1200, next_trigger_ts);
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerAdvancesWhenLastStartEqualsRefreshStart)
{
  ObMViewSchedInfo *top = make_node(100, true);
  top->last_start_time_ = 900;
  top->refresh_interval_ = 300;
  top->set_refresh_start(900);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 300;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(1200, next_trigger_ts);
  ObSEArray<uint64_t, 1> out;
  ASSERT_EQ(OB_SUCCESS,
            sched_info_mgr_.collect_group_schedule_mviews(GROUP_ID, next_trigger_ts, out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(100UL, out.at(0));
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerIgnoresNonPeriodicMember)
{
  ObMViewSchedInfo *dep = make_node(50, true);
  ObMViewSchedInfo *top = make_node(100, true);
  dep->last_start_time_ = 700;
  dep->refresh_interval_ = 300;
  top->last_start_time_ = 900;
  top->refresh_interval_ = ObMViewSchedInfo::NO_PERIODIC_SCHEDULE_INTERVAL;
  ASSERT_EQ(OB_SUCCESS, top->dep_mview_ids_.push_back(50));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 300;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(1000, next_trigger_ts);
  ObSEArray<uint64_t, 1> out;
  ASSERT_EQ(OB_SUCCESS,
            sched_info_mgr_.collect_group_schedule_mviews(GROUP_ID, next_trigger_ts, out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(50UL, out.at(0));
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerRejectsGroupWithoutPeriodicMember)
{
  ObMViewSchedInfo *top = make_node(100, true);
  top->refresh_interval_ = ObMViewSchedInfo::NO_PERIODIC_SCHEDULE_INTERVAL;
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 300;
  int64_t next_trigger_ts = 123;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, next_trigger_ts);
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerSelectsPendingDependencyAtStart)
{
  ObMViewSchedInfo *pending = make_node(50, true);
  ObMViewSchedInfo *slow = make_node(100, true);
  ObMViewSchedInfo *top = make_node(200, true);
  pending->last_start_time_ = 10;
  pending->refresh_interval_ = 16;
  pending->set_refresh_start(25);
  slow->last_start_time_ = 0;
  slow->refresh_interval_ = 64;
  top->last_start_time_ = 0;
  top->refresh_interval_ = 64;
  ASSERT_EQ(OB_SUCCESS, top->dep_mview_ids_.push_back(100));
  ASSERT_EQ(OB_SUCCESS, top->dep_mview_ids_.push_back(50));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 16;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(25, next_trigger_ts);
  ObSEArray<uint64_t, 1> out;
  ASSERT_EQ(OB_SUCCESS,
            sched_info_mgr_.collect_group_schedule_mviews(GROUP_ID, next_trigger_ts, out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(50UL, out.at(0));
}

// The fast member is due at 600, while the slow member is not due until 700.
TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerAvoidsPrematurePoll)
{
  ObMViewSchedInfo *slow = make_node(100, true);
  ObMViewSchedInfo *fast = make_node(200, true);
  slow->last_start_time_ = 100;
  slow->refresh_interval_ = 600;
  fast->last_start_time_ = 500;
  fast->refresh_interval_ = 100;
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(slow));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(fast));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 100;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(600, next_trigger_ts);
  ObSEArray<uint64_t, 2> out;
  ASSERT_EQ(OB_SUCCESS,
            sched_info_mgr_.collect_group_schedule_mviews(GROUP_ID, next_trigger_ts, out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(200UL, out.at(0));
}

TEST_F(ObMViewSchedGroupCollectTest, CalcNextTriggerTrustsGroupMembership)
{
  ObMViewSchedInfo *suspended = make_node(100, true);
  ObMViewSchedInfo *running = make_node(200, true);
  suspended->scheduler_suspended_ = true;
  suspended->last_start_time_ = 900;
  suspended->refresh_interval_ = 1000;
  suspended->set_refresh_start(950);
  EXPECT_EQ(950, suspended->get_refresh_start());
  running->last_start_time_ = 500;
  running->refresh_interval_ = 1000;
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(suspended));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(running));
  ObMViewSchedGroupInfo *g = make_group(tops);
  g->refresh_interval_ = 1000;
  int64_t next_trigger_ts = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.get_sched_group_next_trigger_ts(*g, next_trigger_ts));
  EXPECT_EQ(950, next_trigger_ts);
}

// ---------------------------------------------------------------------------
// 40. ObMViewSchedInfo::check_schedule_needed
// ---------------------------------------------------------------------------

class ObMViewSchedInfoCheckScheduleNeededTest : public ::testing::Test
{
public:
  static constexpr int64_t PERIODIC_INTERVAL = ObMViewTieredIntervalCalc::BASE_UNIT_US;

  void SetUp() override
  {
    node_.reset();
    node_.mview_id_ = 100;
    node_.last_start_time_ = 500;
    node_.refresh_interval_ = PERIODIC_INTERVAL;
  }

  bool check_needed(const int64_t current_time,
                    const int64_t dag_refresh_interval)
  {
    bool needed = false;
    const int ret = node_.check_schedule_needed(current_time, dag_refresh_interval, needed);
    last_ret_ = ret;
    return needed;
  }

  ObMViewSchedInfo node_{alloc_};
  int last_ret_ = OB_SUCCESS;
  ObArenaAllocator alloc_;
};

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, OnDemandDagAlwaysNeeded)
{
  node_.refresh_interval_ = -1;
  EXPECT_TRUE(check_needed(1000, -1));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, SchedulerStateIsCheckedBeforeNodeScheduling)
{
  node_.scheduler_suspended_ = true;
  EXPECT_TRUE(node_.need_periodic_schedule());
  EXPECT_TRUE(check_needed(1000500, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
  EXPECT_TRUE(check_needed(1000000, -1));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, NonPeriodicNodeIsNotAutomaticRoot)
{
  node_.refresh_interval_ = -1;
  EXPECT_FALSE(check_needed(1000, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, PeriodicNeededWhenDueTimeReached)
{
  // due_time = 500 + 1s = 1_000_500
  EXPECT_TRUE(check_needed(1000500, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
  EXPECT_TRUE(check_needed(1000501, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, PeriodicNotNeededBeforeDueTime)
{
  // due_time = 500 + 1s = 1_000_500
  EXPECT_FALSE(check_needed(1000499, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, DagIntervalDoesNotPullDueTimeEarlier)
{
  // due_time = 1_000_500 is not reached at 1_000_000, whatever the DAG interval is.
  EXPECT_FALSE(check_needed(1000000, 2 * PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
  EXPECT_FALSE(check_needed(1000000, 1000 * PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, InvalidCurrentTimeReturnsError)
{
  bool needed = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            node_.check_schedule_needed(-1, PERIODIC_INTERVAL, needed));
  EXPECT_FALSE(needed);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, CurrentTimeNotAfterLastStartReturnsError)
{
  bool needed = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            node_.check_schedule_needed(500, PERIODIC_INTERVAL, needed));
  EXPECT_FALSE(needed);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, PendingRefreshStartNotNeededBeforeStart)
{
  node_.set_refresh_start(2000000);
  EXPECT_FALSE(check_needed(1000000, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, PendingRefreshStartNotNeededJustBeforeStart)
{
  node_.set_refresh_start(1500000);
  EXPECT_FALSE(check_needed(1499999, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

TEST_F(ObMViewSchedInfoCheckScheduleNeededTest, PendingRefreshStartNeededAtStart)
{
  node_.set_refresh_start(1500000);
  EXPECT_TRUE(check_needed(1500000, PERIODIC_INTERVAL));
  EXPECT_EQ(OB_SUCCESS, last_ret_);
}

// ---------------------------------------------------------------------------
// 41. ObMViewSchedInfoMgr::collect_schedule_mviews (node-level, uses check_schedule_needed)
// ---------------------------------------------------------------------------

TEST(ObMViewSchedInfoCollectScheduleMviewsTest, TopNotNeededRecursesToNeededDep)
{
  ObMViewSchedInfoMgr sched_info_mgr;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.init(TENANT_ID, 64));
  ObMViewSchedInfo *dep = OB_NEWx(ObMViewSchedInfo,
                                  &sched_info_mgr.alloc_,
                                  sched_info_mgr.alloc_);
  ObMViewSchedInfo *top = OB_NEWx(ObMViewSchedInfo,
                                  &sched_info_mgr.alloc_,
                                  sched_info_mgr.alloc_);
  ASSERT_NE(nullptr, dep);
  ASSERT_NE(nullptr, top);
  const int64_t interval = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  dep->mview_id_ = 10;
  dep->last_start_time_ = 0;
  dep->refresh_interval_ = interval;
  top->mview_id_ = 20;
  top->last_start_time_ = 500;
  top->refresh_interval_ = interval;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.info_map_.set_refactored(dep->mview_id_, dep));
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.info_map_.set_refactored(top->mview_id_, top));
  ASSERT_EQ(OB_SUCCESS, top->dep_mview_ids_.push_back(dep->mview_id_));

  const int64_t current_time = 1000000;
  // due_time for top = 500 + interval = 1_000_500 -> not reached; dep is due at 1_000_000.
  const int64_t dag_interval = 500;
  ObSEArray<uint64_t, 4> out;
  ASSERT_EQ(OB_SUCCESS,
            sched_info_mgr.collect_schedule_mviews(top->mview_id_, current_time, dag_interval, out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(10UL, out.at(0));
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.destroy());
}

TEST(ObMViewSchedInfoCollectScheduleMviewsTest, SchedulerStateDoesNotChangeNodeCollection)
{
  ObMViewSchedInfoMgr sched_info_mgr;
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.init(TENANT_ID, 64));
  ObMViewSchedInfo *dep = OB_NEWx(ObMViewSchedInfo,
                                  &sched_info_mgr.alloc_,
                                  sched_info_mgr.alloc_);
  ObMViewSchedInfo *top = OB_NEWx(ObMViewSchedInfo,
                                  &sched_info_mgr.alloc_,
                                  sched_info_mgr.alloc_);
  ASSERT_NE(nullptr, dep);
  ASSERT_NE(nullptr, top);
  const int64_t interval = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  dep->mview_id_ = 10;
  dep->last_start_time_ = 0;
  dep->refresh_interval_ = interval;
  top->mview_id_ = 20;
  top->last_start_time_ = 0;
  top->refresh_interval_ = interval;
  top->scheduler_suspended_ = true;
  ASSERT_EQ(OB_SUCCESS, top->dep_mview_ids_.push_back(dep->mview_id_));
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.info_map_.set_refactored(dep->mview_id_, dep));
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.info_map_.set_refactored(top->mview_id_, top));

  ObSEArray<uint64_t, 2> out;
  ASSERT_EQ(OB_SUCCESS,
            sched_info_mgr.collect_schedule_mviews(top->mview_id_, 1000000, interval, out));
  ASSERT_EQ(1, out.count());
  EXPECT_EQ(20UL, out.at(0));
  ASSERT_EQ(OB_SUCCESS, sched_info_mgr.destroy());
}

// ---------------------------------------------------------------------------
// 42. ObMViewTieredIntervalCalc tier quantization and DAG validation
// ---------------------------------------------------------------------------

class ObMViewIntervalCalcTest : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.init(TENANT_ID, 64));
  }

  void TearDown() override
  {
    ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.destroy());
    alloc_.reset();
  }

  int push_top_mview_ids(const ObIArray<ObMViewSchedInfo *> &tops,
                         ObIArray<uint64_t> &top_mview_ids) const
  {
    int ret = OB_SUCCESS;
    top_mview_ids.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < tops.count(); ++i) {
      if (OB_ISNULL(tops.at(i))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(top_mview_ids.push_back(tops.at(i)->mview_id_))) {
      }
    }
    return ret;
  }

  ObMViewSchedInfo *make_node(const uint64_t mv, const int64_t specified_interval)
  {
    ObMViewSchedInfo *node = OB_NEWx(ObMViewSchedInfo,
                                    &sched_info_mgr_.alloc_,
                                    sched_info_mgr_.alloc_);
    EXPECT_TRUE(nullptr != node);
    if (nullptr != node) {
      node->mview_id_ = mv;
      node->specified_refresh_interval_ = specified_interval;
      EXPECT_EQ(OB_SUCCESS, sched_info_mgr_.info_map_.set_refactored(mv, node));
    }
    return node;
  }

  int apply_calc_res(const ObMViewIntervalCalcRes &calc_res,
                     const ObIArray<ObMViewSchedInfo *> &tops,
                     int64_t &group_interval)
  {
    int ret = OB_SUCCESS;
    ObSEArray<uint64_t, 8> top_mview_ids;
    ObSEArray<uint64_t, 8> all_mview_ids;
    ObSEArray<ObMViewSchedInfo *, 8> sched_infos;
    ObSEArray<int64_t, 8> refresh_intervals;
    ObMViewSchedInfo *sched_info = NULL;
    int64_t refresh_interval = ObMViewSchedInfo::NO_PERIODIC_SCHEDULE_INTERVAL;
    if (OB_UNLIKELY(tops.empty() || !calc_res.is_applicable_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(push_top_mview_ids(tops, top_mview_ids))) {
    } else if (OB_FAIL(ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order(sched_info_mgr_.info_map_,
                                                                                 top_mview_ids,
                                                                                 all_mview_ids))) {
    } else if (OB_FAIL(sched_infos.prepare_allocate(all_mview_ids.count()))) {
    } else if (OB_FAIL(refresh_intervals.prepare_allocate(all_mview_ids.count()))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_mview_ids.count(); ++i) {
      if (OB_ISNULL(sched_info = sched_info_mgr_.get_sched_info(all_mview_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(calc_res.mview_refresh_interval_map_.get_refactored(all_mview_ids.at(i),
                                                                             refresh_interval))) {
      } else {
        sched_infos.at(i) = sched_info;
        refresh_intervals.at(i) = refresh_interval;
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < sched_infos.count(); ++i) {
        sched_infos.at(i)->refresh_interval_ = refresh_intervals.at(i);
      }
      group_interval = calc_res.group_refresh_interval_;
    }
    return ret;
  }

  int run_tiered_calc(const ObIArray<ObMViewSchedInfo *> &tops,
                      int64_t &group_interval)
  {
    ObSEArray<uint64_t, 8> top_mview_ids;
    sql::ObSqlArray<uint64_t> top_arr(alloc_);
    ObSEArray<uint64_t, 8> all_mview_ids;
    int ret = OB_SUCCESS;
    ObMViewIntervalCalcRes calc_res;
    if (OB_FAIL(push_top_mview_ids(tops, top_mview_ids))) {
    } else if (OB_FAIL(top_arr.assign(top_mview_ids))) {
    } else if (OB_FAIL(ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order(sched_info_mgr_.info_map_,
                                                                                 top_mview_ids,
                                                                                 all_mview_ids))) {
    } else if (OB_FAIL(calc_res.prepare(TENANT_ID, all_mview_ids.count()))) {
    } else {
      ObMViewTieredIntervalCalc calc(TENANT_ID, sched_info_mgr_.info_map_, top_arr, calc_res);
      if (OB_FAIL(calc.calc())) {
      } else if (calc_res.is_applicable_ && OB_FAIL(apply_calc_res(calc_res, tops, group_interval))) {
      }
    }
    return ret;
  }

  int run_lcm_calc(const ObIArray<ObMViewSchedInfo *> &tops,
                   int64_t &group_interval,
                   bool &is_applicable)
  {
    ObSEArray<uint64_t, 8> top_mview_ids;
    sql::ObSqlArray<uint64_t> top_arr(alloc_);
    ObSEArray<uint64_t, 8> all_mview_ids;
    int ret = OB_SUCCESS;
    ObMViewIntervalCalcRes calc_res;
    is_applicable = false;
    if (OB_FAIL(push_top_mview_ids(tops, top_mview_ids))) {
    } else if (OB_FAIL(top_arr.assign(top_mview_ids))) {
    } else if (OB_FAIL(ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order(sched_info_mgr_.info_map_,
                                                                                 top_mview_ids,
                                                                                 all_mview_ids))) {
    } else if (OB_FAIL(calc_res.prepare(TENANT_ID, all_mview_ids.count()))) {
    } else {
      ObMViewLcmIntervalCalc calc(TENANT_ID, sched_info_mgr_.info_map_, top_arr, calc_res);
      if (OB_FAIL(calc.calc())) {
      } else {
        is_applicable = calc_res.is_applicable_;
        if (is_applicable && OB_FAIL(apply_calc_res(calc_res, tops, group_interval))) {
        }
      }
    }
    return ret;
  }

  int run_default_calc(const ObIArray<ObMViewSchedInfo *> &tops,
                       int64_t &group_interval)
  {
    int ret = OB_SUCCESS;
    ObSEArray<uint64_t, 8> top_mview_ids;
    sql::ObSqlArray<uint64_t> top_arr(alloc_);
    ObSEArray<uint64_t, 8> all_mview_ids;
    ObMViewIntervalCalcRes calc_res;
    if (OB_FAIL(push_top_mview_ids(tops, top_mview_ids))) {
    } else if (OB_FAIL(top_arr.assign(top_mview_ids))) {
    } else if (OB_FAIL(ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order(sched_info_mgr_.info_map_,
                                                                                 top_mview_ids,
                                                                                 all_mview_ids))) {
    } else if (OB_FAIL(calc_res.prepare(TENANT_ID, all_mview_ids.count()))) {
    } else if (OB_FAIL(ObMViewSchedInfoUtils::calc_refresh_interval(TENANT_ID,
                                                                    sched_info_mgr_.info_map_,
                                                                    top_arr,
                                                                    calc_res))) {
    } else if (OB_FAIL(apply_calc_res(calc_res, tops, group_interval))) {
    }
    return ret;
  }

  ObArenaAllocator alloc_;
  ObMViewSchedInfoMgr sched_info_mgr_;
};

TEST_F(ObMViewIntervalCalcTest, BuildAndPublishUsesDefaultCalcResult)
{
  const int64_t expected_interval = 2 * ObMViewTieredIntervalCalc::BASE_UNIT_US;
  const int64_t published_interval = 3 * ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *top = make_node(100, 3 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  top->refresh_interval_ = 101;
  ObSEArray<uint64_t, 8> all_mview_ids;
  ObSEArray<uint64_t, 1> top_mview_ids;
  sql::ObSqlArray<uint64_t> top_arr(alloc_);
  ObMViewIntervalCalcRes calc_res;
  ObMVGroupKey group_key;
  EXPECT_FALSE(calc_res.is_applicable_);
  ASSERT_EQ(OB_SUCCESS, top_mview_ids.push_back(top->mview_id_));
  ASSERT_EQ(OB_SUCCESS, top_arr.assign(top_mview_ids));
  ASSERT_EQ(OB_SUCCESS,
            ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order(sched_info_mgr_.info_map_,
                                                                      top_mview_ids,
                                                                      all_mview_ids));
  ASSERT_EQ(OB_SUCCESS, calc_res.prepare(TENANT_ID, all_mview_ids.count()));

  ObMViewTieredIntervalCalc calc(TENANT_ID, sched_info_mgr_.info_map_, top_arr, calc_res);
  ASSERT_EQ(OB_SUCCESS, calc.calc());
  EXPECT_TRUE(calc_res.is_applicable_);
  EXPECT_EQ(1, calc_res.mview_refresh_interval_map_.size());
  int64_t mview_refresh_interval = ObMViewSchedInfo::NO_PERIODIC_SCHEDULE_INTERVAL;
  ASSERT_EQ(OB_SUCCESS, calc_res.mview_refresh_interval_map_.get_refactored(100, mview_refresh_interval));
  EXPECT_EQ(expected_interval, mview_refresh_interval);
  EXPECT_EQ(expected_interval, calc_res.group_refresh_interval_);
  EXPECT_EQ(101, top->refresh_interval_);

  ASSERT_EQ(OB_SUCCESS, sched_info_mgr_.build_and_publish_sched_group(top->mview_id_,
                                                                      top_mview_ids,
                                                                      all_mview_ids,
                                                                      group_key));
  const ObMViewSchedGroupInfo *group_info = sched_info_mgr_.get_sched_group_info(top->mview_id_);
  ASSERT_NE(nullptr, group_info);
  EXPECT_EQ(top->mview_id_, group_key.group_id_);
  EXPECT_EQ(group_info->modified_ts_, group_key.modified_ts_);
  EXPECT_EQ(published_interval, top->refresh_interval_);
  EXPECT_EQ(published_interval, group_info->refresh_interval_);
  calc_res.reuse();
  EXPECT_FALSE(calc_res.is_applicable_);
  EXPECT_TRUE(calc_res.mview_refresh_interval_map_.empty());
}

TEST_F(ObMViewIntervalCalcTest, CalcRefreshIntervalRejectsUnpreparedResult)
{
  ObMViewSchedInfo *top = make_node(100, ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ASSERT_NE(nullptr, top);
  ObSEArray<uint64_t, 1> top_mview_ids;
  ASSERT_EQ(OB_SUCCESS, top_mview_ids.push_back(top->mview_id_));
  sql::ObSqlArray<uint64_t> top_arr(alloc_);
  ASSERT_EQ(OB_SUCCESS, top_arr.assign(top_mview_ids));
  ObMViewIntervalCalcRes calc_res;

  EXPECT_EQ(OB_NOT_INIT,
            ObMViewSchedInfoUtils::calc_refresh_interval(TENANT_ID,
                                                          sched_info_mgr_.info_map_,
                                                          top_arr,
                                                          calc_res));
  EXPECT_FALSE(calc_res.is_applicable_);
  EXPECT_FALSE(calc_res.mview_refresh_interval_map_.created());
}

TEST_F(ObMViewIntervalCalcTest, TieredCalcQuantizesToPowerOfTau)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *on_demand = make_node(100, 0);
  ObMViewSchedInfo *above_base = make_node(200, base_unit + 1);
  ObMViewSchedInfo *below_double = make_node(300, 2 * base_unit - 1);
  ObMViewSchedInfo *double_base = make_node(400, 2 * base_unit);
  ObSEArray<ObMViewSchedInfo *, 4> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(on_demand));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(above_base));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(below_double));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(double_base));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_tiered_calc(tops, group_interval));
  EXPECT_EQ(-1, on_demand->refresh_interval_);
  EXPECT_EQ(base_unit, above_base->refresh_interval_);
  EXPECT_EQ(base_unit, below_double->refresh_interval_);
  EXPECT_EQ(2 * base_unit, double_base->refresh_interval_);
}

TEST_F(ObMViewIntervalCalcTest, SingleTopSetsNodeAndGroupInterval)
{
  ObMViewSchedInfo *top = make_node(100, 3 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_tiered_calc(tops, group_interval));
  EXPECT_EQ(2 * ObMViewTieredIntervalCalc::BASE_UNIT_US, top->refresh_interval_);
  EXPECT_EQ(2 * ObMViewTieredIntervalCalc::BASE_UNIT_US, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, GroupIntervalIsGcdAcrossTops)
{
  ObMViewSchedInfo *slow = make_node(100, 8 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ObMViewSchedInfo *fast = make_node(200, 3 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(slow));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(fast));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_tiered_calc(tops, group_interval));
  EXPECT_EQ(8 * ObMViewTieredIntervalCalc::BASE_UNIT_US, slow->refresh_interval_);
  EXPECT_EQ(2 * ObMViewTieredIntervalCalc::BASE_UNIT_US, fast->refresh_interval_);
  EXPECT_EQ(2 * ObMViewTieredIntervalCalc::BASE_UNIT_US, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, OnDemandNodeDoesNotAffectGroupInterval)
{
  ObMViewSchedInfo *on_demand = make_node(100, 0);
  ObMViewSchedInfo *periodic = make_node(200, ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(on_demand));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(periodic));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_tiered_calc(tops, group_interval));
  EXPECT_EQ(-1, on_demand->refresh_interval_);
  EXPECT_EQ(ObMViewTieredIntervalCalc::BASE_UNIT_US, periodic->refresh_interval_);
  EXPECT_EQ(ObMViewTieredIntervalCalc::BASE_UNIT_US, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, OnDemandDependencyDoesNotBlockPeriodicNode)
{
  ObMViewSchedInfo *on_demand = make_node(100, 0);
  ObMViewSchedInfo *periodic = make_node(200, ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ASSERT_EQ(OB_SUCCESS, periodic->dep_mview_ids_.push_back(on_demand->mview_id_));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(periodic));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_tiered_calc(tops, group_interval));
  EXPECT_EQ(-1, on_demand->refresh_interval_);
  EXPECT_TRUE(periodic->need_periodic_schedule());
  EXPECT_EQ(ObMViewTieredIntervalCalc::BASE_UNIT_US, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, IntervalCalcTrustsGroupMembership)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *suspended = make_node(100, base_unit);
  ObMViewSchedInfo *running = make_node(200, 2 * base_unit);
  suspended->scheduler_suspended_ = true;
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(suspended));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(running));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_default_calc(tops, group_interval));
  EXPECT_EQ(base_unit, suspended->refresh_interval_);
  EXPECT_EQ(2 * base_unit, running->refresh_interval_);
  EXPECT_EQ(base_unit, group_interval);
}

TEST(ObAlterMViewArgTest, RefreshStateSurvivesCopySerializationAndReset)
{
  const share::schema::ObMVRefreshMode modes[] = {
    share::schema::ObMVRefreshMode::NEVER,
    share::schema::ObMVRefreshMode::DEMAND,
    share::schema::ObMVRefreshMode::MAX,
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(modes); ++i) {
    SCOPED_TRACE(i);
    const bool is_suspended = share::schema::ObMVRefreshMode::MAX == modes[i];
    obrpc::ObAlterMViewArg arg;
    if (is_suspended) {
      arg.set_sched_suspended();
    } else {
      arg.set_refresh_mode(modes[i]);
    }
    EXPECT_TRUE(arg.is_valid());
    EXPECT_FALSE(arg.is_alter_refresh_method());

    obrpc::ObAlterMViewArg copied;
    const share::schema::ObMVRefreshMode previous_mode = share::schema::ObMVRefreshMode::NEVER == modes[i]
        ? share::schema::ObMVRefreshMode::DEMAND : share::schema::ObMVRefreshMode::NEVER;
    copied.set_refresh_mode(previous_mode);
    if (!is_suspended) {
      copied.set_sched_suspended();
    }
    ASSERT_EQ(OB_SUCCESS, copied.assign(arg));
    EXPECT_EQ(!is_suspended, copied.is_alter_refresh_mode());
    EXPECT_EQ(modes[i], copied.get_refresh_mode());
    EXPECT_EQ(is_suspended, copied.is_alter_sched_suspended());
    EXPECT_EQ(is_suspended, copied.is_sched_suspended());
    EXPECT_FALSE(copied.is_alter_refresh_method());

    char buf[4096];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, copied.serialize(buf, sizeof(buf), pos));
    EXPECT_EQ(copied.get_serialize_size(), pos);
    obrpc::ObAlterMViewArg decoded;
    decoded.set_refresh_mode(previous_mode);
    if (!is_suspended) {
      decoded.set_sched_suspended();
    }
    int64_t decode_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
    EXPECT_EQ(pos, decode_pos);
    EXPECT_TRUE(decoded.is_valid());
    EXPECT_EQ(!is_suspended, decoded.is_alter_refresh_mode());
    EXPECT_EQ(modes[i], decoded.get_refresh_mode());
    EXPECT_EQ(is_suspended, decoded.is_alter_sched_suspended());
    EXPECT_EQ(is_suspended, decoded.is_sched_suspended());
    EXPECT_FALSE(decoded.is_alter_refresh_method());

    decoded.reset();
    EXPECT_TRUE(decoded.is_valid());
    EXPECT_FALSE(decoded.is_alter_refresh_mode());
    EXPECT_EQ(share::schema::ObMVRefreshMode::MAX, decoded.get_refresh_mode());
    EXPECT_FALSE(decoded.is_alter_sched_suspended());
    EXPECT_FALSE(decoded.is_sched_suspended());
    EXPECT_FALSE(decoded.is_alter_refresh_method());
  }
}

TEST(ObAlterMViewArgTest, RejectsInvalidRefreshMode)
{
  obrpc::ObAlterMViewArg arg;
  EXPECT_TRUE(arg.is_valid());
  arg.set_refresh_mode(share::schema::ObMVRefreshMode::MAX);
  EXPECT_FALSE(arg.is_valid());
  arg.set_refresh_mode(static_cast<share::schema::ObMVRefreshMode>(-1));
  EXPECT_FALSE(arg.is_valid());
  arg.reset();
  EXPECT_TRUE(arg.is_valid());
}

TEST(ObMVRefreshInfoTest, SuspendSurvivesSerializationAndResetRestoresUnsetSentinels)
{
  // Unset sentinels: ALTER treats MAX as "not specified"; CREATE fills real defaults elsewhere.
  const obrpc::ObMVRefreshInfo defaults;
  EXPECT_EQ(share::schema::ObMVRefreshMethod::MAX, defaults.refresh_method_);
  EXPECT_EQ(share::schema::ObMVRefreshMode::MAX, defaults.refresh_mode_);
  EXPECT_EQ(-1, defaults.parallel_);
  EXPECT_EQ(-1, defaults.refresh_dop_);
  EXPECT_EQ(share::schema::ObMVNestedRefreshMode::MAX, defaults.nested_refresh_mode_);
  EXPECT_EQ(OB_INVALID_VERSION, defaults.compat_version_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, defaults.refresh_interval_);
  EXPECT_EQ(share::schema::ObMViewBuildMode::MAX, defaults.build_mode_);
  EXPECT_FALSE(defaults.sched_suspended_);

  obrpc::ObMVRefreshInfo info;
  info.sched_suspended_ = true;
  EXPECT_FALSE(defaults == info);
  info.refresh_method_ = share::schema::ObMVRefreshMethod::FAST;
  info.refresh_mode_ = share::schema::ObMVRefreshMode::NEVER;
  char buf[4096];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, info.serialize(buf, sizeof(buf), pos));
  EXPECT_EQ(info.get_serialize_size(), pos);
  obrpc::ObMVRefreshInfo decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(decoded.sched_suspended_);
  EXPECT_TRUE(info == decoded);

  decoded.reset();
  EXPECT_EQ(share::schema::ObMVRefreshMethod::MAX, decoded.refresh_method_);
  EXPECT_EQ(share::schema::ObMVRefreshMode::MAX, decoded.refresh_mode_);
  EXPECT_EQ(-1, decoded.parallel_);
  EXPECT_EQ(-1, decoded.refresh_dop_);
  EXPECT_EQ(share::schema::ObMVNestedRefreshMode::MAX, decoded.nested_refresh_mode_);
  EXPECT_EQ(OB_INVALID_VERSION, decoded.compat_version_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, decoded.refresh_interval_);
  EXPECT_EQ(share::schema::ObMViewBuildMode::MAX, decoded.build_mode_);
  EXPECT_FALSE(decoded.sched_suspended_);
  EXPECT_TRUE(defaults == decoded);
}

TEST(ObMViewInfoTest, SchedulerFlagPreservesOtherBits)
{
  share::schema::ObMViewInfo info;
  EXPECT_FALSE(info.is_refresh_scheduler_suspended());
  info.set_refresh_flags(1ULL << 8);
  info.set_refresh_scheduler_suspended(true);
  EXPECT_TRUE(info.is_refresh_scheduler_suspended());
  EXPECT_NE(0UL, info.get_refresh_flags() & (1ULL << 8));
  info.set_refresh_scheduler_suspended(false);
  EXPECT_FALSE(info.is_refresh_scheduler_suspended());
  EXPECT_EQ(1ULL << 8, info.get_refresh_flags());
}

TEST_F(ObMViewIntervalCalcTest, TieredCalcMarksChildPeriodFasterThanDepNotApplicable)
{
  ObMViewSchedInfo *dep = make_node(10, 8 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ObMViewSchedInfo *child = make_node(20, 3 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ObSEArray<uint64_t, 8> all_mview_ids;
  ObSEArray<uint64_t, 1> top_mview_ids;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  sql::ObSqlArray<uint64_t> top_arr(alloc_);
  ObMViewIntervalCalcRes calc_res;
  ASSERT_EQ(OB_SUCCESS, top_mview_ids.push_back(child->mview_id_));
  ASSERT_EQ(OB_SUCCESS, top_arr.assign(top_mview_ids));
  ASSERT_EQ(OB_SUCCESS,
            ObMViewSchedInfoUtils::collect_mview_ids_in_refresh_order(sched_info_mgr_.info_map_,
                                                                      top_mview_ids,
                                                                      all_mview_ids));
  ASSERT_EQ(OB_SUCCESS, calc_res.prepare(TENANT_ID, all_mview_ids.count()));
  ObMViewTieredIntervalCalc calc(TENANT_ID, sched_info_mgr_.info_map_, top_arr, calc_res);
  ASSERT_EQ(OB_SUCCESS, calc.calc());
  EXPECT_FALSE(calc_res.is_applicable_);
}

TEST_F(ObMViewIntervalCalcTest, DefaultCalcRejectsChildPeriodFasterThanDep)
{
  ObMViewSchedInfo *dep = make_node(10, 8 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ObMViewSchedInfo *child = make_node(20, 3 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  dep->refresh_interval_ = 101;
  child->refresh_interval_ = 202;
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  int64_t group_interval = 303;
  EXPECT_EQ(OB_ERR_UNEXPECTED, run_default_calc(tops, group_interval));
  EXPECT_EQ(101, dep->refresh_interval_);
  EXPECT_EQ(202, child->refresh_interval_);
  EXPECT_EQ(303, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, DefaultCalcRejectsEmptyTopList)
{
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  int64_t group_interval = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, run_default_calc(tops, group_interval));
  EXPECT_EQ(-1, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, DefaultCalcReturnsErrorForNullTop)
{
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(NULL));
  int64_t group_interval = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, run_default_calc(tops, group_interval));
}

TEST_F(ObMViewIntervalCalcTest, RejectsNullDependencyWithoutCrashing)
{
  ObMViewSchedInfo *top = make_node(10, ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ASSERT_EQ(OB_SUCCESS, top->dep_mview_ids_.push_back(OB_INVALID_ID));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  int64_t group_interval = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, run_tiered_calc(tops, group_interval));
}

TEST_F(ObMViewIntervalCalcTest, TieredCalcHandlesMaximumInt64Interval)
{
  ObMViewSchedInfo *top = make_node(10, INT64_MAX);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(top));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_tiered_calc(tops, group_interval));
  const int64_t max_tier = ObMViewTieredIntervalCalc::BASE_UNIT_US * (1LL << 43);
  EXPECT_EQ(max_tier, top->refresh_interval_);
  EXPECT_EQ(max_tier, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, ValidChainPreservesDepFirstOrdering)
{
  ObMViewSchedInfo *dep = make_node(10, 2 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ObMViewSchedInfo *child = make_node(20, 8 * ObMViewTieredIntervalCalc::BASE_UNIT_US);
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_tiered_calc(tops, group_interval));
  EXPECT_EQ(2 * ObMViewTieredIntervalCalc::BASE_UNIT_US, dep->refresh_interval_);
  EXPECT_EQ(8 * ObMViewTieredIntervalCalc::BASE_UNIT_US, child->refresh_interval_);
  EXPECT_EQ(2 * ObMViewTieredIntervalCalc::BASE_UNIT_US, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, LeastCommonMultipleSingleSourcePreservesSpecifiedInterval)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *source = make_node(10, 95 * base_unit);
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(source));
  int64_t group_interval = -1;
  bool is_applicable = false;
  ASSERT_EQ(OB_SUCCESS, run_lcm_calc(tops, group_interval, is_applicable));
  EXPECT_TRUE(is_applicable);
  EXPECT_EQ(95 * base_unit, source->refresh_interval_);
  EXPECT_EQ(95 * base_unit, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, LeastCommonMultipleAllowsFivePercentDecrease)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *dep = make_node(10, 95 * base_unit);
  ObMViewSchedInfo *child = make_node(20, 100 * base_unit);
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  int64_t group_interval = -1;
  bool is_applicable = false;
  ASSERT_EQ(OB_SUCCESS, run_lcm_calc(tops, group_interval, is_applicable));
  EXPECT_TRUE(is_applicable);
  EXPECT_EQ(95 * base_unit, dep->refresh_interval_);
  EXPECT_EQ(95 * base_unit, child->refresh_interval_);
  EXPECT_EQ(95 * base_unit, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, LeastCommonMultipleCombinesDependenciesAndGroupUsesGcd)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *first_dep = make_node(10, 6 * base_unit);
  ObMViewSchedInfo *second_dep = make_node(20, 10 * base_unit);
  ObMViewSchedInfo *child = make_node(30, 31 * base_unit);
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(20));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  int64_t group_interval = -1;
  bool is_applicable = false;
  ASSERT_EQ(OB_SUCCESS, run_lcm_calc(tops, group_interval, is_applicable));
  EXPECT_TRUE(is_applicable);
  EXPECT_EQ(30 * base_unit, child->refresh_interval_);
  EXPECT_EQ(2 * base_unit, group_interval);
}

// The LCM calculator keeps non power-of-two node intervals, so the group interval
// is the GCD of the member intervals rather than the smallest one.
TEST_F(ObMViewIntervalCalcTest, LeastCommonMultipleGroupIntervalIsGcdNotMinAcrossTops)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *first_top = make_node(10, 9 * base_unit);
  ObMViewSchedInfo *second_top = make_node(20, 6 * base_unit);
  ObSEArray<ObMViewSchedInfo *, 2> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(first_top));
  ASSERT_EQ(OB_SUCCESS, tops.push_back(second_top));
  int64_t group_interval = -1;
  bool is_applicable = false;
  ASSERT_EQ(OB_SUCCESS, run_lcm_calc(tops, group_interval, is_applicable));
  EXPECT_TRUE(is_applicable);
  EXPECT_EQ(9 * base_unit, first_top->refresh_interval_);
  EXPECT_EQ(6 * base_unit, second_top->refresh_interval_);
  EXPECT_EQ(3 * base_unit, group_interval);
}

TEST_F(ObMViewIntervalCalcTest, LeastCommonMultipleIsNotApplicableWhenDecreaseExceedsFivePercent)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *dep = make_node(10, 90 * base_unit);
  ObMViewSchedInfo *child = make_node(20, 100 * base_unit);
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  int64_t group_interval = -1;
  bool is_applicable = true;
  EXPECT_EQ(OB_SUCCESS, run_lcm_calc(tops, group_interval, is_applicable));
  EXPECT_FALSE(is_applicable);
}

TEST_F(ObMViewIntervalCalcTest, LeastCommonMultipleIsNotApplicableWhenLcmExceedsSpecifiedInterval)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *first_dep = make_node(10, 7 * base_unit);
  ObMViewSchedInfo *second_dep = make_node(20, 11 * base_unit);
  ObMViewSchedInfo *child = make_node(30, 20 * base_unit);
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(20));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  int64_t group_interval = -1;
  bool is_applicable = true;
  EXPECT_EQ(OB_SUCCESS, run_lcm_calc(tops, group_interval, is_applicable));
  EXPECT_FALSE(is_applicable);
}

TEST_F(ObMViewIntervalCalcTest, FallsBackToTieredWhenLcmIsNotApplicable)
{
  const int64_t base_unit = ObMViewTieredIntervalCalc::BASE_UNIT_US;
  ObMViewSchedInfo *dep = make_node(10, 90 * base_unit);
  ObMViewSchedInfo *child = make_node(20, 100 * base_unit);
  ASSERT_EQ(OB_SUCCESS, child->dep_mview_ids_.push_back(10));
  ObSEArray<ObMViewSchedInfo *, 1> tops;
  ASSERT_EQ(OB_SUCCESS, tops.push_back(child));
  int64_t group_interval = -1;
  ASSERT_EQ(OB_SUCCESS, run_default_calc(tops, group_interval));
  EXPECT_EQ(64 * base_unit, dep->refresh_interval_);
  EXPECT_EQ(64 * base_unit, child->refresh_interval_);
  EXPECT_EQ(64 * base_unit, group_interval);
}

TEST(ObMViewPendingTaskDefineTest, TimerHeapNodeCompareOrdersByTriggerTs)
{
  ObMViewTimerHeapNode early;
  early.group_id_ = 1;
  early.next_trigger_ts_ = 100;
  early.modified_ts_ = 1;
  ObMViewTimerHeapNode late;
  late.group_id_ = 1;
  late.next_trigger_ts_ = 200;
  late.modified_ts_ = 1;
  ObMViewTimerHeapNodeCompare cmp;
  // Min-heap: smaller next_trigger_ts has higher priority, so compare returns
  // true when a is ordered after b (a has larger trigger ts).
  EXPECT_FALSE(cmp(early, late));
  EXPECT_TRUE(cmp(late, early));

  ObMViewTimerHeapNode same_ts_a;
  same_ts_a.group_id_ = 10;
  same_ts_a.next_trigger_ts_ = 100;
  same_ts_a.modified_ts_ = 1;
  ObMViewTimerHeapNode same_ts_b;
  same_ts_b.group_id_ = 20;
  same_ts_b.next_trigger_ts_ = 100;
  same_ts_b.modified_ts_ = 1;
  EXPECT_TRUE(cmp(same_ts_b, same_ts_a));
  EXPECT_FALSE(cmp(same_ts_a, same_ts_b));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

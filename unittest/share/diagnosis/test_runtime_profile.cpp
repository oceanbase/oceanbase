/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/diagnosis/ob_runtime_profile.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace std;

class ObRuntimeProfileTest : public ::testing::Test
{};

class MockMonitorOperator : public ObOperator
{
public:
  MockMonitorOperator(ObExecContext &exec_ctx, const ObOpSpec &spec)
      : ObOperator(exec_ctx, spec, nullptr)
  {}
  virtual int inner_get_next_row() override { return OB_ITER_END; }
  virtual void destroy() override {}
};

TEST_F(ObRuntimeProfileTest, add_block_time_only_when_db_time_is_counting)
{
  ObArenaAllocator allocator;
  ObExecContext exec_ctx(allocator);
  ObOpSpec spec(allocator, PHY_INVALID);
  MockMonitorOperator op(exec_ctx, spec);
  ObMonitorNode monitor_node;
  monitor_node.set_op(&op);

  op.cpu_begin_level_ = 0;
  monitor_node.add_block_time(100);
  ASSERT_EQ(0, monitor_node.block_time_);

  op.cpu_begin_level_ = 1;
  monitor_node.add_block_time(100);
  ASSERT_EQ(100, monitor_node.block_time_);
}

TEST_F(ObRuntimeProfileTest, add_block_time_for_monitor_node_without_operator)
{
  ObMonitorNode monitor_node;
  monitor_node.add_block_time(100);
  ASSERT_EQ(100, monitor_node.block_time_);
}

inline void join_filter()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  ObProfileSwitcher switcher(ObProfileId::PHY_JOIN_FILTER);
  uint64_t filter_count = 10000;
  uint64_t total_count = 11000;
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, filter_count);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, total_count);
  get_current_profile()->to_format_json(&arena_alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":10000, \"total row count\":11000}}", json);

  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, filter_count);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, total_count);
  uint64_t io_time=999999;
  INC_METRIC_VAL(ObMetricId::IO_TIME, io_time);
  get_current_profile()->to_format_json(&arena_alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}", json);

};

inline void hash_join()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;

  ObOpProfile op_profile(ObProfileId::PHY_HASH_JOIN, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  get_current_profile()->to_format_json(&arena_alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", json);

  uint64_t io_time=999999;
  INC_METRIC_VAL(ObMetricId::IO_TIME, io_time);

  uint64_t bucket_size = 2048;
  SET_METRIC_VAL(ObMetricId::HASH_BUCKET_COUNT, bucket_size);

  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048}}", json);

  join_filter();
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", json);

  uint64_t hash_row_count = 13000;
  INC_METRIC_VAL(ObMetricId::HASH_ROW_COUNT, hash_row_count);
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", json);

  get_current_profile()->to_format_json(&arena_alloc, json, false, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"io time\":\"999.999us\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}", json);

}

TEST_F(ObRuntimeProfileTest, test_realtime_profile)
{
  hash_join();
}

TEST_F(ObRuntimeProfileTest, test_more_metrics)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  ObOpProfile op_profile(ObProfileId::PHY_JOIN_FILTER, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, 100);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, 100);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_CHECK_COUNT, 100);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_ID, 100);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_LENGTH, 100);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_BIT_SET, 100);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_BY_BASS_COUNT_BEFORE_READY, 100);
  INC_METRIC_VAL(ObMetricId::IO_TIME, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  INC_METRIC_VAL(ObMetricId::RESCAN_TIMES, 100);
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"total row count\":100, \"check row count\":100, \"filter id\":100, \"filter length\":100, \"filter bitset\":100, \"by-pass row count\":100, \"io time\":\"100ns\", \"output rows\":100, \"output batches\":100, \"skipped rows\":100, \"rescan times\":100}}", json);
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::STANDARD);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"total row count\":100, \"check row count\":100, \"filter length\":100, \"io time\":\"100ns\", \"output rows\":100, \"rescan times\":100}}", json);
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::CRITICAL);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"io time\":\"100ns\", \"output rows\":100}}", json);
  cout << json << endl;
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_pod<ObOpProfile<ObMetric>::MetricWrap>::value, "keep MetricWrap as pod");
  system("rm -f test_runtime_profile.log*");
  OB_LOGGER.set_file_name("test_runtime_profile.log", true, true);
  OB_LOGGER.set_log_level("TRACE", "TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}

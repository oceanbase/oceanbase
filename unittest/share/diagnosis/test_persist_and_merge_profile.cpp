/**
 * Copyright (c) 2025 OceanBase
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
#define private public
#define protected public
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/diagnosis/ob_profile_util.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace std;

class ObRuntimeProfileTest : public ::testing::Test
{};

inline void join_filter()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile_json = nullptr;
  ObProfileSwitcher switcher(ObProfileId::PHY_JOIN_FILTER);
  uint64_t filter_count = 10000;
  uint64_t total_count = 11000;
  int64_t persist_profile_size=0;
  ObOpProfile<ObMergeMetric> *merge_profile = nullptr;
  const char *merge_profile_json = nullptr;
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, filter_count);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, total_count);
  get_current_profile()->to_format_json(&arena_alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":10000, \"total row count\":11000}}", json);
  const char *persist_profile = nullptr;
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  ObOpProfile<> *new_profile = nullptr;
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json);
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":10000, \"total row count\":11000}}", persist_profile_json);
  ASSERT_EQ(80, persist_profile_size);
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_JOIN_FILTER,  &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc);
    ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc);
    merge_profile->to_format_json(&arena_alloc, merge_profile_json);
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":20000, \"min\":10000, \"max\":10000}, \"total row count\":{\"sum\":22000, \"min\":11000, \"max\":11000}}}", merge_profile_json);

  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, filter_count);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, total_count);
  uint64_t io_time=999999;//999,999
  INC_METRIC_VAL(ObMetricId::TOTAL_IO_TIME, io_time);
  get_current_profile()->to_format_json(&arena_alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"total io time\":\"999.999US\"}}", json);
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json);
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"total io time\":\"999.999US\"}}", persist_profile_json);
  ASSERT_EQ(96, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_JOIN_FILTER, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc);
    ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc);
    merge_profile->to_format_json(&arena_alloc, merge_profile_json);
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"total io time\":{\"avg\":\"999.999US\", \"min\":\"999.999US\", \"max\":\"999.999US\"}}}", merge_profile_json);
};

inline void hash_join()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile = nullptr;
  int64_t persist_profile_size = 0;
  ObOpProfile<> *new_profile = nullptr;
  const char *persist_profile_json = nullptr;
  ObOpProfile<ObMergeMetric> *merge_profile = nullptr;
  const char *merge_profile_json = nullptr;

  ObOpProfile<> op_profile(ObProfileId::PHY_HASH_JOIN, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  get_current_profile()->to_format_json(&arena_alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", json);
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json);
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", persist_profile_json);
  ASSERT_EQ(48, persist_profile_size);
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc);
    ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc);
    merge_profile->to_format_json(&arena_alloc, merge_profile_json);
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", merge_profile_json);


  uint64_t io_time=999999;
  INC_METRIC_VAL(ObMetricId::TOTAL_IO_TIME, io_time);

  uint64_t bucket_size = 2048;
  SET_METRIC_VAL(ObMetricId::HASH_BUCKET_COUNT, bucket_size);

  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":\"999.999US\", \"bucket size\":2048}}", json);
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC);
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":\"999.999US\", \"bucket size\":2048}}", persist_profile_json);
  ASSERT_EQ(80, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc);
    ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc);
    merge_profile->to_format_json(&arena_alloc, merge_profile_json, true, metric::Level::AD_HOC);
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":{\"avg\":\"999.999US\", \"min\":\"999.999US\", \"max\":\"999.999US\"}, \"bucket size\":{\"sum\":4096}}}", merge_profile_json);

  join_filter();
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":\"999.999US\", \"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"total io time\":\"999.999US\"}}}", json);
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC);
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":\"999.999US\", \"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"total io time\":\"999.999US\"}}}", persist_profile_json);
  ASSERT_EQ(152, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  }
  ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc);
  ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc);
  merge_profile->to_format_json(&arena_alloc, merge_profile_json);
  cout << merge_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":{\"avg\":\"999.999US\", \"min\":\"999.999US\", \"max\":\"999.999US\"}, \"bucket size\":{\"sum\":4096}, \"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"total io time\":{\"avg\":\"999.999US\", \"min\":\"999.999US\", \"max\":\"999.999US\"}}}}", merge_profile_json);


  uint64_t hash_row_count = 13000;
  INC_METRIC_VAL(ObMetricId::HASH_ROW_COUNT, hash_row_count);
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":\"999.999US\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"total io time\":\"999.999US\"}}}", json);
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC);
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":\"999.999US\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"total io time\":\"999.999US\"}}}", persist_profile_json);
  ASSERT_EQ(168, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc);
    ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc);
    merge_profile->to_format_json(&arena_alloc, merge_profile_json);
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"total io time\":{\"avg\":\"999.999US\", \"min\":\"999.999US\", \"max\":\"999.999US\"}, \"bucket size\":{\"sum\":4096}, \"total row count\":{\"sum\":26000}, \"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"total io time\":{\"avg\":\"999.999US\", \"min\":\"999.999US\", \"max\":\"999.999US\"}}}}", merge_profile_json);
}


TEST_F(ObRuntimeProfileTest, test_realtime_profile)
{
  hash_join();
}

TEST_F(ObRuntimeProfileTest, test_mock_rpc_send_persist_profile)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile = nullptr;
  int64_t persist_profile_size = 0;
  ObOpProfile<> *new_profile = nullptr;
  const char *persist_profile_json = nullptr;

  ObOpProfile<> op_profile(ObProfileId::PHY_HASH_JOIN, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", json);
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC);
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", persist_profile_json);

  uint64_t bucket_size = 2048;
  SET_METRIC_VAL(ObMetricId::HASH_BUCKET_COUNT, bucket_size);
  join_filter();
  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);

  char *des_persist_profile = (char*)arena_alloc.alloc(persist_profile_size);
  MEMCPY(des_persist_profile, persist_profile, persist_profile_size);

  convert_persist_profile_to_realtime(des_persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC);
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"total io time\":\"999.999US\"}}}", persist_profile_json);
  ASSERT_EQ(136, persist_profile_size);
}


void stk5()
{
  int ret = OB_SUCCESS;
}

void stk4()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_INSERT);
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
}

void stk3()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_TABLE_SCAN);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
}

void stk2()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_GRANULE_ITERATOR);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk3();
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  stk4();
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  }

void stk1()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_JOIN_FILTER);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, 100);
  stk2();
  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  stk5();
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
}

TEST_F(ObRuntimeProfileTest, test_deep_profile_tree)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc;
  const char *json = nullptr;
  const char *persist_profile = nullptr;
  int64_t persist_profile_size = 0;
  ObOpProfile<> *new_profile = nullptr;
  const char *persist_profile_json = nullptr;

  INC_METRIC_VAL(ObMetricId::SKIPPED_ROWS, 100);
  ObOpProfile<> op_profile(ObProfileId::PHY_HASH_JOIN, &arena_alloc);
  ObProfileSwitcher switcher(&op_profile);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  stk1();
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);

  get_current_profile()->to_format_json(&arena_alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100}, \"PHY_INSERT\":{}}}}}", json);

  get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc);
  convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc);
  new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC);
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"output batches\":100, \"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100, \"skipped rows\":100}, \"PHY_INSERT\":{\"skipped rows\":100, \"output batches\":100}}}}}", persist_profile_json);
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  system("rm -f test_persist_and_merge_profile.log*");
  OB_LOGGER.set_file_name("test_persist_and_merge_profile.log", true, true);
  OB_LOGGER.set_log_level("TRACE", "TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}

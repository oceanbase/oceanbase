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
#include "pl/sys_package/ob_dbms_xprofile.cpp"

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
  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":10000, \"total row count\":11000}}", json);
  const char *persist_profile = nullptr;
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  ObOpProfile<> *new_profile = nullptr;
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":10000, \"total row count\":11000}}", persist_profile_json);
  ASSERT_EQ(80, persist_profile_size);
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_JOIN_FILTER,  &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":20000, \"min\":10000, \"max\":10000}, \"total row count\":{\"sum\":22000, \"min\":11000, \"max\":11000}}}", merge_profile_json);

  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_FILTERED_COUNT, filter_count);
  INC_METRIC_VAL(ObMetricId::JOIN_FILTER_TOTAL_COUNT, total_count);
  uint64_t io_time=999999;//999,999
  INC_METRIC_VAL(ObMetricId::IO_TIME, io_time);
  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}", persist_profile_json);
  ASSERT_EQ(96, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_JOIN_FILTER, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}}}", merge_profile_json);
  ASSERT_EQ(0, ret);
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
  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", persist_profile_json);
  ASSERT_EQ(48, persist_profile_size);
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", merge_profile_json);


  uint64_t io_time=999999;
  INC_METRIC_VAL(ObMetricId::IO_TIME, io_time);

  uint64_t bucket_size = 2048;
  SET_METRIC_VAL(ObMetricId::HASH_BUCKET_COUNT, bucket_size);

  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048}}", persist_profile_json);
  ASSERT_EQ(80, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json, true, metric::Level::AD_HOC));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}, \"bucket size\":{\"sum\":4096}}}", merge_profile_json);

  join_filter();
  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", persist_profile_json);
  ASSERT_EQ(152, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  }
  OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
  OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
  OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
  cout << merge_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}, \"bucket size\":{\"sum\":4096}, \"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}}}}", merge_profile_json);


  uint64_t hash_row_count = 13000;
  INC_METRIC_VAL(ObMetricId::HASH_ROW_COUNT, hash_row_count);
  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  cout << persist_profile_size << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":\"999.999us\", \"bucket size\":2048, \"total row count\":13000, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", persist_profile_json);
  ASSERT_EQ(168, persist_profile_size);
  merge_profile->~ObOpProfile<ObMergeMetric>();
  merge_profile = nullptr;
  if (OB_ISNULL(merge_profile = OB_NEWx(ObOpProfile<ObMergeMetric>, &arena_alloc, ObProfileId::PHY_HASH_JOIN, &arena_alloc))) {
    cout << "failed to allocate memory" << endl;
  } else {
    OZ(ObProfileUtil::merge_profile(*merge_profile, get_current_profile(), &arena_alloc));
    OZ(ObProfileUtil::merge_profile(*merge_profile, new_profile, &arena_alloc));
    OZ(merge_profile->to_format_json(&arena_alloc, merge_profile_json));
    cout << merge_profile_json << endl;
  }
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}, \"bucket size\":{\"sum\":4096}, \"total row count\":{\"sum\":26000}, \"PHY_JOIN_FILTER\":{\"filtered row count\":{\"sum\":40000, \"min\":20000, \"max\":20000}, \"total row count\":{\"sum\":44000, \"min\":22000, \"max\":22000}, \"io time\":{\"avg\":\"999.999us\", \"min\":\"999.999us\", \"max\":\"999.999us\"}}}}", merge_profile_json);
  ASSERT_EQ(0, ret);
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
  OZ(get_current_profile()->to_format_json(&arena_alloc, json, true, metric::Level::AD_HOC));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", json);
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{}}", persist_profile_json);

  uint64_t bucket_size = 2048;
  SET_METRIC_VAL(ObMetricId::HASH_BUCKET_COUNT, bucket_size);
  join_filter();
  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));

  char *des_persist_profile = (char*)arena_alloc.alloc(persist_profile_size);
  MEMCPY(des_persist_profile, persist_profile, persist_profile_size);

  OZ(convert_persist_profile_to_realtime(des_persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"bucket size\":2048, \"PHY_JOIN_FILTER\":{\"filtered row count\":20000, \"total row count\":22000, \"io time\":\"999.999us\"}}}", persist_profile_json);
  ASSERT_EQ(136, persist_profile_size);
  ASSERT_EQ(0, ret);
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
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
}

void stk0_0()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_LIMIT);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk1();
}

void stk0_1()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_SUBPLAN_SCAN);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk1();
}

void stk0_2()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::PHY_HASH_GROUP_BY);
  INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
  stk1();
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
  stk0_0();
  stk0_1();
  stk0_2();
  INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);

  OZ(get_current_profile()->to_format_json(&arena_alloc, json));
  cout << json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"output rows\":100, \"PHY_LIMIT\":{\"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100}, \"PHY_INSERT\":{}}}}, \"PHY_SUBPLAN_SCAN\":{\"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100}, \"PHY_INSERT\":{}}}}, \"PHY_HASH_GROUP_BY\":{\"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100}, \"PHY_INSERT\":{}}}}}}", json);

  OZ(get_current_profile()->to_persist_profile(persist_profile, persist_profile_size, &arena_alloc));
  OZ(convert_persist_profile_to_realtime(persist_profile, persist_profile_size, new_profile, &arena_alloc));
  OZ(new_profile->to_format_json(&arena_alloc, persist_profile_json, true, metric::Level::AD_HOC));
  cout << persist_profile_json << endl;
  ASSERT_STREQ("{\"PHY_HASH_JOIN\":{\"output batches\":100, \"output rows\":100, \"PHY_LIMIT\":{\"output batches\":100, \"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100, \"skipped rows\":100}, \"PHY_INSERT\":{\"skipped rows\":100, \"output batches\":100}}}}, \"PHY_SUBPLAN_SCAN\":{\"output batches\":100, \"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100, \"skipped rows\":100}, \"PHY_INSERT\":{\"skipped rows\":100, \"output batches\":100}}}}, \"PHY_HASH_GROUP_BY\":{\"output batches\":100, \"output rows\":100, \"PHY_JOIN_FILTER\":{\"filtered row count\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_GRANULE_ITERATOR\":{\"output rows\":100, \"skipped rows\":100, \"output batches\":100, \"PHY_TABLE_SCAN\":{\"output rows\":100, \"skipped rows\":100}, \"PHY_INSERT\":{\"skipped rows\":100, \"output batches\":100}}}}}}", persist_profile_json);
  const char *pretty_text = nullptr;
  OZ(new_profile->pretty_print(&arena_alloc, pretty_text, "", "", "  ", metric::Level::AD_HOC));
  if (pretty_text != nullptr) {
    cout << pretty_text << endl;
  }
  ASSERT_STREQ("PHY_HASH_JOIN\n  output batches:100\n  output rows:100\n  PHY_LIMIT\n    output batches:100\n    output rows:100\n    PHY_JOIN_FILTER\n      filtered row count:100\n      skipped rows:100\n      output batches:100\n      PHY_GRANULE_ITERATOR\n        output rows:100\n        skipped rows:100\n        output batches:100\n        PHY_TABLE_SCAN\n          output rows:100\n          skipped rows:100\n        PHY_INSERT\n          skipped rows:100\n          output batches:100\n  PHY_SUBPLAN_SCAN\n    output batches:100\n    output rows:100\n    PHY_JOIN_FILTER\n      filtered row count:100\n      skipped rows:100\n      output batches:100\n      PHY_GRANULE_ITERATOR\n        output rows:100\n        skipped rows:100\n        output batches:100\n        PHY_TABLE_SCAN\n          output rows:100\n          skipped rows:100\n        PHY_INSERT\n          skipped rows:100\n          output batches:100\n  PHY_HASH_GROUP_BY\n    output batches:100\n    output rows:100\n    PHY_JOIN_FILTER\n      filtered row count:100\n      skipped rows:100\n      output batches:100\n      PHY_GRANULE_ITERATOR\n        output rows:100\n        skipped rows:100\n        output batches:100\n        PHY_TABLE_SCAN\n          output rows:100\n          skipped rows:100\n        PHY_INSERT\n          skipped rows:100\n          output batches:100", pretty_text);
  ASSERT_EQ(0, ret);

}

void add_child_profile(ObProfileId child_profile_id, ObMetricId metric_id, uint64_t metric_value)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(child_profile_id);
  INC_METRIC_VAL(metric_id, metric_value);
}

class ProfilePrinter
{
public:
  struct OpProfileInfo {
    ObOpProfile<> *op_profile_;
    int64_t op_id_;
    int64_t op_depth_;
  };
  ProfilePrinter() {
    int ret = OB_SUCCESS;
    OpProfileInfo op_profile_infos[MAX_OP_COUNT];
    op_profile_infos[0] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_COORD, &arena_alloc_), 0, 0};
    op_profile_infos[1] = {new ObOpProfile<>(ObProfileId::PHY_PX_REDUCE_TRANSMIT, &arena_alloc_), 1, 1};
    op_profile_infos[2] = {new ObOpProfile<>(ObProfileId::PHY_MERGE_UNION, &arena_alloc_), 2, 2};
    op_profile_infos[3] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_RECEIVE, &arena_alloc_), 3, 3};
    op_profile_infos[4] = {new ObOpProfile<>(ObProfileId::PHY_PX_REPART_TRANSMIT, &arena_alloc_), 4, 4};
    op_profile_infos[5] = {new ObOpProfile<>(ObProfileId::PHY_GRANULE_ITERATOR, &arena_alloc_), 5, 5};
    op_profile_infos[6] = {new ObOpProfile<>(ObProfileId::PHY_TABLE_SCAN, &arena_alloc_), 6, 6};
    op_profile_infos[7] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_RECEIVE, &arena_alloc_), 7, 3};
    op_profile_infos[8] = {new ObOpProfile<>(ObProfileId::PHY_PX_REPART_TRANSMIT, &arena_alloc_), 8, 4};
    op_profile_infos[9] = {new ObOpProfile<>(ObProfileId::PHY_GRANULE_ITERATOR, &arena_alloc_), 9, 5};
    op_profile_infos[10] = {new ObOpProfile<>(ObProfileId::PHY_TABLE_SCAN, &arena_alloc_), 10, 6};
    op_profile_infos[11] = {new ObOpProfile<>(ObProfileId::PHY_PX_FIFO_RECEIVE, &arena_alloc_), 11, 3};
    op_profile_infos[12] = {new ObOpProfile<>(ObProfileId::PHY_PX_REPART_TRANSMIT, &arena_alloc_), 12, 4};
    op_profile_infos[13] = {new ObOpProfile<>(ObProfileId::PHY_GRANULE_ITERATOR, &arena_alloc_), 13, 5};
    op_profile_infos[14] = {new ObOpProfile<>(ObProfileId::PHY_TABLE_SCAN, &arena_alloc_), 14, 6};
    {
      ObProfileSwitcher switcher(op_profile_infos[6].op_profile_);
      add_child_profile(ObProfileId::LAKE_TABLE_FILE_READER,
                        ObMetricId::LAKE_TABLE_READ_COUNT, 100);
      add_child_profile(ObProfileId::LAKE_TABLE_FILE_READER,
                        ObMetricId::LAKE_TABLE_SYNC_READ_COUNT, 100);
      add_child_profile(ObProfileId::LAKE_TABLE_STORAGE_IO,
                        ObMetricId::LAKE_TABLE_MAX_IO_TIME, 100);
      add_child_profile(ObProfileId::LAKE_TABLE_STORAGE_IO,
                        ObMetricId::LAKE_TABLE_AVG_IO_TIME, 100);
    }
    for (int64_t i = 0; i < MAX_OP_COUNT; i++) {
      ObProfileItem item;
      item.op_id_ = op_profile_infos[i].op_id_;
      item.profile_ = op_profile_infos[i].op_profile_;
      item.plan_depth_ = op_profile_infos[i].op_depth_;
      all_profiles_.push_back(item);
      ObProfileSwitcher switcher(op_profile_infos[i].op_profile_);
      INC_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, 100);
      INC_METRIC_VAL(ObMetricId::OUTPUT_ROWS, 100);
    }
  }
  ~ProfilePrinter() {
    for (int64_t i = 0; i < MAX_OP_COUNT; i++) {
      delete all_profiles_[i].profile_;
    }
  }
  void print_selected_profile(const std::vector<int64_t> &selected_op_ids, char *&buf);
private:
  static const int64_t MAX_OP_COUNT = 15;
  ObArenaAllocator arena_alloc_;
  sql::ObTMArray<ObProfileItem> all_profiles_;
};

void ProfilePrinter::print_selected_profile(const std::vector<int64_t> &selected_op_ids, char *&buf)
{
  int ret = OB_SUCCESS;
  sql::ObTMArray<ObProfileItem> profile_items;
  sql::ObTMArray<ObMergedProfileItem> merged_items;
  sql::ObTMArray<ExecutionBound> execution_bounds;
  for (int64_t op_id : selected_op_ids) {
    profile_items.push_back(all_profiles_.at(op_id));
  }
  OZ(ObProfileUtil::get_merged_profiles(&arena_alloc_, profile_items, merged_items, execution_bounds));
  pl::ProfilePrefixHelper prefix_helper(arena_alloc_);
  OZ(prefix_helper.prepare_pretty_prefix(merged_items));
  int64_t buf_len = 1024 * 1024;
  int64_t pos = 0;
  buf = static_cast<char *>(arena_alloc_.alloc(buf_len));
  pos = 0;
  for (int64_t i = 0; i < merged_items.count() && OB_SUCC(ret); ++i) {
    const char *text = nullptr;
    const ObMergedProfileItem &item = merged_items.at(i);
    OZ(item.profile_->pretty_print(
        &arena_alloc_, text, prefix_helper.get_prefixs().at(i).profile_prefix_,
        prefix_helper.get_prefixs().at(i).profile_suffix_,
        prefix_helper.get_prefixs().at(i).metric_prefix_, metric::Level::AD_HOC));
    OZ(BUF_PRINTF("%s\n", text));
  }
  cout << buf << endl;
  ASSERT_EQ(0, ret);
}

TEST_F(ObRuntimeProfileTest, test_pretty_print_profile_1)
{
  int ret = OB_SUCCESS;
  ProfilePrinter printer;
  char *buf = nullptr;
  /* full plan is:
    0.PHY_PX_FIFO_COORD
    └─1.PHY_PX_REDUCE_TRANSMIT
      └─2.PHY_MERGE_UNION
        ├─3.PHY_PX_FIFO_RECEIVE
        │ └─4.PHY_PX_REPART_TRANSMIT
        │   └─5.PHY_GRANULE_ITERATOR
        │     └─6.PHY_TABLE_SCAN
        ├─7.PHY_PX_FIFO_RECEIVE
        │ └─8.PHY_PX_REPART_TRANSMIT
        │   └─9.PHY_GRANULE_ITERATOR
        │     └─10.PHY_TABLE_SCAN
        └─11.PHY_PX_FIFO_RECEIVE
          └─12.PHY_PX_REPART_TRANSMIT
            └─13.PHY_GRANULE_ITERATOR
              └─14.PHY_TABLE_SCAN
  */
  printer.print_selected_profile({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, buf);
  ASSERT_STREQ("0.PHY_PX_FIFO_COORD(<1%, dop=1)\n\xE2\x94\x82  output batches:100 [min=100, max=100]\n\xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n\xE2\x94\x94\xE2\x94\x80" "1.PHY_PX_REDUCE_TRANSMIT(<1%, dop=1)\n  \xE2\x94\x82  output batches:100 [min=100, max=100]\n  \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n  \xE2\x94\x94\xE2\x94\x80" "2.PHY_MERGE_UNION(<1%, dop=1)\n    \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x9C\xE2\x94\x80" "3.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n    \xE2\x94\x82 \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82 \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82 \xE2\x94\x94\xE2\x94\x80" "4.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n    \xE2\x94\x82   \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82   \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82   \xE2\x94\x94\xE2\x94\x80" "5.PHY_GRANULE_ITERATOR(<1%, dop=1)\n    \xE2\x94\x82     \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82     \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82     \xE2\x94\x94\xE2\x94\x80" "6.PHY_TABLE_SCAN(<1%, dop=1)\n    \xE2\x94\x82         output batches:100 [min=100, max=100]\n    \xE2\x94\x82         output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82         Lake Table File Reader\n    \xE2\x94\x82           lake table read count:100\n    \xE2\x94\x82           lake table sync read count:100\n    \xE2\x94\x82         Lake Table Storage IO\n    \xE2\x94\x82           lake table max io time:100ns [max=100ns]\n    \xE2\x94\x82           lake table avg io time:100ns\n    \xE2\x94\x9C\xE2\x94\x80" "7.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n    \xE2\x94\x82 \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82 \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82 \xE2\x94\x94\xE2\x94\x80" "8.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n    \xE2\x94\x82   \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82   \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82   \xE2\x94\x94\xE2\x94\x80" "9.PHY_GRANULE_ITERATOR(<1%, dop=1)\n    \xE2\x94\x82     \xE2\x94\x82  output batches:100 [min=100, max=100]\n    \xE2\x94\x82     \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x82     \xE2\x94\x94\xE2\x94\x80" "10.PHY_TABLE_SCAN(<1%, dop=1)\n    \xE2\x94\x82         output batches:100 [min=100, max=100]\n    \xE2\x94\x82         output rows:100 [min=100, max=100, stddev=0]\n    \xE2\x94\x94\xE2\x94\x80" "11.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n      \xE2\x94\x82  output batches:100 [min=100, max=100]\n      \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n      \xE2\x94\x94\xE2\x94\x80" "12.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n        \xE2\x94\x82  output batches:100 [min=100, max=100]\n        \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n        \xE2\x94\x94\xE2\x94\x80" "13.PHY_GRANULE_ITERATOR(<1%, dop=1)\n          \xE2\x94\x82  output batches:100 [min=100, max=100]\n          \xE2\x94\x82  output rows:100 [min=100, max=100, stddev=0]\n          \xE2\x94\x94\xE2\x94\x80" "14.PHY_TABLE_SCAN(<1%, dop=1)\n              output batches:100 [min=100, max=100]\n              output rows:100 [min=100, max=100, stddev=0]\n", buf);
  printer.print_selected_profile({0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, buf);
  ASSERT_STREQ("0.PHY_PX_FIFO_COORD(<1%, dop=1)\n   output batches:100 [min=100, max=100]\n   output rows:100 [min=100, max=100, stddev=0]\n      3.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        4.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n          5.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            6.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n               Lake Table File Reader\n                 lake table read count:100\n                 lake table sync read count:100\n               Lake Table Storage IO\n                 lake table max io time:100ns [max=100ns]\n                 lake table avg io time:100ns\n      7.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        8.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n          9.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            10.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n      11.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        12.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n", buf);
  printer.print_selected_profile({0, 5, 6, 9, 10, 11, 12}, buf);
  ASSERT_STREQ("0.PHY_PX_FIFO_COORD(<1%, dop=1)\n   output batches:100 [min=100, max=100]\n   output rows:100 [min=100, max=100, stddev=0]\n          5.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            6.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n               Lake Table File Reader\n                 lake table read count:100\n                 lake table sync read count:100\n               Lake Table Storage IO\n                 lake table max io time:100ns [max=100ns]\n                 lake table avg io time:100ns\n          9.PHY_GRANULE_ITERATOR(<1%, dop=1)\n             output batches:100 [min=100, max=100]\n             output rows:100 [min=100, max=100, stddev=0]\n            10.PHY_TABLE_SCAN(<1%, dop=1)\n               output batches:100 [min=100, max=100]\n               output rows:100 [min=100, max=100, stddev=0]\n      11.PHY_PX_FIFO_RECEIVE(<1%, dop=1)\n         output batches:100 [min=100, max=100]\n         output rows:100 [min=100, max=100, stddev=0]\n        12.PHY_PX_REPART_TRANSMIT(<1%, dop=1)\n           output batches:100 [min=100, max=100]\n           output rows:100 [min=100, max=100, stddev=0]\n", buf);
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

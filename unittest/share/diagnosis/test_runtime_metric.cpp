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
#include "share/diagnosis/ob_runtime_metrics.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace std;

class ObRuntimeMetricTest : public ::testing::Test
{};

TEST_F(ObRuntimeMetricTest, test_counter)
{
  ObArenaAllocator alloc;
  const char *json = nullptr;
  ObMetric numerical_counter;
  numerical_counter.id_ = ObMetricId::JOIN_FILTER_FILTERED_COUNT;
  numerical_counter.value_ = 0;
  numerical_counter.inc(10);
  numerical_counter.inc(20);
  numerical_counter.inc(30);
  ASSERT_EQ(60, numerical_counter.value());
  numerical_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"filtered row count\":60}", json);

  numerical_counter.inc(1000);
  ASSERT_EQ(1060, numerical_counter.value());
  numerical_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"filtered row count\":1060}", json);

  numerical_counter.inc(15000000);
  numerical_counter.to_format_json(&alloc, json);
  ASSERT_EQ(15001060, numerical_counter.value());
  ASSERT_STREQ("{\"filtered row count\":15001060}", json);


  ObMetric byte_counter;
  byte_counter.id_ = ObMetricId::IO_READ_BYTES;
  byte_counter.value_ = 0;
  byte_counter.inc(10);
  byte_counter.inc(20);
  byte_counter.inc(30);
  ASSERT_EQ(60, byte_counter.value());
  byte_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"total io bytes read from disk\":\"60B\"}", json);

  byte_counter.inc(1000);
  ASSERT_EQ(1060, byte_counter.value());
  byte_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"total io bytes read from disk\":\"1.035KB\"}", json);

  byte_counter.inc(15000000);
  byte_counter.to_format_json(&alloc, json);
  ASSERT_EQ(15001060, byte_counter.value());
  ASSERT_STREQ("{\"total io bytes read from disk\":\"14.306MB\"}", json);


  ObMetric time_counter;
  time_counter.id_ = ObMetricId::IO_TIME;
  time_counter.value_ = 0;
  time_counter.inc(10);
  time_counter.inc(20);
  time_counter.inc(30);
  ASSERT_EQ(60, time_counter.value());
  time_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"io time\":\"60ns\"}", json);

  time_counter.inc(1000);
  ASSERT_EQ(1060, time_counter.value());
  time_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"io time\":\"1.060us\"}", json);

  time_counter.inc(15000000);
  time_counter.to_format_json(&alloc, json);
  ASSERT_EQ(15001060, time_counter.value());
  ASSERT_STREQ("{\"io time\":\"15.001ms\"}", json);

  ObMetric timestamp_gauge;
  timestamp_gauge.id_ = ObMetricId::OPEN_TIME;
  timestamp_gauge.value_ = 0;
  timestamp_gauge.set(1753983318743793);
  ASSERT_EQ(1753983318743793, timestamp_gauge.value());
  timestamp_gauge.to_format_json(&alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"open time\":\"2025-08-01 01:35:18.743793\"}", json);
}

TEST_F(ObRuntimeMetricTest, test_gauge)
{
  ObArenaAllocator alloc;
  const char *json = nullptr;
  ObMetric gauge;
  gauge.id_ = ObMetricId::HASH_BUCKET_COUNT;
  gauge.value_ = 0;
  gauge.set(10);
  gauge.set(30);
  gauge.set(20);
  ASSERT_EQ(20, gauge.value());
  gauge.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"bucket size\":20}", json);
}

TEST_F(ObRuntimeMetricTest, test_pretty_print)
{
  ObArenaAllocator alloc;
  int64_t buf_len = 1024;
  int64_t pos = 0;
  char *buf = static_cast<char *>(alloc.alloc(buf_len));
  const ObString prefix = "│  ";
  ObMetric gauge;
  gauge.id_ = ObMetricId::JOIN_FILTER_FILTERED_COUNT;
  gauge.value_ = 10;
  gauge.pretty_print(buf, buf_len, pos, prefix);
  cout << buf << endl;
  ASSERT_STREQ("│  filtered row count:10", buf);

  ObMergeMetric merged_gauge;
  merged_gauge.id_ = ObMetricId::JOIN_FILTER_FILTERED_COUNT;
  merged_gauge.update(gauge.value());
  pos = 0;
  merged_gauge.pretty_print(buf, buf_len, pos, prefix);
  cout << buf << endl;
  ASSERT_STREQ("│  filtered row count:10 [min=10, max=10]", buf);
}

TEST_F(ObRuntimeMetricTest, test_standard_deviation) {
  // Test standard deviation calculation using Welford's algorithm
  // test with values [10, 20]
  // mean: (10+20) / 2 = 15
  // variance: sum((x-15)^2) / 2 = (25+25) / 2 = 25
  // standard deviation: sqrt(25) = 5
  ObMergeMetric merged_metric;
  merged_metric.id_ = ObMetricId::OUTPUT_ROWS;
  merged_metric.update(10);
  merged_metric.update(20);
  ASSERT_EQ(5, merged_metric.get_deviation_value());

  // test with values [2, 4, 4, 4, 5, 5, 7, 9]
  // mean: (2+4+4+4+5+5+7+9) / 8 = 40 / 8 = 5
  // variance: sum((x-5)^2) / 8 = (9+1+1+1+0+0+4+16) / 8 = 32 / 8 = 4
  // standard deviation: sqrt(4) = 2
  ObMergeMetric merged_metric2;
  merged_metric2.id_ = ObMetricId::OUTPUT_ROWS;
  uint64_t test_values[] = {2, 4, 4, 4, 5, 5, 7, 9};
  int num_values = sizeof(test_values) / sizeof(test_values[0]);

  for (int i = 0; i < num_values; i++) {
    merged_metric2.update(test_values[i]);
  }

  ASSERT_EQ(2, merged_metric2.get_deviation_value());

  // test all same values
  ObMergeMetric merged_metric3;
  merged_metric3.id_ = ObMetricId::OUTPUT_ROWS;
  for (int i = 0; i < 5; i++) {
    merged_metric3.update(100);
  }
  ASSERT_EQ(0, merged_metric3.get_deviation_value());

  // test large values
  // mean: (1000+2000+3000+4000+5000) / 5 = 3000
  // variance: sum((x-3000)^2) / 5 = (4M+1M+0+1M+4M) / 5 = 10M / 5 = 2M
  // standard deviation: sqrt(2000000) ≈ 1414.21
  ObMergeMetric merged_metric4;
  merged_metric4.id_ = ObMetricId::OUTPUT_ROWS;
  uint64_t large_values[] = {1000, 2000, 3000, 4000, 5000};
  int num_large = sizeof(large_values) / sizeof(large_values[0]);

  for (int i = 0; i < num_large; i++) {
    merged_metric4.update(large_values[i]);
  }
  ASSERT_EQ(1414, merged_metric4.get_deviation_value());

  ObArenaAllocator alloc;
  int64_t buf_len = 1024;
  int64_t pos = 0;
  char *buf = static_cast<char *>(alloc.alloc(buf_len));
  merged_metric4.pretty_print(buf, buf_len, pos, "");
  cout << buf << endl;
  ASSERT_STREQ("output rows:15000 [min=1000, max=5000, stddev=1414]", buf);
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_pod<ObMetric>::value, "keep metric as pod");
  system("rm -f test_runtime_metric.log*");
  OB_LOGGER.set_file_name("test_runtime_metric.log", true, true);
  OB_LOGGER.set_log_level("TRACE", "TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}

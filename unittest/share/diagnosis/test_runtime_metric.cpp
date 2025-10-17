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
  time_counter.id_ = ObMetricId::TOTAL_IO_TIME;
  time_counter.value_ = 0;
  time_counter.inc(10);
  time_counter.inc(20);
  time_counter.inc(30);
  ASSERT_EQ(60, time_counter.value());
  time_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"total io time\":\"60NS\"}", json);

  time_counter.inc(1000);
  ASSERT_EQ(1060, time_counter.value());
  time_counter.to_format_json(&alloc, json);
  ASSERT_STREQ("{\"total io time\":\"1.060US\"}", json);

  time_counter.inc(15000000);
  time_counter.to_format_json(&alloc, json);
  ASSERT_EQ(15001060, time_counter.value());
  ASSERT_STREQ("{\"total io time\":\"15.001MS\"}", json);

  ObMetric timestamp_gauge;
  timestamp_gauge.id_ = ObMetricId::OPEN_TIME;
  timestamp_gauge.value_ = 0;
  timestamp_gauge.set(1753983318743793);
  ASSERT_EQ(1753983318743793, timestamp_gauge.value());
  timestamp_gauge.to_format_json(&alloc, json);
  cout << json << endl;
  ASSERT_STREQ("{\"open time\":2025-08-01 01:35:18.743793}", json);
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

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
#define USING_LOG_PREFIX STORAGE
#define private public
#define protected public

#include "share/compaction/ob_schedule_daily_maintenance_window.h"

namespace oceanbase
{
namespace unittest
{

class TestDailyMaintenanceWindow : public ::testing::Test
{
public:
  TestDailyMaintenanceWindow() {}
  ~TestDailyMaintenanceWindow() {}
  void test_config_parse_and_encode(const common::ObString &input_job_config,
                                    const int64_t version,
                                    const int64_t thread_cnt,
                                    const char *prev_plan,
                                    const char *prev_group);
};

void TestDailyMaintenanceWindow::test_config_parse_and_encode(
    const common::ObString &input_job_config,
    const int64_t version,
    const int64_t thread_cnt,
    const char *prev_plan,
    const char *prev_group)
{
  LOG_INFO("Start test parse and encode config", K(input_job_config), K(prev_plan), K(prev_group), K(thread_cnt));
  ObSqlString output_job_config;
  ObArenaAllocator allocator;
  ObDailyWindowJobConfig config(allocator);
  ASSERT_EQ(OB_SUCCESS, config.parse_from_string(input_job_config));
  ASSERT_EQ(version, config.version_);
  ASSERT_EQ(thread_cnt, config.thread_cnt_);
  if (OB_ISNULL(prev_plan)) {
    ASSERT_FALSE(config.is_prev_plan_submitted());
  } else {
    ASSERT_TRUE(config.is_prev_plan_submitted());
    ASSERT_EQ(0, config.prev_plan_.string().case_compare(prev_plan));
  }

  if (OB_ISNULL(prev_group)) {
    ASSERT_FALSE(config.is_prev_group_submitted());
  } else {
    ASSERT_TRUE(config.is_prev_group_submitted());
    ASSERT_EQ(0, config.prev_group_.string().case_compare(prev_group));
  }
  ASSERT_EQ(OB_SUCCESS, config.encode_to_string(output_job_config));
  LOG_INFO("succeed to encode config", K(input_job_config), K(config), K(output_job_config));
  ASSERT_EQ(0, output_job_config.string().case_compare(input_job_config));
}

TEST_F(TestDailyMaintenanceWindow, basic_test)
{
  int ret = OB_SUCCESS;
  test_config_parse_and_encode("{\"version\":1, \"thread_cnt\":100, \"prev_plan\":\"MyPlan\", \"prev_group\":\"MyGroup\"}", 1, 100, "MyPlan", "MyGroup");
  test_config_parse_and_encode("{\"version\":1, \"thread_cnt\":10, \"prev_plan\":\"MyPlan\"}", 1, 10, "MyPlan", nullptr);
  test_config_parse_and_encode("{\"version\":1, \"thread_cnt\":10, \"prev_group\":\"MyGroup\"}", 1, 10, nullptr, "MyGroup");
  test_config_parse_and_encode("{\"version\":1, \"thread_cnt\":10}", 1, 10,nullptr, nullptr);
  test_config_parse_and_encode("{\"version\":1, \"thread_cnt\":0, \"prev_plan\":\"\", \"prev_group\":\"\"}", 1, 0,"", "");
}

TEST_F(TestDailyMaintenanceWindow, test_empty_string)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDailyWindowJobConfig config(allocator);
  ObString empty_str1;
  ObString empty_str2("");
  ASSERT_EQ(OB_SUCCESS, config.set_prev_plan(empty_str1));
  ASSERT_EQ(OB_SUCCESS, config.set_prev_group(empty_str2));
  ASSERT_FALSE(config.is_prev_plan_submitted());
  ASSERT_TRUE(config.is_prev_group_submitted());
  ObSqlString output_job_config;
  ASSERT_EQ(OB_SUCCESS, config.encode_to_string(output_job_config));
  ASSERT_EQ(0, output_job_config.string().case_compare("{\"version\":1, \"thread_cnt\":0, \"prev_group\":\"\"}"));
  LOG_INFO("succeed to encode config with empty string", K(config), K(output_job_config));
}

TEST_F(TestDailyMaintenanceWindow, test_reset_prev_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDailyWindowJobConfig config(allocator);
  ObString prev_plan("MyPlan");
  ObString prev_group("MyGroup");
  ASSERT_EQ(OB_SUCCESS, config.set_prev_plan(prev_plan));
  ASSERT_EQ(OB_SUCCESS, config.set_prev_group(prev_group));
  ASSERT_TRUE(config.is_prev_plan_submitted());
  ASSERT_TRUE(config.is_prev_group_submitted());
  config.thread_cnt_ = 100;
  ObSqlString output_job_config;
  config.reset_prev_info();
  ASSERT_EQ(OB_SUCCESS, config.encode_to_string(output_job_config));
  ASSERT_EQ(0, output_job_config.string().case_compare("{\"version\":1, \"thread_cnt\":100}"));
  LOG_INFO("succeed to encode config with reset except thread cnt", K(config), K(output_job_config));
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_daily_maintenance_window.log*");
  OB_LOGGER.set_file_name("test_daily_maintenance_window.log", true);
  OB_LOGGER.set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
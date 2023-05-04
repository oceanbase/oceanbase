/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <pthread.h>

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h" // Print*
#include "logservice/palf/log_define.h"
#include "share/ob_throttling_utils.h"


namespace oceanbase
{
using namespace share;
using namespace common;
namespace unittest
{

class TestThrottlingUtils : public ::testing::Test
{
  public:
    TestThrottlingUtils();
    virtual ~TestThrottlingUtils();
    virtual void test_get_throttling_interval_size(
        const int64_t chunk_size, const int64_t request_size,
        const int64_t trigger_limit, const int64_t stop_limit,
        const int64_t overused_size, const double decay_factor);
    virtual void test_get_throttling_interval_percentage(
        const int64_t chunk_size, const int64_t request_size,
        const int64_t trigger_limit, const int64_t stop_limit,
        const int64_t overused_percentage, const double decay_factor);
    virtual void test_get_throttling_interval(
        const int64_t chunk_size, const int64_t request_size,
        const int64_t trigger_limit, const int64_t stop_limit,
        const double decay_factor);
    virtual void SetUp();
    virtual void TearDown();
  protected:
};

TestThrottlingUtils::TestThrottlingUtils()  {}

TestThrottlingUtils::~TestThrottlingUtils()
{
}

void TestThrottlingUtils::test_get_throttling_interval_size(
    const int64_t chunk_size, const int64_t request_size,
    const int64_t trigger_limit, const int64_t stop_limit,
    const int64_t overused_size, const double decay_factor)
{
  int ret = OB_SUCCESS;
  const int64_t GB = 1024 * 1024 * 1024L;
  const int64_t MB = 1024 * 1024L;
  const int64_t cur_hold = trigger_limit + overused_size;
  int64_t interval_us = 0;
  ASSERT_EQ(OB_SUCCESS, ObThrottlingUtils::get_throttling_interval(chunk_size, request_size, trigger_limit,
  cur_hold, decay_factor, interval_us));
  LOG_INFO("get_interval", K(interval_us), K(request_size), "overused_size(MB)", overused_size/MB, "cur_hold(GB)", cur_hold /GB, "trigger_limit(GB)",
  trigger_limit / GB, "stop_limit", stop_limit/GB, K(interval_us));
}

void TestThrottlingUtils::test_get_throttling_interval_percentage(
    const int64_t chunk_size, const int64_t request_size,
    const int64_t trigger_limit, const int64_t stop_limit,
    const int64_t overused_percentage, const double decay_factor)
{
  int ret = OB_SUCCESS;
  const int64_t GB = 1024 * 1024 * 1024L;
  const int64_t MB = 1024 * 1024L;
  const int64_t overused_size = (stop_limit-trigger_limit) * overused_percentage / 100;
  const int64_t cur_hold = trigger_limit + overused_size;
  int64_t interval_us = 0;
  ASSERT_EQ(OB_SUCCESS, ObThrottlingUtils::get_throttling_interval(chunk_size, request_size, trigger_limit, cur_hold, decay_factor, interval_us));
  LOG_INFO("get_interval", K(interval_us), K(request_size), K(overused_percentage), "cur_hold(GB)", cur_hold / GB, "trigger_limit(GB)",
  trigger_limit / GB, "stop_limit", stop_limit/GB);
}

void TestThrottlingUtils::test_get_throttling_interval(
    const int64_t chunk_size, const int64_t request_size,
    const int64_t trigger_limit, const int64_t stop_limit,
    const double decay_factor)
{
  const int64_t GB = 1024 * 1024 * 1024L;
  const int64_t MB = 1024 * 1024L;
  LOG_INFO("BEGIN TEST big round", K(chunk_size), K(request_size), K(trigger_limit), K(stop_limit), K(decay_factor));
  int64_t interval_us = 0;
  int64_t overused_size = 1 * MB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);

  overused_size = 10 * MB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);

  overused_size = 100 * MB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);

  overused_size = 200 * MB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);

  overused_size = 300 * MB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);

  overused_size = 400 * MB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);

  overused_size = 500 * MB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);

  overused_size = 1 * GB;
  test_get_throttling_interval_size(chunk_size, request_size, trigger_limit, stop_limit, overused_size, decay_factor);


  int64_t overused_percentage = 1;
  test_get_throttling_interval_percentage(chunk_size, request_size, trigger_limit, stop_limit, overused_percentage, decay_factor);

  overused_percentage = 5;
  test_get_throttling_interval_percentage(chunk_size, request_size, trigger_limit, stop_limit, overused_percentage, decay_factor);

  for (overused_percentage = 10; overused_percentage < 100; overused_percentage += 10) {
    test_get_throttling_interval_percentage(chunk_size, request_size, trigger_limit,
                                      stop_limit, overused_percentage,
                                      decay_factor);
  }
  LOG_INFO("END TEST big round", K(chunk_size), K(request_size), K(trigger_limit), K(stop_limit), K(decay_factor));
}

void TestThrottlingUtils::SetUp()
{
}

void TestThrottlingUtils::TearDown()
{
  LOG_INFO("TestThrottlingUtils has TearDown");
  //ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
}

TEST_F(TestThrottlingUtils, test_calc_decay_factor)
{
  const int64_t GB = 1024 * 1024 * 1024L;
  const int64_t MB = 1024 * 1024L;
  int64_t available_size = 0;
  int64_t duration_us = 0;
  int64_t chunk_size = 0;
  double decay_factor = 0.0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,  ObThrottlingUtils::calc_decay_factor(available_size, duration_us, chunk_size, decay_factor));
  available_size = 1 * GB;
  ASSERT_EQ(OB_INVALID_ARGUMENT,  ObThrottlingUtils::calc_decay_factor(available_size, duration_us, chunk_size, decay_factor));
  duration_us = 1800 * 1000 * 1000L;
  ASSERT_EQ(OB_INVALID_ARGUMENT,  ObThrottlingUtils::calc_decay_factor(available_size, duration_us, chunk_size, decay_factor));
  chunk_size = palf::MAX_LOG_BUFFER_SIZE;
  ASSERT_EQ(OB_SUCCESS,  ObThrottlingUtils::calc_decay_factor(available_size, duration_us, chunk_size, decay_factor));
  //0.10934
  LOG_INFO("calc_decay_factor", K(chunk_size), K(duration_us), K(available_size), K(decay_factor));
  ASSERT_EQ(10933, static_cast<int64_t>(decay_factor * 100000.0));
  available_size = 10 * GB;

  ASSERT_EQ(OB_SUCCESS,  ObThrottlingUtils::calc_decay_factor(available_size, duration_us, chunk_size, decay_factor));
  LOG_INFO("calc_decay_factor", K(chunk_size), K(duration_us), K(available_size), K(decay_factor));
  ASSERT_EQ(109728, static_cast<int64_t>(decay_factor * 10000000000.0));
  available_size = 40 * GB;
  ASSERT_EQ(OB_SUCCESS,  ObThrottlingUtils::calc_decay_factor(available_size, duration_us, chunk_size, decay_factor));
  LOG_INFO("calc_decay_factor", K(chunk_size), K(duration_us), K(available_size), K(decay_factor));
  ASSERT_EQ(42875, static_cast<int64_t>(decay_factor * 1000000000000.0));
}

TEST_F(TestThrottlingUtils, test_get_throttling_interval_basic)
{
  const int64_t KB = 1024L;
  const int64_t MB = 1024 * 1024L;
  const int64_t GB = 1024 * 1024 * 1024L;
  int64_t chunk_size = 0;
  int64_t request_size = 0;
  int64_t trigger_limit = 0;
  int64_t cur_hold = 0;
  double decay_factor = 0.0;

  int64_t interval_us = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObThrottlingUtils::get_throttling_interval(chunk_size, request_size,  trigger_limit, cur_hold, decay_factor, interval_us));

  chunk_size = palf::MAX_LOG_BUFFER_SIZE;
  request_size = 1 * MB;
  trigger_limit = 50 * GB;
  int64_t stop_limit = 95 * GB;
  cur_hold = 51 * GB;

  int64_t available_size = 40 * GB;
  int64_t duration_us = 1800 * 1000 * 1000L;
  ASSERT_EQ(OB_SUCCESS, ObThrottlingUtils::calc_decay_factor(available_size, duration_us, chunk_size, decay_factor));
  request_size = 1 * KB;
  LOG_INFO("TEST REQUEST_SIZE", K(request_size));
  test_get_throttling_interval(chunk_size, request_size, trigger_limit, stop_limit, decay_factor);

  request_size = 10 * KB;
  LOG_INFO("TEST REQUEST_SIZE", K(request_size));
  test_get_throttling_interval(chunk_size, request_size, trigger_limit, stop_limit, decay_factor);

  request_size = 100 * KB;
  LOG_INFO("TEST REQUEST_SIZE", K(request_size));
  test_get_throttling_interval(chunk_size, request_size, trigger_limit, stop_limit, decay_factor);

  request_size = 1 * MB;
  LOG_INFO("TEST REQUEST_SIZE", K(request_size));
  test_get_throttling_interval(chunk_size, request_size, trigger_limit, stop_limit, decay_factor);

  request_size = palf::MAX_LOG_BUFFER_SIZE;
  LOG_INFO("TEST REQUEST_SIZE", K(request_size));
  test_get_throttling_interval(chunk_size, request_size, trigger_limit, stop_limit, decay_factor);

}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf ./test_throttling_utils.log*");
  OB_LOGGER.set_file_name("test_throttling_utils.log", true);
  OB_LOGGER.set_log_level("INFO");
  LOG_INFO("begin unittest::test_throttling_utils");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2023 OceanBase
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
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "share/table/ob_ttl_util.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

class TestTTLDeleteRateThrottler : public ::testing::Test
{
public:
  TestTTLDeleteRateThrottler();
  virtual ~TestTTLDeleteRateThrottler();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTTLDeleteRateThrottler);
};

TestTTLDeleteRateThrottler::TestTTLDeleteRateThrottler()
{
}

TestTTLDeleteRateThrottler::~TestTTLDeleteRateThrottler()
{
}

void TestTTLDeleteRateThrottler::SetUp()
{
}

void TestTTLDeleteRateThrottler::TearDown()
{
}

// 测试构造函数和初始状态
TEST_F(TestTTLDeleteRateThrottler, test_constructor)
{
  ObTTLDeleteRateThrottler throttler;
  ASSERT_FALSE(throttler.is_inited_);
  ASSERT_EQ(0, throttler.max_rows_per_second_);
  ASSERT_EQ(0.0, throttler.smooth_factor_);
  ASSERT_EQ(0, throttler.trans_start_time_);
  ASSERT_EQ(0, throttler.prev_delete_time_);
  ASSERT_EQ(0, throttler.expected_interval_us_);
}

// 测试 reset 方法
TEST_F(TestTTLDeleteRateThrottler, test_reset)
{
  ObTTLDeleteRateThrottler throttler;
  // 手动设置一些值来模拟初始化后的状态
  throttler.is_inited_ = true;
  throttler.max_rows_per_second_ = 1000;
  throttler.smooth_factor_ = 0.8;
  throttler.trans_start_time_ = ObTimeUtility::current_time();
  throttler.prev_delete_time_ = ObTimeUtility::current_time();
  throttler.expected_interval_us_ = 1000;

  throttler.reset();

  ASSERT_FALSE(throttler.is_inited_);
  ASSERT_EQ(0, throttler.max_rows_per_second_);
  ASSERT_EQ(0.0, throttler.smooth_factor_);
  ASSERT_EQ(0, throttler.trans_start_time_);
  ASSERT_EQ(0, throttler.prev_delete_time_);
  ASSERT_EQ(0, throttler.expected_interval_us_);
}

// 测试 calculate_wait_time 方法 - 不需要等待的情况
TEST_F(TestTTLDeleteRateThrottler, test_calculate_wait_time_no_wait)
{
  ObTTLDeleteRateThrottler throttler;
  int64_t cur_time = ObTimeUtility::current_time();
  throttler.prev_delete_time_ = cur_time - 2000; // 2ms 前
  throttler.expected_interval_us_ = 1000; // 期望间隔 1ms

  int64_t wait_time = throttler.calculate_wait_time();
  ASSERT_EQ(0, wait_time); // 已经超过期望间隔，不需要等待
}

// 测试 calculate_wait_time 方法 - 需要等待的情况
TEST_F(TestTTLDeleteRateThrottler, test_calculate_wait_time_need_wait)
{
  ObTTLDeleteRateThrottler throttler;
  int64_t cur_time = ObTimeUtility::current_time();
  throttler.prev_delete_time_ = cur_time - 500; // 0.5ms 前
  throttler.expected_interval_us_ = 1000; // 期望间隔 1ms
  throttler.smooth_factor_ = 0.8;

  int64_t wait_time = throttler.calculate_wait_time();
  // 期望等待时间 = (1000 - 500) * 0.8 = 400 微秒
  // 由于时间可能有微小误差，允许一定范围
  ASSERT_GE(wait_time, 350);
  ASSERT_LE(wait_time, 450);
}

// 测试 need_commit_trans_early 方法 - 不需要提前提交
TEST_F(TestTTLDeleteRateThrottler, test_need_commit_trans_early_false)
{
  ObTTLDeleteRateThrottler throttler;
  int64_t cur_time = ObTimeUtility::current_time();
  throttler.trans_start_time_ = cur_time - 1000000; // 1秒前开始
  int64_t trans_timeout_ts = cur_time + 9000000; // 还有9秒超时，总共10秒超时

  bool need_early = throttler.need_commit_trans_early(trans_timeout_ts);
  ASSERT_FALSE(need_early); // 使用时间 < 总超时时间 * 0.8，不需要提前提交
}

// 测试 need_commit_trans_early 方法 - 需要提前提交
TEST_F(TestTTLDeleteRateThrottler, test_need_commit_trans_early_true)
{
  ObTTLDeleteRateThrottler throttler;
  int64_t cur_time = ObTimeUtility::current_time();
  throttler.trans_start_time_ = cur_time - 9000000; // 9秒前开始
  int64_t trans_timeout_ts = cur_time + 1000000; // 还有1秒超时，总共10秒超时

  bool need_early = throttler.need_commit_trans_early(trans_timeout_ts);
  ASSERT_TRUE(need_early); // 使用时间 > 总超时时间 * 0.8，需要提前提交
}

// 测试 check_and_try_throttle - 未初始化的情况
TEST_F(TestTTLDeleteRateThrottler, test_check_and_try_throttle_not_inited)
{
  ObTTLDeleteRateThrottler throttler;
  throttler.is_inited_ = false;
  int ret = throttler.check_and_try_throttle(ObTimeUtility::current_time() + 10000000);
  // 未初始化时应该返回 OB_NOT_INIT
  ASSERT_EQ(OB_NOT_INIT, ret);
}

// 测试 check_and_try_throttle - max_rows_per_second 为 0 的情况
TEST_F(TestTTLDeleteRateThrottler, test_check_and_try_throttle_zero_rate)
{
  ObTTLDeleteRateThrottler throttler;
  throttler.is_inited_ = true;
  throttler.max_rows_per_second_ = 0;

  int ret = throttler.check_and_try_throttle(ObTimeUtility::current_time() + 10000000);
  ASSERT_EQ(OB_SUCCESS, ret); // 速率为0时，不进行限速，直接返回成功
}

// 测试 check_and_try_throttle - 正常限速情况
TEST_F(TestTTLDeleteRateThrottler, test_check_and_try_throttle_normal)
{
  ObTTLDeleteRateThrottler throttler;
  // 手动设置成员变量来模拟初始化后的状态
  throttler.is_inited_ = true;
  throttler.max_rows_per_second_ = 1000; // 每秒1000行
  throttler.expected_interval_us_ = 1000; // 期望间隔1ms
  throttler.smooth_factor_ = 0.8;
  throttler.trans_start_time_ = ObTimeUtility::current_time();
  throttler.prev_delete_time_ = ObTimeUtility::current_time() - 500; // 0.5ms前

  int64_t trans_timeout_ts = ObTimeUtility::current_time() + 10000000; // 10秒后超时
  int ret = throttler.check_and_try_throttle(trans_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 验证 prev_delete_time_ 已更新
  ASSERT_GT(throttler.prev_delete_time_, throttler.trans_start_time_);
}

// 测试 check_and_try_throttle - 需要提前提交事务的情况
TEST_F(TestTTLDeleteRateThrottler, test_check_and_try_throttle_early_commit)
{
  ObTTLDeleteRateThrottler throttler;
  throttler.is_inited_ = true;
  throttler.max_rows_per_second_ = 1000;
  throttler.trans_start_time_ = ObTimeUtility::current_time() - 9000000; // 9秒前开始
  throttler.prev_delete_time_ = throttler.trans_start_time_;

  int64_t trans_timeout_ts = ObTimeUtility::current_time() + 1000000; // 还有1秒超时
  int ret = throttler.check_and_try_throttle(trans_timeout_ts);
  ASSERT_EQ(OB_ITER_END, ret); // 需要提前提交，返回 OB_ITER_END
}

// 测试完整的限速流程 - 连续多次调用
TEST_F(TestTTLDeleteRateThrottler, test_rate_limiting_flow)
{
  ObTTLDeleteRateThrottler throttler;
  // 手动初始化状态，模拟 init 后的状态
  throttler.is_inited_ = true;
  throttler.max_rows_per_second_ = 10000; // 每秒10000行
  throttler.expected_interval_us_ = 100; // 期望间隔100微秒
  throttler.smooth_factor_ = 0.8;
  throttler.trans_start_time_ = ObTimeUtility::current_time();
  throttler.prev_delete_time_ = throttler.trans_start_time_;

  int64_t trans_timeout_ts = ObTimeUtility::current_time() + 10000000; // 10秒后超时

  // 第一次调用，应该不需要等待（或等待时间很短）
  int ret1 = throttler.check_and_try_throttle(trans_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret1);
  int64_t first_delete_time = throttler.prev_delete_time_;

  // 立即第二次调用，应该需要等待
  int ret2 = throttler.check_and_try_throttle(trans_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret2);
  int64_t second_delete_time = throttler.prev_delete_time_;

  // 验证两次删除时间之间有间隔
  ASSERT_GE(second_delete_time - first_delete_time, 0);
}

// 测试边界情况 - expected_interval_us_ 为 0
TEST_F(TestTTLDeleteRateThrottler, test_edge_case_zero_interval)
{
  ObTTLDeleteRateThrottler throttler;
  throttler.max_rows_per_second_ = 1000;
  throttler.expected_interval_us_ = 0; // 边界情况
  throttler.smooth_factor_ = 0.8;
  throttler.trans_start_time_ = ObTimeUtility::current_time();
  throttler.prev_delete_time_ = ObTimeUtility::current_time();

  int64_t wait_time = throttler.calculate_wait_time();
  // 如果 expected_interval_us_ 为 0，elapsed 总是 >= 0，所以返回 0
  ASSERT_EQ(0, wait_time);
}

// 测试边界情况 - 时间刚好等于期望间隔
TEST_F(TestTTLDeleteRateThrottler, test_edge_case_exact_interval)
{
  ObTTLDeleteRateThrottler throttler;
  int64_t cur_time = ObTimeUtility::current_time();
  throttler.prev_delete_time_ = cur_time - 1000; // 刚好1ms前
  throttler.expected_interval_us_ = 1000; // 期望间隔1ms
  throttler.smooth_factor_ = 0.8;

  int64_t wait_time = throttler.calculate_wait_time();
  // elapsed >= expected_interval_us_，应该返回 0
  ASSERT_EQ(0, wait_time);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ttl_del_rate_limit.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

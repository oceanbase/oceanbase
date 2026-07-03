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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_vector_index_async_task_util.h"
#undef private
#undef protected

using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

// THREAD_FACTOR == 0.5: thread count = floor(cpu * 0.5) clamped to [2, 64].

// ---------------------------------------------------------------------------
// ADAPT-001: Small tenant (cpu=2) -> floor(2*0.5)=1, clamped to min 2
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, ADAPT_001_small_tenant_cpu_2)
{
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(2));
}

// ---------------------------------------------------------------------------
// ADAPT-002: Medium tenant (cpu=5) -> floor(5*0.5)=2
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, ADAPT_002_medium_tenant_cpu_5)
{
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(5));
}

// ---------------------------------------------------------------------------
// ADAPT-003: Large tenant (cpu=20) -> floor(20*0.5)=10
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, ADAPT_003_large_tenant_cpu_20)
{
  ASSERT_EQ(10, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(20));
}

// ---------------------------------------------------------------------------
// ADAPT-004: CPU scale up 2 -> 8 (floor(8*0.5)=4)
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, ADAPT_004_scale_up_2_to_8)
{
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(2));
  ASSERT_EQ(4, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(8));
}

// ---------------------------------------------------------------------------
// ADAPT-005: CPU scale down 8 -> 3 (floor(8*0.5)=4, floor(3*0.5)=1 -> min 2)
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, ADAPT_005_scale_down_8_to_3)
{
  ASSERT_EQ(4, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(8));
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(3));
}

// ---------------------------------------------------------------------------
// ADAPT-006: Same CPU yields same result (idempotency / no-op property)
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, ADAPT_006_idempotency)
{
  const int64_t r1 = ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(5);
  const int64_t r2 = ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(5);
  ASSERT_EQ(r1, r2);
  ASSERT_EQ(2, r1);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, edge_cpu_0)
{
  // floor(0*0.5)=0, clamped to min 2
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(0));
}

TEST(TestCalcMaxThreadCountByCpu, edge_cpu_1)
{
  // floor(1*0.5)=0, clamped to min 2
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(1));
}

TEST(TestCalcMaxThreadCountByCpu, edge_cpu_3)
{
  // floor(3*0.5)=1, clamped to min 2
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(3));
}

TEST(TestCalcMaxThreadCountByCpu, edge_cpu_4)
{
  // floor(4*0.5)=2
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(4));
}

TEST(TestCalcMaxThreadCountByCpu, edge_cpu_15)
{
  // floor(15*0.5)=7
  ASSERT_EQ(7, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(15));
}

TEST(TestCalcMaxThreadCountByCpu, edge_cpu_14)
{
  // floor(14*0.5)=7
  ASSERT_EQ(7, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(14));
}

TEST(TestCalcMaxThreadCountByCpu, edge_cpu_100)
{
  // floor(100*0.5)=50, below max 64
  ASSERT_EQ(50, ObVecIndexAsyncTaskHandler::calc_max_thread_count_by_cpu(100));
}

// ---------------------------------------------------------------------------
// Constants sanity check
// ---------------------------------------------------------------------------
TEST(TestCalcMaxThreadCountByCpu, constants)
{
  ASSERT_EQ(2, ObVecIndexAsyncTaskHandler::MIN_THREAD_COUNT);
  ASSERT_EQ(64, ObVecIndexAsyncTaskHandler::MAX_THREAD_COUNT);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

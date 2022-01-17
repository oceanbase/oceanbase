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

#include <gtest/gtest.h>
#include <random>
#include <iostream>
#include <stdint.h>
#include "lib/coro/co.h"
#include "lib/ob_errno.h"
#include "lib/random/ob_random.h"
#include "lib/lock/ob_latch.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread/thread_pool.h"
#include "testing.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

static const int64_t CO_CNT = 10;
static const int64_t T_CNT = 4;
static const int64_t ROUND = 10000;
static const int64_t RATIO = 4;

void workload()
{
  int64_t loops = ObRandom::rand(100, 200);
  for (int i = 0; i < loops; i++) {
    ObRandom::rand(1, 1000);
  }
}

void rand_yield()
{
  workload();
  if (ObRandom::rand(1, 1000) < 50) {
    CO_YIELD();
  }
}

struct CoLatchTester {
  CoLatchTester() : latch_(), sum_(0)
  {}
  ObLatch latch_;
  int64_t sum_;
};

TEST(TestCoLatch, wrlock)
{
  CoLatchTester tester;

  cotesting::FlexPool{[&] {
                        const auto cidx = static_cast<uint32_t>(CO_CURRENT().get_cidx());
                        // const auto tidx = static_cast<uint32_t>(CO_CURRENT().get_tidx());
                        for (int64_t i = 0; i < ROUND; i++) {
                          if (0 == i % 4) {
                            tester.latch_.wrlock(1, INT64_MAX, &cidx);
                            rand_yield();
                            tester.sum_++;
                            // std::cout << "thread: " << tidx
                            // << " cid: " << cidx
                            // << " sum: " << tester.sum_
                            // << std::endl;
                            tester.latch_.unlock(&cidx);
                          } else {
                            tester.latch_.rdlock(1);
                            rand_yield();
                            tester.latch_.unlock(&cidx);
                          }
                        }
                      },
      T_CNT,
      CO_CNT}
      .start();
  ASSERT_EQ(CO_CNT * T_CNT * ROUND / RATIO, tester.sum_);
}

CoLatchTester g_tester;

class LatchStress : public ThreadPool {
public:
  LatchStress()
  {}
  virtual ~LatchStress()
  {}
  void run1() final
  {
    for (int64_t i = 0; i < ROUND; i++) {
      if (0 == i % 4) {
        g_tester.latch_.wrlock(1);
        workload();
        g_tester.sum_++;
        g_tester.latch_.unlock();
      } else {
        g_tester.latch_.rdlock(1);
        workload();
        g_tester.latch_.unlock();
      }
    }
  }
};

TEST(TestCoLatch, wrlock_mix)
{
  const int64_t thread_cnt = 4;

  LatchStress stress;
  stress.set_thread_count(thread_cnt);
  stress.start();

  cotesting::FlexPool{[&] {
                        const auto cidx = static_cast<uint32_t>(CO_CURRENT().get_cidx());
                        for (int64_t i = 0; i < ROUND; i++) {
                          if (0 == i % 4) {
                            g_tester.latch_.wrlock(1, INT64_MAX, &cidx);
                            rand_yield();
                            g_tester.sum_++;
                            // std::cout << "thread: " << tidx
                            // << " cid: " << cidx
                            // << " sum: " << tester.sum_
                            // << std::endl;
                            g_tester.latch_.unlock(&cidx);
                          } else {
                            g_tester.latch_.rdlock(1);
                            rand_yield();
                            g_tester.latch_.unlock(&cidx);
                          }
                        }
                      },
      T_CNT,
      CO_CNT}
      .start();

  stress.wait();

  ASSERT_EQ((CO_CNT * T_CNT + thread_cnt) * ROUND / RATIO, g_tester.sum_);
}

TEST(TestCoLatch, timeout)
{

  TIME_LESS(2000 * 1000L, [] {
    ObLatch latch;
    EXPECT_EQ(OB_SUCCESS, latch.wrlock(1));
    cotesting::FlexPool(
        [&] {
          // const auto cid = static_cast<uint32_t>(CO_CURRENT().get_cidx());
          EXPECT_EQ(OB_TIMEOUT, latch.wrlock(1, 1000L * 1000L));
        },
        1,
        10)
        .start();
    EXPECT_EQ(OB_SUCCESS, latch.unlock());
  });
}

ObLatch g_latch;

class LatchTimeout : public ThreadPool {
public:
  LatchTimeout()
  {}
  virtual ~LatchTimeout()
  {}
  void run1() final
  {
    int64_t abs_timeout = ObTimeUtility::current_time() + 2000;
    EXPECT_EQ(OB_TIMEOUT, g_latch.wrlock(1, abs_timeout));
  }
};

TEST(TestCoLatch, thread_timeout)
{
  g_latch.wrlock(1);
  LatchTimeout t;
  t.set_thread_count(10);
  t.start();
  usleep(10L * 1000L);
  t.wait();
  EXPECT_EQ(OB_SUCCESS, g_latch.unlock());
}

int main(int argc, char* argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

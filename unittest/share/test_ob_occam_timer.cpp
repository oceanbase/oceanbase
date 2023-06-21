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

#define UNITTEST_DEBUG

#include "share/ob_occam_timer.h"
#include <gtest/gtest.h>
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "common/ob_clock_generator.h"
#include "share/ob_occam_time_guard.h"

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;

class TestObOccamTimer: public ::testing::Test
{
public:
  TestObOccamTimer() {};
  virtual ~TestObOccamTimer() {};
  virtual void SetUp() {
    thread_pool = new ObOccamThreadPool;
    thread_pool->init_and_start(8);
    occam_timer = new ObOccamTimer();
    occam_timer->init_and_start(*thread_pool, 1000, "Timer");
  };
  virtual void TearDown() {
    delete thread_pool;
    delete occam_timer;
    ASSERT_EQ(occam::DefaultAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(function::DefaultFunctionAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(future::DefaultFutureAllocator::get_default_allocator().total_alive_num, 0);
    ASSERT_EQ(guard::DefaultSharedGuardAllocator::get_default_allocator().total_alive_num, 0);
  };
  ObOccamThreadPool *thread_pool;
  ObOccamTimer *occam_timer;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObOccamTimer);
};

void test(int &a) {
  ATOMIC_STORE(&a, 2);
}

TEST_F(TestObOccamTimer, normal) {
  int ret = OB_SUCCESS;
  int a = 1;
  
  ObOccamTimerTaskRAIIHandle task_desc;
  ret = occam_timer->schedule_task_after(task_desc, 1_s, [&a](){ test(a); return false; });
  
  this_thread::sleep_for(chrono::milliseconds(1200));
  ASSERT_EQ(ret, OB_SUCCESS);
  task_desc.stop_and_wait();
  task_desc.stop_and_wait();
  ASSERT_EQ(ATOMIC_LOAD(&a), 2);
}

TEST_F(TestObOccamTimer, run_2_task_parallel) {
  int ret = OB_SUCCESS;
  int64_t a = 1;
  vector<ObOccamTimerTaskRAIIHandle> v_task_desc(8);
  for (int i = 0 ; i < 8; ++i) {
    ret = occam_timer->schedule_task_after(v_task_desc[i],
                                          1_ms,
                                          [&a](){
                                            this_thread::sleep_for(chrono::milliseconds(100));
                                            ATOMIC_INC(&a);
                                            OB_LOG(INFO, "inc done", K(ObClockGenerator::getRealClock()));
                                            return false;
                                          });
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  OB_LOG(INFO, "commit done", K(ObClockGenerator::getRealClock()));
  this_thread::sleep_for(chrono::milliseconds(200));
  OB_LOG(INFO, "read a", K(ObClockGenerator::getRealClock()));
  ASSERT_EQ(ATOMIC_LOAD(&a), 9);
  for (auto &x : v_task_desc)
    x.stop_and_wait();
}

TEST_F(TestObOccamTimer, cancel_task) {
  int ret = OB_SUCCESS;
  int64_t a = 1;
  {
    ObUniqueGuard<ObOccamTimerTaskRAIIHandle> handle;
    ob_make_unique<ObOccamTimerTaskRAIIHandle>(handle);
    occam_timer->schedule_task_repeat(*handle,
                                    10_ms,
                                    [&a](){ this_thread::sleep_for(chrono::milliseconds(100)); ++a; return false; });
  }
  this_thread::sleep_for(chrono::milliseconds(150));
  ASSERT_EQ(ATOMIC_LOAD(&a), 1);
}

TEST_F(TestObOccamTimer, run_repeat_task) {
  int ret = OB_SUCCESS;
  int64_t a = 1;
  ObUniqueGuard<ObOccamTimerTaskRAIIHandle> handle;
  ob_make_unique<ObOccamTimerTaskRAIIHandle>(handle);
  occam_timer->schedule_task_repeat(*handle,
                                   100_ms,
                                   [&a](){ ++a; return false; });
  this_thread::sleep_for(chrono::milliseconds(1050));
  ASSERT_EQ(ATOMIC_LOAD(&a), 11);
}

TEST_F(TestObOccamTimer, run_repeat_task_conflict) {
  int ret = OB_SUCCESS;
  int64_t a = 1;
  ObUniqueGuard<ObOccamTimerTaskRAIIHandle> handle;
  ob_make_unique<ObOccamTimerTaskRAIIHandle>(handle);
  occam_timer->schedule_task_repeat(*handle,
                                   10_ms,
                                   [&a](){ this_thread::sleep_for(chrono::milliseconds(150)); ++a; return false; });
  this_thread::sleep_for(chrono::milliseconds(205));
  ASSERT_EQ(ATOMIC_LOAD(&a), 2);
}

TEST_F(TestObOccamTimer, stop_task_wait) {
  int ret = OB_SUCCESS;
  int64_t a = 1;
  ObUniqueGuard<ObOccamTimerTaskRAIIHandle> handle;
  ob_make_unique<ObOccamTimerTaskRAIIHandle>(handle);
  OB_LOG(INFO, "before commit task", K(ObClockGenerator::getRealClock()));
  ret = occam_timer->schedule_task_repeat(*handle,
                                         100_ms,
                                         [&a](){ 
                                           OB_LOG(INFO, "execute task", K(ObClockGenerator::getRealClock()));
                                           this_thread::sleep_for(chrono::milliseconds(500)); ++a;
                                           return false;
                                          });
  ASSERT_EQ(ret, OB_SUCCESS);
  this_thread::sleep_for(chrono::milliseconds(200));
  int64_t time1 = ObClockGenerator::getRealClock();
  OB_LOG(INFO, "before wait", K(ObClockGenerator::getRealClock()));
  handle->stop_and_wait();
  OB_LOG(INFO, "after wait", K(ObClockGenerator::getRealClock()));
  int64_t time2 = ObClockGenerator::getRealClock();
  cout << time1 << "," << time2 << ",diff:" << time2 - time1 << " a:" << a << endl;
  ASSERT_EQ(time2 - time1 > 300_ms, true);
  ASSERT_EQ(time2 - time1 < 500_ms, true);
  ASSERT_EQ(a, 2);
}

TEST_F(TestObOccamTimer, schedule_task_ignore_handle) {
  int ret = OB_SUCCESS;
  int64_t a = 1;
  ret = occam_timer->schedule_task_ignore_handle_after(100_ms,
                                                      [&a]() { ++a; return false; });
  ASSERT_EQ(ret, OB_SUCCESS);
  this_thread::sleep_for(chrono::milliseconds(350));
  ASSERT_EQ(a, 2);
}

TEST_F(TestObOccamTimer, schedule_repeat_ignore_handle_destroyed_in_timer) {
  int ret = OB_SUCCESS;
  ObSharedGuard<int> p;
  ob_make_shared<int>(p, 1);
  ObWeakGuard<int> p_w(p);
  ret = occam_timer->schedule_task_ignore_handle_repeat_and_immediately(100_ms,
                                                                        [p]() {
                                                                          static int b = 0;
                                                                          if (++b == 2) {
                                                                            return true;
                                                                          } else {
                                                                            return false;
                                                                          }
                                                                        });// 将在20ms后析构该任务
  ASSERT_EQ(ret, OB_SUCCESS);
  this_thread::sleep_for(chrono::milliseconds(250));
  p.reset();
  ASSERT_EQ(p_w.upgradeable(), false);
}

TEST_F(TestObOccamTimer, hung) {
  int ret = OB_SUCCESS;
  ObSharedGuard<int> p;
  ob_make_shared<int>(p, 1);
  ObWeakGuard<int> p_w(p);
  ObOccamTimerTaskRAIIHandle *handle = new ObOccamTimerTaskRAIIHandle();
  ret = occam_timer->schedule_task_repeat_and_immediately(*handle,
                                                          100_ms,
                                                          [p]() { return false; });
  ASSERT_EQ(ret, OB_SUCCESS);
  this_thread::sleep_for(chrono::milliseconds(300));
  p.reset();
  ASSERT_EQ(p_w.upgradeable(), true);
  delete handle;// or will hung
}

TEST_F(TestObOccamTimer, stop_and_wait) {
  int ret = OB_SUCCESS;
  ret = occam_timer->schedule_task_ignore_handle_repeat(20_s, [](){ return false; });
  ASSERT_EQ(ret, OB_SUCCESS);
  occam_timer->destroy();
}

TEST_F(TestObOccamTimer, stop_repeat_task_inside_destroy_outside) {
  int ret = OB_SUCCESS;
  ObOccamTimerTaskRAIIHandle handle;
  ret = occam_timer->schedule_task_repeat_and_immediately(handle, 20_ms, [](){ return true; });
  ASSERT_EQ(ret, OB_SUCCESS);
  std::this_thread::sleep_for(chrono::milliseconds(50));
  handle.stop_and_wait();
  occam_timer->destroy();
}

TEST_F(TestObOccamTimer, stop_and_destroy_repeat_task_outside) {
  int ret = OB_SUCCESS;
  ObOccamTimerTaskRAIIHandle handle;
  ret = occam_timer->schedule_task_repeat_and_immediately(handle, 20_ms, [](){ return false; });
  ASSERT_EQ(ret, OB_SUCCESS);
  std::this_thread::sleep_for(chrono::milliseconds(50));
  handle.stop_and_wait();
  occam_timer->destroy();
}

TEST_F(TestObOccamTimer, test_specify_first_delay) {
  int ret = OB_SUCCESS;
  ObOccamTimerTaskRAIIHandle handle;
  ret = occam_timer->schedule_task_repeat_spcifiy_first_delay(handle, 20_ms, 100_ms, [](){ OB_LOG(INFO, "run task", K(ObClockGenerator::getRealClock())); return false; });
  ASSERT_EQ(ret, OB_SUCCESS);
  std::this_thread::sleep_for(chrono::milliseconds(505));
  handle.stop_and_wait();
  occam_timer->destroy();
}

void test_func(const char *function_name = __builtin_FUNCTION(), const char *file = __builtin_FILE(), const int64_t line = __builtin_LINE())
{
  OB_LOG(INFO, "print function name", K(function_name), K(file), K(line));
}

void test_func_1()
{
  test_func();
}

TEST_F(TestObOccamTimer, test_function_name) {
  test_func_1();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_occam_timer.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_occam_timer.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


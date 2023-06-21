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
#include "share/ob_occam_time_guard.h"
#include <gtest/gtest.h>
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "common/ob_clock_generator.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;

class TestObOccamTimeGuard: public ::testing::Test
{
public:
  TestObOccamTimeGuard() {};
  virtual ~TestObOccamTimeGuard() {};
  virtual void SetUp() { OB_LOG(DEBUG, "set up", K(ObClockGenerator::getRealClock())); };
  virtual void TearDown() { OB_LOG(DEBUG, "TearDown", K(ObClockGenerator::getRealClock())); };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObOccamTimeGuard);
};

void test1() {
  TIMEGUARD_INIT(OCCAM, 10_ms, 1_s);
  this_thread::sleep_for(chrono::seconds(2));
  // OB_LOG(INFO, "", KTIMERANGE(ObClockGenerator::getRealClock(), HOUR, DAY));// compile error
}

void test0() {
  TIMEGUARD_INIT(OCCAM, 10_ms, 1_s);
  CLICK();
  test1();
  this_thread::sleep_for(chrono::seconds(2));
}

void test2() {
  TIMEGUARD_INIT(10_ms, 1_s);
  this_thread::sleep_for(chrono::seconds(2));
}

TEST_F(TestObOccamTimeGuard, thread_num_overflow) {
  auto just_sleep_1s = []() { TIMEGUARD_INIT(99_s, 100_s); this_thread::sleep_for(chrono::seconds(10)); };
  auto just_sleep_1s_report_timeout_and_hung = []() { TIMEGUARD_INIT(0.5_s, 0.5_s); this_thread::sleep_for(chrono::seconds(2)); };
  auto just_sleep_10s = []() { TIMEGUARD_INIT(99_s, 100_s); this_thread::sleep_for(chrono::seconds(20)); };
  int64_t max_num = occam::ObThreadHungDetector::MAX_THREAD_NUM;
  {
    vector<thread> v_th;
    v_th.emplace_back(just_sleep_1s);// the first thread will release quickly
    for (int64_t idx = 1; idx < max_num - 1; ++idx) {
      v_th.emplace_back(just_sleep_10s); // all middle thread will realese slowly
    }
    v_th.emplace_back(just_sleep_1s);// the last thread will release quickly
    // now all global slot is full
    v_th.emplace_back(just_sleep_1s_report_timeout_and_hung);// here will print WARN LOG, say will not detect thread hung, but will still detect function timeout
    this_thread::sleep_for(chrono::seconds(10)); // wait first slot and last slot realese, now there are two slots free
    v_th.emplace_back(just_sleep_1s_report_timeout_and_hung);// this thread will use first slot, and report function timeout, thread hung
    v_th.emplace_back(just_sleep_1s_report_timeout_and_hung);// this thread will use last slot, and report function timeout, thread hung
    v_th.emplace_back(just_sleep_1s_report_timeout_and_hung);// this thread will print WARN LOG, say will not detect thread hung, but will still detect function timeout
    for (auto &th : v_th) {
      th.join();
    } // now all global slot is release
  }
  thread th(just_sleep_1s_report_timeout_and_hung);// this thread will use self's slot, and report function timeout, thread hung
  th.join();
}

TEST_F(TestObOccamTimeGuard, normal) {
  std::thread t1(test0);
  std::thread t2(test2);
  t1.join();
  t2.join();
}

TEST_F(TestObOccamTimeGuard, normal2) {
  TIMEGUARD_INIT(OCCAM, 1_s);
  this_thread::sleep_for(chrono::seconds(2));
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_occam_time_guard.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  oceanbase::common::ObTscTimestamp::get_instance().init();
  logger.set_file_name("test_ob_occam_time_guard.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
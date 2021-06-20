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
#include "lib/oblog/ob_log.h"
#include "election/ob_election.h"
#include "election/ob_election_rpc.h"
#include "election/ob_election_timer.h"
#include "common/ob_clock_generator.h"
#include "storage/transaction/ob_time_wheel.h"

namespace oceanbase {
namespace unittest {

using namespace obrpc;
using namespace election;
using namespace common;

class TestObElectionTimer : public ::testing::Test {
public:
  TestObElectionTimer()
  {}
  virtual ~TestObElectionTimer()
  {}

  virtual void SetUp();
  virtual void TearDown();

private:
  static const int64_t TIMER_THREAD_COUNT = 6;
  int init();
  int start();
  int stop();
  int wait();

protected:
  election::ObElection e_;
  election::ObElectionTimer timer_;
  election::ObElectionRpc rpc_;
  common::ObTimeWheel tw_;
};

int TestObElectionTimer::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tw_.start())) {
    ELECT_LOG(WARN, "time wheel start error.", K(ret));
  }
  return ret;
}

int TestObElectionTimer::stop()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tw_.stop())) {
    ELECT_LOG(WARN, "time wheel stop error.", K(ret));
  }
  return ret;
}

int TestObElectionTimer::wait()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tw_.wait())) {
    ELECT_LOG(WARN, "time wheel wait error.", K(ret));
  }
  return ret;
}

void TestObElectionTimer::SetUp()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init())) {
    ELECT_LOG(WARN, "election timer init error.", K(ret));
  } else if (OB_FAIL(start())) {
    ELECT_LOG(WARN, "election timer start error.", K(ret));
  }
}

void TestObElectionTimer::TearDown()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stop())) {
    ELECT_LOG(WARN, "election stop error.", K(ret));
  } else if (OB_FAIL(wait())) {
    ELECT_LOG(WARN, "election wait error.", K(ret));
  }
}

int TestObElectionTimer::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tw_.init(1000, TIMER_THREAD_COUNT, "test_election_timer"))) {
    ELECT_LOG(WARN, "time wheel init error.", K(ret));
  } else if (OB_FAIL(timer_.init(&e_, &tw_, &rpc_))) {
    ELECT_LOG(WARN, "timer init error.", K(ret));
  } else {
    ELECT_LOG(INFO, "timer init success.");
  }

  return ret;
}

TEST_F(TestObElectionTimer, timer_init_stop)
{
  int64_t ts = ObClockGenerator::getClock();
  ts = ((ts + T_ELECT2) / T_ELECT2) * T_ELECT2;

  for (int64_t i = 0; i < 100; i++) {
    EXPECT_EQ(OB_SUCCESS, timer_.start(ts));
    EXPECT_EQ(OB_ERR_UNEXPECTED, timer_.start(ts));

    EXPECT_EQ(OB_SUCCESS, timer_.stop());
    EXPECT_EQ(OB_SUCCESS, timer_.stop());

    EXPECT_EQ(OB_SUCCESS, timer_.start(ts));
    EXPECT_EQ(OB_SUCCESS, timer_.try_stop());
    EXPECT_EQ(OB_SUCCESS, timer_.try_stop());
  }
}

TEST_F(TestObElectionTimer, register_gt)
{
  int64_t ts = ObClockGenerator::getClock();
  ts = ((ts + T_ELECT2) / T_ELECT2) * T_ELECT2;
  int64_t delay = 0;

  for (int64_t i = 0; i < 100; i++) {
    EXPECT_EQ(OB_SUCCESS, timer_.start(ts));
    EXPECT_EQ(OB_SUCCESS, timer_.register_gt1_once(ts, delay));
    ELECT_LOG(INFO, "register gt1 once", "run expect time", ts, "run after", delay);

    EXPECT_EQ(OB_SUCCESS, timer_.register_gt2_once(ts, delay));
    ELECT_LOG(INFO, "register gt2 once", "run expect time", ts, "run after", delay);

    EXPECT_EQ(OB_SUCCESS, timer_.register_gt3_once(ts, delay));
    ELECT_LOG(INFO, "register gt3 once", "run expect time", ts, "run after", delay);

    EXPECT_EQ(OB_SUCCESS, timer_.register_gt4_once(ts, delay));
    ELECT_LOG(INFO, "register gt4 once", "run expect time", ts, "run after", delay);

    EXPECT_EQ(OB_SUCCESS, timer_.stop());
  }
}

}  // namespace unittest
}  // namespace oceanbase

using oceanbase::common::OB_SUCCESS;
using oceanbase::common::ObClockGenerator;
using oceanbase::common::ObLogger;
int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_timer.log", OB_LOG_LEVEL_INFO, true);

  if (OB_SUCCESS != (ret = ObClockGenerator::init())) {
    ELECT_LOG(WARN, "clock generator init error.", K(ret));
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }

  oceanbase::election::ASYNC_LOG_DESTROY();
  (void)oceanbase::common::ObClockGenerator::destroy();

  return ret;
}

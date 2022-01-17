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

#include "storage/transaction/ob_trans_result.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace unittest {
class TestObTransResult : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  static void* wait_timeout(void* args);
  static void* wait_normal(void* args);

public:
  static const int64_t WAIT_TIMES_US = 5 * 1000 * 1000;
  static const int64_t VALID_WAIT_TIME = -1;
  static const int64_t SLEEP_TIME_OUT_MS = 6000;
  static const int64_t SLEEP_TIME_COMMON_MS = 1000;
  static const int64_t THREAD_NUM = 10;
};

void* TestObTransResult::wait_timeout(void* args)
{
  int res = OB_SUCCESS;
  ObTransCond* cond = static_cast<ObTransCond*>(args);
  EXPECT_EQ(OB_INVALID_ARGUMENT, cond->wait(TestObTransResult::VALID_WAIT_TIME, res));
  EXPECT_EQ(OB_TIMEOUT, cond->wait(TestObTransResult::WAIT_TIMES_US, res));
  pthread_exit(NULL);
}

void* TestObTransResult::wait_normal(void* args)
{
  int res = OB_SUCCESS;
  ObTransCond* cond = static_cast<ObTransCond*>(args);
  EXPECT_EQ(OB_INVALID_ARGUMENT, cond->wait(TestObTransResult::VALID_WAIT_TIME, res));
  EXPECT_EQ(OB_SUCCESS, cond->wait(TestObTransResult::WAIT_TIMES_US, res));
  EXPECT_EQ(OB_ERR_UNEXPECTED, res);
  pthread_exit(NULL);
}
// wait and awake of threads
TEST_F(TestObTransResult, wait_timeout)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // create 10 wait threads
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  pthread_t tids[THREAD_NUM];

  ObTransCond* cond = new ObTransCond();
  ASSERT_TRUE(NULL != cond);
  // test the case of wait timeout
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_TRUE(0 == pthread_create(&tids[i], &attr, TestObTransResult::wait_timeout, static_cast<void*>(cond)));
  }
  // sleep 6s
  ObClockGenerator::msleep(SLEEP_TIME_OUT_MS);
  int msg_type = OB_TIMEOUT;
  cond->notify(msg_type);

  // replay of threads
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_join(tids[i], NULL));
  }
  //--------------------------------------------------------------
  cond->reset();
  // test the case of awake
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_TRUE(0 == pthread_create(&tids[i], &attr, TestObTransResult::wait_normal, static_cast<void*>(cond)));
  }
  // sleep 1s
  ObClockGenerator::msleep(SLEEP_TIME_COMMON_MS);
  msg_type = OB_ERR_UNEXPECTED;
  cond->notify(msg_type);

  // replay of threads
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_join(tids[i], NULL));
  }
  // memory collection
  delete cond;
  cond = NULL;
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_result.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  if (OB_SUCCESS != ObClockGenerator::init()) {
    TRANS_LOG(WARN, "init ob_clock_generator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}

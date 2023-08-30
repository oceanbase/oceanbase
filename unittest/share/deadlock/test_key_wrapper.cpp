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

#include "share/deadlock/ob_deadlock_key_wrapper.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/deadlock/test/test_key.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace share::detector;
using namespace std;


class TestUserBinaryKey : public ::testing::Test {
public:
  TestUserBinaryKey() {}
  ~TestUserBinaryKey() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// 序列化和反序列化的功能
TEST_F(TestUserBinaryKey, serialization) {
  UserBinaryKey key1;
  key1.set_user_key(ObDeadLockTestIntKey(1));
  const int64_t length = 1024;
  int64_t pos = 0;
  char* buffer = new char[key1.get_serialize_size()];
  ASSERT_EQ(OB_SUCCESS, key1.serialize(buffer, length, pos));
  UserBinaryKey key2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, key2.deserialize(buffer, length, pos));
  ASSERT_EQ(true, key1 == key2);
}

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_key_wrapper.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_key_wrapper.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
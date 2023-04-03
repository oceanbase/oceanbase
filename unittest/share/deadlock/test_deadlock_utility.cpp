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

#include "share/deadlock/ob_deadlock_detector_common_define.h"

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


class TestDeadLockUtility : public ::testing::Test {
public:
  TestDeadLockUtility() {}
  ~TestDeadLockUtility() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// 序列化和反序列化的功能
TEST_F(TestDeadLockUtility, interface) {
  ObDetectorUserReportInfo info;
  ObSharedGuard<char> ptr;
  ptr.assign((char*)("a"), [](char*){});
  ASSERT_EQ(OB_SUCCESS, info.set_module_name(ptr));
  ptr.assign((char*)("b"), [](char*){});
  ASSERT_EQ(OB_SUCCESS, info.set_visitor(ptr));
  ptr.assign((char*)("c"), [](char*){});
  ASSERT_EQ(OB_SUCCESS, info.set_resource(ptr));
  ASSERT_EQ(OB_SUCCESS, info.set_extra_info("1","2"));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info(ObString("1"),"2"));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info("1",ObString("2")));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info(ObString("1"),ObString("2")));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info("1",ObString("2"),ObString("3"),"4"));// OK
  // info.set_extra_info("1",ObString("2"),ObString("3"));// compile error, number of args not even
  // info.set_extra_info(string("1"), "2");// compile error, arg types neither ObString nor char*
  // info.set_extra_info("1","2","3","4","5","6","7","8","9","10","11","12");// compile error, number of args reach column's limit
}

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_deadlock_utility.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_deadlock_utility.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
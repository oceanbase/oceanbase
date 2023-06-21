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
#include <gtest/gtest.h>
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/multi_data_source/compile_utility/compile_mapper.h"
namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;

class TestMdsCompile: public ::testing::Test
{
public:
  TestMdsCompile() {};
  virtual ~TestMdsCompile() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsCompile);
};

TEST_F(TestMdsCompile, basic) {
  ObTuple<int, double, char> tuple_;
  tuple_.element<int>() = 1;
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
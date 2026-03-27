/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "logservice/leader_coordinator/table_accessor.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace palf;
using namespace logservice;
using namespace logservice::coordinator;
namespace unittest
{

class TestObTule : public ::testing::Test
{
public:
  TestObTule() {}
};

TEST_F(TestObTule, normal)
{
  double num = 4.53;
  ObTuple<int64_t, ObString, bool, double> t1(1, "23456", true, num);
  char buffer[t1.get_serialize_size()];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, t1.serialize(buffer, t1.get_serialize_size(), pos));
  ObTuple<int64_t, ObString, bool, double> t2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, t2.deserialize(buffer, t1.get_serialize_size(), pos));
  OB_LOG(INFO, "debug", K(t1), K(t2));
  ASSERT_EQ(t2.element<0>(), 1);
  ASSERT_EQ(t2.element<1>(), "23456");
  ASSERT_EQ(t2.element<2>(), true);
  ASSERT_EQ(t2.element<3>(), num);
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf etc run log wallet store");
  system("rm -rf test_ob_tuple.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_tuple.log", false, false);
  logger.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
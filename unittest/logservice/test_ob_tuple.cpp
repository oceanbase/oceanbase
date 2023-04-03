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
#include "common/ob_range.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "logservice/leader_coordinator/table_accessor.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include "lib/net/ob_addr.h"

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
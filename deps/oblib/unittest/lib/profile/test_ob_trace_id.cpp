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

#include "lib/profile/ob_trace_id.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"

using namespace oceanbase::common;
class TestTraceID: public ::testing::Test
{
public:
  TestTraceID() {}
  virtual ~TestTraceID(){}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTraceID);
protected:
  // function members
protected:
  // data members
};

TEST_F(TestTraceID, basic_test)
{
  ObAddr fake_addr;
  fake_addr.parse_from_cstring("127.0.0.1:1000");

  ASSERT_EQ(32, sizeof(ObCurTraceId::TraceId));
  ObCurTraceId::init(fake_addr);
  ASSERT_FALSE(ObCurTraceId::is_user_request());
  ObCurTraceId::mark_user_request();
  ASSERT_TRUE(ObCurTraceId::is_user_request());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "deps/oblib/src/lib/thread/thread.h"

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

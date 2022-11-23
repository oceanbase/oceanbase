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

#include "sql/executor/ob_task_event.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObTaskEventTest : public ::testing::Test
{
public:
  ObTaskEventTest();
  virtual ~ObTaskEventTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObTaskEventTest(const ObTaskEventTest &other);
  ObTaskEventTest& operator=(const ObTaskEventTest &other);
private:
  // data members
};
ObTaskEventTest::ObTaskEventTest()
{
}

ObTaskEventTest::~ObTaskEventTest()
{
}

void ObTaskEventTest::SetUp()
{
}

void ObTaskEventTest::TearDown()
{
}

TEST_F(ObTaskEventTest, all_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  const static uint64_t valid_id_c = 423;
  const static int64_t err_code_a = static_cast<int64_t>(OB_NOT_SUPPORTED);
  ObTaskEvent task_event;
  ObTaskEvent task_event2;
  ObTaskLocation task_loc;
  ObTaskLocation task_loc2;
  ObJobID job_id;
  ObTaskID task_id;
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);
  ObAddr ctrl_server(ObAddr::IPV4, "127.0.0.1", 8080);

  // initial state
  ASSERT_FALSE(task_event.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, task_event.init(task_loc, err_code_a));
  ASSERT_FALSE(task_event.is_valid());
  ASSERT_EQ(OB_INVALID_ID, task_loc.get_execution_id());
  ASSERT_EQ(OB_INVALID_ID, task_loc.get_job_id());
  ASSERT_EQ(OB_INVALID_ID, task_loc.get_task_id());
  ASSERT_TRUE(job_id.equal(task_loc.get_ob_job_id()));
  ASSERT_TRUE(task_id.equal(task_loc.get_ob_task_id()));
  ASSERT_TRUE(task_event.get_task_location().equal(task_loc));
  ASSERT_TRUE(task_event.get_task_location().equal(task_loc2));
  ASSERT_FALSE(task_event.get_err_code() == err_code_a);

  // state change
  task_loc.set_server(server);
  task_loc.set_ctrl_server(ctrl_server);
  task_loc.set_execution_id(valid_id_a);
  task_loc.set_job_id(valid_id_b);
  task_loc.set_task_id(valid_id_c);
  ASSERT_TRUE(task_loc.is_valid());
  ASSERT_EQ(OB_SUCCESS, task_event.init(task_loc, err_code_a));
  ASSERT_TRUE(task_event.is_valid());
  ASSERT_TRUE(task_event.get_task_location().equal(task_loc));
  ASSERT_FALSE(task_event.get_task_location().equal(task_loc2));
  ASSERT_TRUE(task_event.get_err_code() == err_code_a);

  // assign
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR_TASK_EVENT);
  ASSERT_FALSE(task_event.equal(task_event2));
  ASSERT_FALSE(task_event2.is_valid());
  ASSERT_EQ(OB_SUCCESS, task_event2.assign(allocator, task_event));
  ASSERT_TRUE(task_event.equal(task_event2));
  ASSERT_TRUE(task_event2.is_valid());

  task_event.reset();
  ASSERT_FALSE(task_event.equal(task_event2));
  ASSERT_FALSE(task_event.get_task_location().equal(task_loc));
  ASSERT_TRUE(task_event.get_task_location().equal(task_loc2));
  ASSERT_FALSE(task_event.get_err_code() == err_code_a);
  ASSERT_FALSE(task_event.is_valid());
  ASSERT_TRUE(task_event2.is_valid());
  task_event2.reset();
  ASSERT_TRUE(task_event.equal(task_event2));
  ASSERT_FALSE(task_event.is_valid());
  ASSERT_FALSE(task_event2.is_valid());
}

TEST_F(ObTaskEventTest, serialize_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  const static uint64_t valid_id_c = 423;
  const static uint64_t err_code_a = static_cast<int64_t>(OB_NOT_SUPPORTED);
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);
  ObAddr ctrl_server(ObAddr::IPV4, "127.0.0.1", 8080);

  ObTaskEvent task_event;
  ObTaskEvent task_event2;
  ObTaskLocation task_loc;

  // invalid val deserialize
  char buf[1024];
  int64_t buf_len = 1024;
  int64_t pos = 0;
  int64_t pos2 = 0;
  int ret = task_event.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  task_event2.deserialize(buf, pos, pos2);
  EXPECT_FALSE(true == task_event.is_valid());
  EXPECT_FALSE(true == task_event2.is_valid());
  ASSERT_TRUE(task_event.equal(task_event2));

  // valid val deserialize
  task_loc.set_server(server);
  task_loc.set_ctrl_server(ctrl_server);
  task_loc.set_execution_id(valid_id_a);
  task_loc.set_job_id(valid_id_b);
  task_loc.set_task_id(valid_id_c);
  ASSERT_EQ(OB_SUCCESS, task_event.init(task_loc, err_code_a));
  ret = task_event.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  task_event2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(true == task_event.is_valid());
  EXPECT_TRUE(true  == task_event2.is_valid());
  ASSERT_TRUE(task_event.equal(task_event2));

  EXPECT_TRUE(task_event.get_serialize_size() == task_event2.get_serialize_size());
  SQL_EXE_LOG(INFO, "info:", K(task_event));
  SQL_EXE_LOG(INFO, "info:", "size", task_event.get_serialize_size());
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

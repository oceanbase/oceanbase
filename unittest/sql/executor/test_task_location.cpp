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

#include "sql/executor/ob_task_location.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObTaskLocationTest : public ::testing::Test
{
public:
  ObTaskLocationTest();
  virtual ~ObTaskLocationTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObTaskLocationTest(const ObTaskLocationTest &other);
  ObTaskLocationTest& operator=(const ObTaskLocationTest &other);
private:
  // data members
};
ObTaskLocationTest::ObTaskLocationTest()
{
}

ObTaskLocationTest::~ObTaskLocationTest()
{
}

void ObTaskLocationTest::SetUp()
{
}

void ObTaskLocationTest::TearDown()
{
}

TEST_F(ObTaskLocationTest, all_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  const static uint64_t valid_id_c = 423;
  ObTaskLocation task_loc;
  ObTaskLocation task_loc2;
  ObTaskLocation task_loc3;
  ObJobID job_id;
  ObTaskID task_id;
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);
  ObAddr ctrl_server(ObAddr::IPV4, "127.0.0.1", 8888);

  // initial state
  ASSERT_EQ(OB_INVALID_ID, task_loc.get_execution_id());
  ASSERT_EQ(OB_INVALID_ID, task_loc.get_job_id());
  ASSERT_EQ(OB_INVALID_ID, task_loc.get_task_id());
  ASSERT_TRUE(job_id.equal(task_loc.get_ob_job_id()));
  ASSERT_TRUE(task_id.equal(task_loc.get_ob_task_id()));

  // state change
  task_loc.set_server(server);
  task_loc.set_ctrl_server(ctrl_server);
  task_loc.set_execution_id(valid_id_a);
  task_loc.set_job_id(valid_id_b);
  task_loc.set_task_id(valid_id_c);
  ASSERT_TRUE(task_loc.is_valid());

  // state change
  ASSERT_FALSE(task_loc2.is_valid());
  task_loc2.set_server(server);
  ASSERT_FALSE(task_loc2.is_valid());
  task_loc2.set_ctrl_server(ctrl_server);
  ASSERT_FALSE(task_loc2.is_valid());
  task_loc2.set_execution_id(valid_id_a);
  ASSERT_FALSE(task_loc2.is_valid());
  task_loc2.set_job_id(valid_id_b);
  ASSERT_FALSE(task_loc2.is_valid());
  task_loc2.set_task_id(valid_id_c);
  ASSERT_TRUE(task_loc2.is_valid());

  // assign
  ASSERT_FALSE(task_loc3.equal(task_loc));
  task_loc3 = task_loc;
  ASSERT_TRUE(task_loc3.equal(task_loc));

  ASSERT_EQ(valid_id_a, task_loc.get_execution_id());
  ASSERT_EQ(valid_id_b, task_loc.get_job_id());
  ASSERT_EQ(valid_id_c, task_loc.get_task_id());
  ASSERT_EQ(server, task_loc.get_server());

  ASSERT_TRUE(task_loc.equal(task_loc2));
  ASSERT_TRUE(task_loc.hash() == task_loc2.hash());

  task_loc.reset();
  ASSERT_FALSE(task_loc.equal(task_loc2));
  task_loc2.reset();
  ASSERT_TRUE(task_loc.equal(task_loc2));
}

TEST_F(ObTaskLocationTest, serialize_test)
{
  ObTaskLocation task_loc;
  ObTaskLocation task_loc2;

  task_loc.set_execution_id(OB_INVALID_ID);
  task_loc.set_job_id(OB_INVALID_ID);
  task_loc.set_task_id(OB_INVALID_ID);

  // invalid val deserialize
  char buf[1024];
  int64_t buf_len = 1024;
  int64_t pos = 0;
  int64_t pos2 = 0;
  int ret = task_loc.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  task_loc2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(true == task_loc.equal(task_loc2));

  // valid val deserialize
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);
  ObAddr ctrl_server(ObAddr::IPV4, "127.0.0.1", 8888);

  task_loc.set_server(server);
  task_loc.set_ctrl_server(ctrl_server);
  task_loc.set_execution_id(valid_id_a);
  task_loc.set_job_id(valid_id_b);
  task_loc.set_task_id(valid_id_c);
  task_loc.set_server(server);
  ret = task_loc.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  task_loc2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(true == task_loc.is_valid());
  EXPECT_TRUE(true  == task_loc2.is_valid());
  EXPECT_TRUE(true == task_loc.equal(task_loc2));

  EXPECT_TRUE(task_loc.get_serialize_size() == task_loc2.get_serialize_size());
  SQL_EXE_LOG(INFO, "info:", K(task_loc));
  SQL_EXE_LOG(INFO, "info:", "size", task_loc.get_serialize_size());
}

TEST_F(ObTaskLocationTest, hash_test)
{
  ObJobID job_id;
  ObTaskLocation task_loc;
  ObTaskLocation task_loc2;
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);

  task_loc.set_server(server);
  task_loc.set_execution_id(valid_id_a);
  task_loc.set_job_id(valid_id_b);
  task_loc.set_task_id(valid_id_c);

  task_loc2.set_server(server);
  task_loc2.set_execution_id(valid_id_a);
  task_loc2.set_job_id(valid_id_b);
  task_loc2.set_task_id(valid_id_c);

  ASSERT_TRUE(task_loc.hash() == task_loc2.hash());
  task_loc.reset();
  task_loc2.reset();
  ASSERT_TRUE(task_loc.hash() == task_loc2.hash());
}


TEST_F(ObTaskLocationTest, struct_test)
{
  ObJobID job_id;
  ObTaskID task_id;
  ObTaskLocation task_loc;
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);

  task_loc.set_server(server);
  task_loc.set_execution_id(valid_id_a);
  task_loc.set_job_id(valid_id_b);
  task_loc.set_task_id(valid_id_c);

  job_id = task_loc.get_ob_job_id();
  EXPECT_TRUE(job_id.get_execution_id() == valid_id_a);
  EXPECT_TRUE(job_id.get_job_id() == valid_id_b);
  EXPECT_TRUE(job_id.get_execution_id() == task_loc.get_execution_id());
  EXPECT_TRUE(job_id.get_job_id() == task_loc.get_job_id());


  task_id = task_loc.get_ob_task_id();
  EXPECT_TRUE(task_id.get_execution_id() == valid_id_a);
  EXPECT_TRUE(task_id.get_job_id() == valid_id_b);
  EXPECT_TRUE(task_id.get_task_id() == valid_id_c);
  EXPECT_TRUE(task_id.get_execution_id() == task_loc.get_execution_id());
  EXPECT_TRUE(task_id.get_job_id() == task_loc.get_job_id());
  EXPECT_TRUE(task_id.get_task_id() == task_loc.get_task_id());
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

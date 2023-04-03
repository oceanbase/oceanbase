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
#include "sql/executor/ob_task_id.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObTaskIDTest : public ::testing::Test
{
public:
  ObTaskIDTest();
  virtual ~ObTaskIDTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObTaskIDTest(const ObTaskIDTest &other);
  ObTaskIDTest& operator=(const ObTaskIDTest &other);
private:
  // data members
};
ObTaskIDTest::ObTaskIDTest()
{
}

ObTaskIDTest::~ObTaskIDTest()
{
}

void ObTaskIDTest::SetUp()
{
}

void ObTaskIDTest::TearDown()
{
}

TEST_F(ObTaskIDTest, basic_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  const static uint64_t valid_id_c = 423;
  ObTaskID task_id;

  // initial state
  ASSERT_EQ(OB_INVALID_ID, task_id.get_execution_id());
  ASSERT_EQ(OB_INVALID_ID, task_id.get_job_id());
  ASSERT_EQ(OB_INVALID_ID, task_id.get_task_id());

  // state change
  task_id.set_execution_id(valid_id_a);
  task_id.set_job_id(valid_id_b);
  task_id.set_task_id(valid_id_c);
  ASSERT_EQ(valid_id_a, task_id.get_execution_id());
  ASSERT_EQ(valid_id_b, task_id.get_job_id());
  ASSERT_EQ(valid_id_c, task_id.get_task_id());
}

TEST_F(ObTaskIDTest, equal_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  const static uint64_t valid_id_c = 423;
  ObTaskID task_id;
  ObTaskID task_id2;

  // consist initial state
  ASSERT_EQ(true, task_id.equal(task_id2));
  ASSERT_EQ(true, task_id2.equal(task_id));

  // state change
  task_id.set_execution_id(valid_id_a);
  task_id.set_job_id(valid_id_b);
  task_id.set_task_id(valid_id_c);
  task_id2.set_execution_id(valid_id_a);
  task_id2.set_job_id(valid_id_b);
  task_id2.set_task_id(valid_id_c);
  ASSERT_EQ(true, task_id.equal(task_id2));
  ASSERT_EQ(true, task_id2.equal(task_id));

  // state change
  task_id.set_execution_id(valid_id_a);
  task_id.set_job_id(valid_id_b);
  task_id.set_task_id(valid_id_c);
  task_id2.set_execution_id(valid_id_a);
  task_id2.set_job_id(valid_id_a);
  task_id2.set_task_id(valid_id_a);
  EXPECT_TRUE(false == task_id.equal(task_id2));
  EXPECT_TRUE(false == task_id2.equal(task_id));

  task_id.reset();
  task_id2.reset();
  EXPECT_TRUE(false == task_id.is_valid());
  EXPECT_TRUE(false == task_id2.is_valid());
  EXPECT_TRUE(true == task_id.equal(task_id2));
}


TEST_F(ObTaskIDTest, valid_test)
{
  const static uint64_t valid_id = 1;
  ObTaskID task_id;
  task_id.set_execution_id(OB_INVALID_ID);
  EXPECT_TRUE(false == task_id.is_valid());
  task_id.set_task_id(OB_INVALID_ID);
  EXPECT_TRUE(false == task_id.is_valid());


  task_id.set_execution_id(OB_INVALID_ID);
  task_id.set_task_id(valid_id);
  EXPECT_TRUE(false == task_id.is_valid());


  task_id.set_execution_id(valid_id);
  task_id.set_task_id(OB_INVALID_ID);
  EXPECT_TRUE(false == task_id.is_valid());


  task_id.set_execution_id(valid_id);
  task_id.set_job_id(valid_id);
  task_id.set_task_id(valid_id);
  EXPECT_TRUE(false == task_id.is_valid());

  task_id.set_server(ObAddr(ObAddr::IPV4, "127.0.0.1", 8888));
  task_id.set_execution_id(valid_id);
  task_id.set_job_id(valid_id);
  task_id.set_task_id(valid_id);
  EXPECT_TRUE(true == task_id.is_valid());

}

TEST_F(ObTaskIDTest, serialize_test)
{
  ObTaskID task_id;
  ObTaskID task_id2;

  task_id.set_server(ObAddr(ObAddr::IPV4, "127.0.0.1", 8888));
  task_id.set_execution_id(OB_INVALID_ID);
  task_id.set_job_id(OB_INVALID_ID);
  task_id.set_task_id(OB_INVALID_ID);

  // invalid val deserialize
  char buf[1024];
  int64_t buf_len = 1024;
  int64_t pos = 0;
  int64_t pos2 = 0;
  int ret = task_id.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  task_id2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(false == task_id.is_valid());
  EXPECT_TRUE(false  == task_id2.is_valid());
  EXPECT_TRUE(true == task_id.equal(task_id2));

  // valid val deserialize
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;
  task_id.set_execution_id(valid_id_a);
  task_id.set_job_id(valid_id_b);
  task_id.set_task_id(valid_id_c);
  ret = task_id.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  task_id2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(true == task_id.is_valid());
  EXPECT_TRUE(true  == task_id2.is_valid());
  EXPECT_TRUE(true == task_id.equal(task_id2));

  EXPECT_TRUE(task_id.get_serialize_size() == task_id2.get_serialize_size());
  SQL_EXE_LOG(INFO, "info", K(task_id), "size", task_id.get_serialize_size());

}

TEST_F(ObTaskIDTest, hash_test)
{
  ObJobID job_id;
  ObTaskID task_id;
  ObTaskID task_id2;
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;

  task_id.set_execution_id(valid_id_a);
  task_id.set_job_id(valid_id_b);
  task_id.set_task_id(valid_id_c);

  task_id2.set_execution_id(valid_id_a);
  task_id2.set_job_id(valid_id_b);
  task_id2.set_task_id(valid_id_c);

  ASSERT_TRUE(task_id.hash() == task_id2.hash());
  task_id.reset();
  task_id2.reset();
  ASSERT_TRUE(task_id.hash() == task_id2.hash());

}


TEST_F(ObTaskIDTest, struct_test)
{
  ObJobID job_id;
  ObTaskID task_id;
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;

  task_id.set_execution_id(valid_id_a);
  task_id.set_job_id(valid_id_b);
  task_id.set_task_id(valid_id_c);

  job_id = task_id.get_ob_job_id();
  EXPECT_TRUE(job_id.get_execution_id() == valid_id_a);
  EXPECT_TRUE(job_id.get_job_id() == valid_id_b);
  EXPECT_TRUE(job_id.get_job_id() == task_id.get_job_id());
  EXPECT_TRUE(job_id.get_execution_id() == task_id.get_execution_id());

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

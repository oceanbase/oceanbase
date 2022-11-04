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
#include "sql/executor/ob_job_id.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObJobIDTest : public ::testing::Test
{
public:
  ObJobIDTest();
  virtual ~ObJobIDTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObJobIDTest(const ObJobIDTest &other);
  ObJobIDTest& operator=(const ObJobIDTest &other);
private:
  // data members
};
ObJobIDTest::ObJobIDTest()
{
}

ObJobIDTest::~ObJobIDTest()
{
}

void ObJobIDTest::SetUp()
{
}

void ObJobIDTest::TearDown()
{
}

TEST_F(ObJobIDTest, basic_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  ObJobID job_id;

  // initial state
  ASSERT_EQ(OB_INVALID_ID, job_id.get_execution_id());
  ASSERT_EQ(OB_INVALID_ID, job_id.get_job_id());

  // state change
  job_id.set_execution_id(valid_id_a);
  job_id.set_job_id(valid_id_b);
  ASSERT_EQ(valid_id_a, job_id.get_execution_id());
  ASSERT_EQ(valid_id_b, job_id.get_job_id());
}

TEST_F(ObJobIDTest, equal_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  ObJobID job_id;
  ObJobID job_id2;

  // consist initial state
  ASSERT_EQ(true, job_id.equal(job_id2));
  ASSERT_EQ(true, job_id2.equal(job_id));

  // state change
  job_id.set_execution_id(valid_id_a);
  job_id.set_job_id(valid_id_b);
  job_id2.set_execution_id(valid_id_a);
  job_id2.set_job_id(valid_id_b);
  ASSERT_EQ(true, job_id.equal(job_id2));
  ASSERT_EQ(true, job_id2.equal(job_id));

  // state change
  job_id.set_execution_id(valid_id_a);
  job_id.set_job_id(valid_id_b);
  job_id2.set_execution_id(valid_id_a);
  job_id2.set_job_id(valid_id_a);
  EXPECT_TRUE(false == job_id.equal(job_id2));
  EXPECT_TRUE(false == job_id2.equal(job_id));

  job_id.reset();
  job_id2.reset();
  EXPECT_TRUE(false == job_id.is_valid());
  EXPECT_TRUE(false == job_id2.is_valid());
  EXPECT_TRUE(true == job_id.equal(job_id2));
}


TEST_F(ObJobIDTest, valid_test)
{
  const static uint64_t valid_id = 1;
  ObJobID job_id;
  job_id.set_execution_id(OB_INVALID_ID);
  job_id.set_job_id(OB_INVALID_ID);
  EXPECT_TRUE(false == job_id.is_valid());

  job_id.set_server(ObAddr(ObAddr::IPV4, "127.0.0.1", 8888));
  job_id.set_execution_id(OB_INVALID_ID);
  job_id.set_job_id(OB_INVALID_ID);
  EXPECT_TRUE(false == job_id.is_valid());

  job_id.set_execution_id(OB_INVALID_ID);
  job_id.set_job_id(valid_id);
  EXPECT_TRUE(false == job_id.is_valid());


  job_id.set_execution_id(valid_id);
  job_id.set_job_id(OB_INVALID_ID);
  EXPECT_TRUE(false == job_id.is_valid());


  job_id.set_execution_id(valid_id);
  job_id.set_job_id(valid_id);
  EXPECT_TRUE(true == job_id.is_valid());

}

TEST_F(ObJobIDTest, serialize_test)
{
  ObJobID job_id;
  ObJobID job_id2;

  job_id.set_server(ObAddr(ObAddr::IPV4, "127.0.0.1", 8888));
  job_id.set_execution_id(OB_INVALID_ID);
  job_id.set_job_id(OB_INVALID_ID);

  // invalid val deserialize
  char buf[1024];
  int64_t buf_len = 1024;
  int64_t pos = 0;
  int64_t pos2 = 0;
  int ret = job_id.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  job_id2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(false == job_id.is_valid());
  EXPECT_TRUE(false  == job_id2.is_valid());
  EXPECT_TRUE(true == job_id.equal(job_id2));

  // valid val deserialize
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  job_id.set_execution_id(valid_id_a);
  job_id.set_job_id(valid_id_b);
  ret = job_id.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  job_id2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(true == job_id.is_valid());
  EXPECT_TRUE(true  == job_id2.is_valid());
  EXPECT_TRUE(true == job_id.equal(job_id2));

  SQL_EXE_LOG(INFO, "info", K(job_id));

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

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
#include "sql/executor/ob_slice_id.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObSliceIDTest : public ::testing::Test
{
public:
  ObSliceIDTest();
  virtual ~ObSliceIDTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObSliceIDTest(const ObSliceIDTest &other);
  ObSliceIDTest& operator=(const ObSliceIDTest &other);
private:
  // data members
};
ObSliceIDTest::ObSliceIDTest()
{
}

ObSliceIDTest::~ObSliceIDTest()
{
}

void ObSliceIDTest::SetUp()
{
}

void ObSliceIDTest::TearDown()
{
}

TEST_F(ObSliceIDTest, all_test)
{
  const static uint64_t valid_id_a = 1033;
  const static uint64_t valid_id_b = 42233;
  const static uint64_t valid_id_c = 423;
  const static uint64_t valid_id_d = 1;
  ObSliceID slice_id;
  ObSliceID slice_id2;
  ObJobID job_id;
  ObTaskID task_id;
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);

  // initial state
  ASSERT_EQ(OB_INVALID_ID, slice_id.get_execution_id());
  ASSERT_EQ(OB_INVALID_ID, slice_id.get_job_id());
  ASSERT_EQ(OB_INVALID_ID, slice_id.get_slice_id());
  ASSERT_TRUE(job_id.equal(slice_id.get_ob_job_id()));
  ASSERT_TRUE(task_id.equal(slice_id.get_ob_task_id()));

  // state change
  slice_id.set_server(server);
  slice_id.set_execution_id(valid_id_a);
  slice_id.set_job_id(valid_id_b);
  slice_id.set_task_id(valid_id_c);
  slice_id.set_slice_id(valid_id_d);
  ASSERT_TRUE(slice_id.is_valid());

  // state change
  ASSERT_FALSE(slice_id2.is_valid());
  slice_id2.set_server(server);
  ASSERT_FALSE(slice_id2.is_valid());
  slice_id2.set_execution_id(valid_id_a);
  ASSERT_FALSE(slice_id2.is_valid());
  slice_id2.set_job_id(valid_id_b);
  ASSERT_FALSE(slice_id2.is_valid());
  slice_id2.set_task_id(valid_id_c);
  ASSERT_FALSE(slice_id2.is_valid());
  slice_id2.set_slice_id(valid_id_d);
  ASSERT_TRUE(slice_id2.is_valid());


  ASSERT_EQ(valid_id_a, slice_id.get_execution_id());
  ASSERT_EQ(valid_id_b, slice_id.get_job_id());
  ASSERT_EQ(valid_id_c, slice_id.get_task_id());
  ASSERT_EQ(valid_id_d, slice_id.get_slice_id());
  ASSERT_EQ(server, slice_id.get_server());

  ASSERT_TRUE(slice_id.equal(slice_id2));
  ASSERT_TRUE(slice_id.hash() == slice_id2.hash());

  slice_id.reset();
  ASSERT_FALSE(slice_id.equal(slice_id2));
  slice_id2.reset();
  ASSERT_TRUE(slice_id.equal(slice_id2));


}

TEST_F(ObSliceIDTest, serialize_test)
{
  ObSliceID slice_id;
  ObSliceID slice_id2;

  slice_id.set_execution_id(OB_INVALID_ID);
  slice_id.set_job_id(OB_INVALID_ID);
  slice_id.set_slice_id(OB_INVALID_ID);

  // invalid val deserialize
  char buf[1024];
  int64_t buf_len = 1024;
  int64_t pos = 0;
  int64_t pos2 = 0;
  int ret = slice_id.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  slice_id2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(true == slice_id.equal(slice_id2));

  // valid val deserialize
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;
  uint64_t valid_id_d = 1;
  ObAddr server(ObAddr::IPV4, "127.0.0.1", 8080);

  slice_id.set_execution_id(valid_id_a);
  slice_id.set_job_id(valid_id_b);
  slice_id.set_task_id(valid_id_c);
  slice_id.set_slice_id(valid_id_d);
  slice_id.set_server(server);
  ret = slice_id.serialize(buf, buf_len, pos);
  EXPECT_TRUE(OB_SUCC(ret));
  slice_id2.deserialize(buf, pos, pos2);
  EXPECT_TRUE(true == slice_id.is_valid());
  EXPECT_TRUE(true  == slice_id2.is_valid());
  EXPECT_TRUE(true == slice_id.equal(slice_id2));

  EXPECT_TRUE(slice_id.get_serialize_size() == slice_id2.get_serialize_size());
  SQL_EXE_LOG(INFO, "info:", K(slice_id));
  SQL_EXE_LOG(INFO, "info:", "size", slice_id.get_serialize_size());
}

TEST_F(ObSliceIDTest, hash_test)
{
  ObJobID job_id;
  ObSliceID slice_id;
  ObSliceID slice_id2;
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;

  slice_id.set_execution_id(valid_id_a);
  slice_id.set_job_id(valid_id_b);
  slice_id.set_slice_id(valid_id_c);

  slice_id2.set_execution_id(valid_id_a);
  slice_id2.set_job_id(valid_id_b);
  slice_id2.set_slice_id(valid_id_c);

  ASSERT_TRUE(slice_id.hash() == slice_id2.hash());
  slice_id.reset();
  slice_id2.reset();
  ASSERT_TRUE(slice_id.hash() == slice_id2.hash());

}


TEST_F(ObSliceIDTest, struct_test)
{
  ObJobID job_id;
  ObSliceID slice_id;
  uint64_t valid_id_a = 1012;
  uint64_t valid_id_b = 13012;
  uint64_t valid_id_c = 130120;

  slice_id.set_execution_id(valid_id_a);
  slice_id.set_job_id(valid_id_b);
  slice_id.set_slice_id(valid_id_c);

  job_id = slice_id.get_ob_job_id();
  EXPECT_TRUE(job_id.get_execution_id() == valid_id_a);
  EXPECT_TRUE(job_id.get_job_id() == valid_id_b);
  EXPECT_TRUE(job_id.get_job_id() == slice_id.get_job_id());
  EXPECT_TRUE(job_id.get_execution_id() == slice_id.get_execution_id());

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

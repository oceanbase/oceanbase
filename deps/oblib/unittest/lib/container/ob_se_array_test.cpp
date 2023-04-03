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

#include "lib/container/ob_se_array.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class ObSEArrayTest: public ::testing::Test
{
  public:
    ObSEArrayTest();
    virtual ~ObSEArrayTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSEArrayTest(const ObSEArrayTest &other);
    ObSEArrayTest& operator=(const ObSEArrayTest &other);
  private:
    // data members
};

ObSEArrayTest::ObSEArrayTest()
{
}

ObSEArrayTest::~ObSEArrayTest()
{
}

void ObSEArrayTest::SetUp()
{
}

void ObSEArrayTest::TearDown()
{
}

struct A
{
  int64_t a_;
  TO_STRING_EMPTY();
};

TEST_F(ObSEArrayTest, basic_test)
{
  ObSEArray<A, 256> searray, searray2;
  A a;
  int NUMS[] = {256, 1024};
  for (int k = 0; k < (int)ARRAYSIZEOF(NUMS); ++k)
  {
    int num = NUMS[k];
    searray.reset();
    searray2.reset();
    for (int64_t i = 0; i < num; ++i)
    {
      a.a_ = i;
      ASSERT_EQ(OB_SUCCESS, searray.push_back(a));
      ASSERT_EQ(i+1, searray.count());
    }
    searray2 = searray;
    for (int64_t i = 0; i < num; ++i)
    {
      ASSERT_EQ(i, searray2.at(i).a_);
    }
    searray.reset();
    ASSERT_EQ(0, searray.count());
  }
}

TEST_F(ObSEArrayTest, destroy)
{
  ObSEArray<A, 8> searray;
  for (int i = 0; i < 9; ++i)
  {
    A a;
    a.a_ = i;
    ASSERT_EQ(OB_SUCCESS, searray.push_back(a));
  }
  searray.destroy();
}

TEST(ObSEArrayTest2, array_remove)
{
  ObSEArray<int, 10> ai;
  for (int i = 0; i < 32; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, ai.push_back(i));
  }
  ASSERT_EQ(32, ai.count());

  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, ai.remove(-1));
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, ai.remove(32));
  ASSERT_EQ(32, ai.count());

  ASSERT_EQ(OB_SUCCESS, ai.remove(10));
  int v = 0;
  ASSERT_EQ(31, static_cast<int>(ai.count()));
  for (int i = 0; i < 31; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, ai.at(static_cast<int64_t>(i), v));
    ASSERT_NE(10, v);
  }
}

TEST(ObSEArrayTest2, serialize)
{
  char buf[1024];
  ObSEArray<int64_t, 100> array;
  const int64_t NUM = 20;
  for (int64_t i = 0; i < NUM; i ++)
  {
    array.push_back(i);
  }

  int64_t pos = 0;
  array.serialize(buf, 1024, pos);

  ObSEArray<int64_t, 100> array2;
  int64_t data_len = pos;
  pos = 0;
  array2.push_back(30);
  array2.deserialize(buf, data_len, pos);

  ASSERT_EQ(array2.count(), NUM);
  for (int64_t i = 0; i < NUM; i ++)
  {
    ASSERT_EQ(i, array2.at(i));
  }
}

TEST_F(ObSEArrayTest, test_null_allocator_array)
{
  const int64_t count = 10;
  int64_t value = 0;
  ObSEArray<int64_t, count, ObNullAllocator> array;
  for (int i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, array.push_back(1));
  }
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, array.push_back(1));
  ASSERT_EQ(OB_SUCCESS, array.pop_back(value));
  ASSERT_EQ(OB_SUCCESS, array.push_back(1));

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

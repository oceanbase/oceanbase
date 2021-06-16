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

#include "lib/container/ob_heap.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class TestComp {
public:
  int get_error_code()
  {
    return OB_SUCCESS;
  }
  bool operator()(int64_t a, int64_t b)
  {
    return a < b ? true : false;
  }
};

class ObHeapTest : public ::testing::Test {
public:
  ObHeapTest();
  virtual ~ObHeapTest();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  ObHeapTest(const ObHeapTest& other);
  ObHeapTest& operator=(const ObHeapTest& other);

private:
  // data members
};

ObHeapTest::ObHeapTest()
{}

ObHeapTest::~ObHeapTest()
{}

void ObHeapTest::SetUp()
{}

void ObHeapTest::TearDown()
{}

TEST_F(ObHeapTest, basic_test)
{
  TestComp tc;
  ObBinaryHeap<int64_t, TestComp, 30> heap(tc, NULL);
  heap.push(1);
  heap.push(9);
  heap.push(8);
  heap.push(7);
  heap.push(11);
  heap.push(6);
  heap.push(4);
  ASSERT_EQ(11, heap.top());
  int ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(9, heap.top());
  ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(8, heap.top());
  ret = heap.replace_top(50);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(50, heap.top());
  ret = heap.replace_top(23);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(23, heap.top());
  ASSERT_EQ(5, heap.count());
}

TEST_F(ObHeapTest, bound_test)
{
  TestComp tc;
  int ret = 0;
  ObArenaAllocator buf;
  ObBinaryHeap<int64_t, TestComp, 5> heap(tc, &buf);
  ret = heap.push(1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(9);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(8);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(7);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(11);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(6);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(4);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(11, heap.top());
  ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(9, heap.top());
  ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(8, heap.top());
  ret = heap.replace_top(50);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(50, heap.top());
  ret = heap.replace_top(23);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(23, heap.top());
  ASSERT_EQ(5, heap.count());
}

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

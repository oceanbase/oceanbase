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
#include "lib/utility/ob_test_util.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_array.h"
using namespace oceanbase::common;

class TestObj {
public:
  TestObj(int64_t val) : value_(val)
  {}
  TestObj() : value_(0)
  {}
  ~TestObj()
  {}
  TestObj& operator=(const TestObj& obj)
  {
    value_ = obj.value_;
    return *this;
  }
  bool operator==(const TestObj& obj) const
  {
    return value_ == obj.value_;
  }

  void set(int64_t val)
  {
    value_ = val;
  }
  TO_STRING_KV(K_(value));
  int64_t value_;
};

class TestArrayArray : public ::testing::Test {
public:
  TestArrayArray();
  virtual ~TestArrayArray();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestArrayArray);

protected:
  // function members
  void setup(int64_t N, ObIArray<TestObj>& arr);
  void verify(int64_t N, const ObIArray<TestObj>& arr);
  void verify(const ObIArray<TestObj>& arr1, const ObIArray<TestObj>& arr2);
  void extend_to(const int64_t N, ObIArray<TestObj>& arr);

protected:
  // data members
};

TestArrayArray::TestArrayArray()
{}

TestArrayArray::~TestArrayArray()
{}

void TestArrayArray::SetUp()
{}

void TestArrayArray::TearDown()
{}

void TestArrayArray::setup(int64_t N, ObIArray<TestObj>& arr)
{
  arr.reset();
  for (int64_t i = 0; i < N; ++i) {
    TestObj obj(i);
    OK(arr.push_back(obj));
  }  // end for
}

void TestArrayArray::verify(int64_t N, const ObIArray<TestObj>& arr)
{
  ASSERT_EQ(N, arr.count());
  for (int64_t i = 0; i < N; ++i) {
    TestObj obj(i);
    ASSERT_EQ(obj, arr.at(i));
  }
}

void TestArrayArray::verify(const ObIArray<TestObj>& arr1, const ObIArray<TestObj>& arr2)
{
  ASSERT_EQ(arr1.count(), arr2.count());
  for (int64_t i = 0; i < arr1.count(); ++i) {
    ASSERT_EQ(arr1.at(i), arr2.at(i));
  }
}

void TestArrayArray::extend_to(const int64_t N, ObIArray<TestObj>& arr)
{
  ASSERT_TRUE(arr.count() <= N);
  for (int64_t i = arr.count(); i < N; ++i) {
    TestObj obj(i);
    OK(arr.push_back(i));
  }  // end for
  ASSERT_EQ(N, arr.count());
}

TEST_F(TestArrayArray, array_push)
{
  int N = 10;
  ObArrayArray<TestObj> arr_arr;
  ObSEArray<TestObj, 10> arr;
  setup(N, arr);
  for (int64_t i = 0; i < N; i++) {
    OK(arr_arr.push_back(arr));
    verify(arr_arr.at(i), arr);
    ASSERT_EQ(arr_arr.count(i), N);
    for (int64_t j = 0; j < arr_arr.count(i); j++) {
      ASSERT_EQ(arr_arr.at(i, j), arr.at(j));
    }
  }
  COMMON_LOG(INFO, "print array array", K(arr_arr));

  arr_arr.reuse();
  N = 20;
  extend_to(N, arr);

  for (int64_t i = 0; i < N; i++) {
    OK(arr_arr.push_back(arr));
    verify(arr_arr.at(i), arr);
    ASSERT_EQ(arr_arr.count(i), N);
    for (int64_t j = 0; j < arr_arr.count(i); j++) {
      ASSERT_EQ(arr_arr.at(i, j), arr.at(j));
    }
  }
  ASSERT_EQ(N, arr_arr.count());
  COMMON_LOG(INFO, "print array array", K(arr_arr));

  for (int64_t i = 0; i < N; i++) {
    TestObj obj(N);
    OK(arr_arr.push_back(i, obj));
  }

  extend_to(N + 1, arr);

  for (int64_t i = 0; i < N; i++) {
    ASSERT_EQ(arr_arr.count(i), N + 1);
    verify(arr_arr.at(i), arr);
  }
  COMMON_LOG(INFO, "print array array", K(arr_arr));
}

int main(int argc, char** argv)
{
  // system("rm -rf test_array_array.log*");
  // OB_LOGGER.set_file_name("test_array_array.log");
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

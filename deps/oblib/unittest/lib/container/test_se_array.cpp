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
using namespace oceanbase::common;

class TestObj
{
public:
  TestObj(int64_t val) : value_(val) {
  }

  TestObj() : value_(0) {
    std::cout << "construct obj " << value_  << std::endl;
  }
  ~TestObj() {
    std::cout << "destruct obj " << value_ << std::endl;
  }
  TestObj &operator=(const TestObj &obj) {
    std::cout << "assign obj " << obj.value_ << std::endl;
    value_ = obj.value_;
    return *this;
  }
  void print() {
    std::cout << "print64_t obj " << value_ << std::endl;
  }

  const int64_t get() const {
    return value_;
  }

  void set(int64_t val) {
    value_ = val;
  }
  TO_STRING_KV("value", value_);
private:
  int64_t value_;
};

class TestSEArray: public ::testing::Test
{
public:
  TestSEArray();
  virtual ~TestSEArray();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSEArray);
protected:
  // function members
  void setup(int64_t N, ObIArray<int64_t> &arr);
  void verify(int64_t N, const ObIArray<int64_t> &arr);
  void verify(int64_t N, const ObIArray<TestObj> &arr);
  void extend_to(int64_t N, ObIArray<int64_t> &arr);
protected:
  // data members
};

TestSEArray::TestSEArray()
{
}

TestSEArray::~TestSEArray()
{
}

void TestSEArray::SetUp()
{
}

void TestSEArray::TearDown()
{
}

void TestSEArray::setup(int64_t N, ObIArray<int64_t> &arr)
{
  arr.reset();
  for (int64_t i = 0;i < N; ++i) {
    OK(arr.push_back(i));
  } // end for
}

void TestSEArray::extend_to(int64_t N, ObIArray<int64_t> &arr)
{
  ASSERT_TRUE(arr.count() <= N);
  for (int64_t i = arr.count(); i < N; ++i) {
    OK(arr.push_back(i));
  } // end for
  ASSERT_EQ(N, arr.count());
}

void TestSEArray::verify(int64_t N, const ObIArray<TestObj> &arr)
{
  ASSERT_EQ(N, arr.count());
  for (int64_t i = 0; i < N; ++i) {
    ASSERT_EQ(i, arr.at(i).get());
  }
}

void TestSEArray::verify(int64_t N, const ObIArray<int64_t> &arr)
{
  ASSERT_EQ(N, arr.count());
  for (int64_t i = 0; i < N; ++i) {
    ASSERT_EQ(i, arr.at(i));
  }
}



TEST_F(TestSEArray, array_push_pop)
{
  const int N = 10;
  ObSEArray<int64_t, N> arr;
  for (int64_t i = 0; i < 2; ++i) {
    setup(N, arr);
    verify(N, arr);
  }

  extend_to(N+9, arr);
  verify(N+9, arr);
}

TEST_F(TestSEArray, obj_push_assign)
{
  ObSEArray<TestObj, 30> arr;
  {
    TestObj obj[10];
    for (int64_t i = 0; i < 10; ++i) {
      obj[i].set(i);
      arr.push_back(obj[i]);
    }
  }
  std::cout << " begin destruct arr" << std::endl;

  ObSEArray<TestObj, 30> arr2;
  ObSEArray<TestObj, 3> arr3;

  arr2 = arr;
  verify(10, arr2);

  arr3 = arr;
  verify(10, arr3);


  ASSERT_EQ(OB_SUCCESS, arr2.assign(arr));
  verify(10, arr2);

  ASSERT_EQ(OB_SUCCESS, arr3.assign(arr));
  verify(10, arr3);
}

TEST_F(TestSEArray, place_holder_1)
{
  ObSEArray<TestObj, 30> arr;

  // test count_ == valid_count_
  TestObj *holder = arr.alloc_place_holder();
  holder->set(1024);
  TestObj elem(3);
  ASSERT_EQ(OB_SUCCESS, arr.pop_back(elem));
  ASSERT_EQ(1024, elem.get());
}

TEST_F(TestSEArray, place_holder_2)
{
  ObSEArray<TestObj, 10> arr;

  // test count_ < valid_count_
  TestObj obj;
  for (int64_t i = 0; i < 10; ++i) {
    obj.set(i);
    arr.push_back(obj);
    ASSERT_EQ(arr.for_test_only_valid_count(), arr.count());
  }

  arr.alloc_place_holder();
  ASSERT_EQ(11, arr.count());
  ASSERT_EQ(10, arr.for_test_only_valid_count());

  for (int64_t i = 0; i < 6; ++i) {
    arr.pop_back();
  }
  arr.alloc_place_holder();
  ASSERT_EQ(6, arr.count());
  ASSERT_EQ(10, arr.for_test_only_valid_count());
}

TEST_F(TestSEArray, prepare_alloc)
{
  // prealloc some objects and do nothing if it exists.
  ObSEArray<TestObj, 10> arr;
  arr.prepare_allocate(3);
  ASSERT_EQ(3, arr.for_test_only_valid_count());
  arr.prepare_allocate(8);
  ASSERT_EQ(8, arr.for_test_only_valid_count());
  arr.prepare_allocate(3);
  ASSERT_EQ(8, arr.for_test_only_valid_count());
  arr.prepare_allocate(11);
  ASSERT_EQ(8, arr.for_test_only_valid_count());
  arr.prepare_allocate(9);
  ASSERT_EQ(9, arr.for_test_only_valid_count());
  arr.prepare_allocate(10);
  ASSERT_EQ(10, arr.for_test_only_valid_count());
  arr.prepare_allocate(12);
  ASSERT_EQ(10, arr.for_test_only_valid_count());
}

TEST_F(TestSEArray, copy_construct)
{
  ObSEArray<int64_t, 10> arr;
  setup(10, arr);

  // test copy construct function
  ObSEArray<int64_t, 10> copy1 = arr;
  verify(10, copy1);
}

TEST_F(TestSEArray, remove_core_dump)
{
  typedef ObArray<int> Item;

  Item item;

  ObSEArray<Item, 2> a;

  ObArray<Item> b;
  b.push_back(item);
  b.push_back(item);
  b.push_back(item);

  a = b;
  a.remove(0);
  a.remove(0);
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

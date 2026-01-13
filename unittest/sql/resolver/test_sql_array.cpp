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

#include <fstream>
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#define private public
#define protected public
#include "lib/utility/utility.h"
#include "lib/container/ob_iarray.h"
#include "src/sql/resolver/ob_sql_array.h"
#undef protected
#undef private

using namespace oceanbase::sql;
using namespace oceanbase::common;

class TestSqlArray: public ::testing::Test
{
public:
  TestSqlArray() {}
  virtual ~TestSqlArray() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSqlArray);
};

TEST_F(TestSqlArray, basic_test)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);

  ASSERT_EQ(40, sizeof(a));
  ASSERT_EQ(0, a.capacity_);
  ASSERT_EQ(0, a.count_);
}

TEST_F(TestSqlArray, push_back)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);

  for (int64_t i = 0; i < 1000; i++) {
    OK(a.push_back(i));
    EXPECT_EQ(i, a.at(i));
    EXPECT_EQ(i + 1, a.count_);
  }
  EXPECT_EQ(1024, a.capacity_);
}

TEST_F(TestSqlArray, pop_back)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);
  int64_t value = 0;

  EXPECT_EQ(OB_ENTRY_NOT_EXIST, a.pop_back(value));

  for (int64_t i = 0; i < 1000; i++) {
    OK(a.push_back(i));
  }
  EXPECT_EQ(1000, a.count_);
  EXPECT_EQ(1024, a.capacity_);


  for (int64_t i = 0; i < 1000; i++) {
    OK(a.pop_back(value));
    EXPECT_EQ(999 - i, value);
    EXPECT_EQ(999 - i, a.count_);
  }
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(1024, a.capacity_);

  for (int64_t i = 0; i < 1000; i++) {
    OK(a.push_back(i));
  }
  EXPECT_EQ(1000, a.count_);
  EXPECT_EQ(1024, a.capacity_);

  for (int64_t i = 0; i < 1000; i++) {
    a.pop_back();
    EXPECT_EQ(999 - i, a.count_);
  }
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(1024, a.capacity_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, a.pop_back(value));
}

TEST_F(TestSqlArray, remove)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);

  EXPECT_EQ(OB_ARRAY_OUT_OF_RANGE, a.remove(0));
  for (int64_t i = 0; i < 10; i++) {
    OK(a.push_back(i));
  }
  EXPECT_EQ(10, a.count_);
  EXPECT_EQ(16, a.capacity_);

  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, a.remove(10));
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, a.remove(15));
  EXPECT_EQ(10, a.count_);
  EXPECT_EQ(16, a.capacity_);

  OK(a.remove(2));
  OK(a.remove(4));
  OK(a.remove(6));

  EXPECT_EQ(7, a.count_);
  EXPECT_EQ(16, a.capacity_);

  int64_t idx = 0;
  for (int64_t i = 0; i < 10; i++) {
    if (i == 2 || i == 5 || i == 8) {
      // do nothing
    } else {
      EXPECT_EQ(i, a.at(idx));
      idx++;
    }
  }
}

TEST_F(TestSqlArray, reserve)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);
  void *ptr = NULL;

  OK(a.reserve(1));
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(4, a.capacity_);
  ptr = a.data_;

  OK(a.reserve(9));
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(12, a.capacity_);
  ASSERT_NE(ptr, a.data_);
  ptr = a.data_;

  OK(a.reserve(11));
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(12, a.capacity_);
  ASSERT_EQ(ptr, a.data_);

  for (int64_t i = 0; i < 10; i++) {
    OK(a.push_back(i));
  }
  EXPECT_EQ(10, a.count_);
  EXPECT_EQ(12, a.capacity_);
  ASSERT_EQ(ptr, a.data_);

  for (int64_t i = 10; i < 21; i++) {
    OK(a.push_back(i));
  }
  EXPECT_EQ(21, a.count_);
  EXPECT_EQ(24, a.capacity_);
  ASSERT_NE(ptr, a.data_);
  ptr = a.data_;

  OK(a.reserve(100));
  EXPECT_EQ(21, a.count_);
  EXPECT_EQ(100, a.capacity_);
  ASSERT_NE(ptr, a.data_);

  for (int64_t i = 0; i < a.count_; i++) {
    EXPECT_EQ(i, a.at(i));
  }
}


TEST_F(TestSqlArray, alloc_place_holder)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);
  int64_t *ptr = NULL;
  ptr = a.alloc_place_holder();
  EXPECT_EQ(a.data_, ptr);
  *ptr = 1;
  EXPECT_EQ(1, a.count_);
  EXPECT_EQ(4, a.capacity_);

  ptr = a.alloc_place_holder();
  EXPECT_EQ(a.data_ + 1, ptr);
  *ptr = 2;
  EXPECT_EQ(2, a.count_);
  EXPECT_EQ(4, a.capacity_);

  for (int64_t i = 0; i < 6; i++) {
    OK(a.push_back(i+2));
  }
  EXPECT_EQ(8, a.count_);
  EXPECT_EQ(8, a.capacity_);

  ptr = a.alloc_place_holder();
  EXPECT_EQ(a.data_ + 8, ptr);
  EXPECT_EQ(9, a.count_);
  EXPECT_EQ(16, a.capacity_);
}

TEST_F(TestSqlArray, prepare_allocate)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);

  OK(a.prepare_allocate(10));
  EXPECT_EQ(10, a.count_);
  EXPECT_EQ(10, a.capacity_);

  OK(a.prepare_allocate(25));
  EXPECT_EQ(25, a.count_);
  EXPECT_EQ(25, a.capacity_);
  for (int64_t i = 0; i < a.count(); i++) {
    EXPECT_EQ(0, a.at(i));
  }

  OK(a.prepare_allocate(11, 999));
  EXPECT_EQ(11, a.count_);
  EXPECT_EQ(25, a.capacity_);
  for (int64_t i = 0; i < a.count(); i++) {
    EXPECT_EQ(0, a.at(i));
  }

  struct A {
    A() : a_(-1) {}
    A(int a) : a_(a) {}
    int a_;
    TO_STRING_KV(K_(a));
  };
  ObSqlArray<A> b(allocator);
  OK(b.prepare_allocate(10));
  EXPECT_EQ(10, b.count_);
  EXPECT_EQ(10, b.capacity_);
  for (int64_t i = 0; i < b.count(); i++) {
    EXPECT_EQ(-1, b.at(i).a_);
  }

  OK(b.prepare_allocate(25, 999));
  EXPECT_EQ(25, b.count_);
  EXPECT_EQ(25, b.capacity_);
  for (int64_t i = 0; i < b.count(); i++) {
    if (i < 10) {
      EXPECT_EQ(-1, b.at(i).a_);
    } else {
      EXPECT_EQ(999, b.at(i).a_);
    }
  }
}

TEST_F(TestSqlArray, reuse_and_reset)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSqlArray<int64_t> a(allocator);
  OK(a.prepare_allocate(10));
  EXPECT_EQ(10, a.count_);
  EXPECT_EQ(10, a.capacity_);

  a.reuse();
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(10, a.capacity_);
  a.reset();
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(0, a.capacity_);
}

TEST_F(TestSqlArray, construct_with_allocator)
{
  int ret = OB_SUCCESS;
  struct A {
    A(ObIAllocator &allocator) : a_(-1), allocator_(allocator) {}
    int assign(const A &other) { a_ = other.a_; return OB_SUCCESS; }
    int a_;
    ObIAllocator &allocator_;
    TO_STRING_KV(K_(a));
  };
  ObArenaAllocator allocator;
  ObSqlArray<A, true> a(allocator);
  ObSqlArray<A, true> b(allocator);
  A obj(allocator);
  ASSERT_EQ(OB_SIZE_OVERFLOW, a.push_back(obj));
  OK(a.reserve(1));
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(1, a.capacity_);
  OK(a.push_back(obj));
  EXPECT_EQ(1, a.count_);
  EXPECT_EQ(1, a.capacity_);

  ASSERT_EQ(OB_ERR_UNEXPECTED, a.prepare_allocate(2));
  ASSERT_EQ(OB_ERR_UNEXPECTED, a.prepare_allocate(10, allocator));
  a.reset();
  OK(a.prepare_allocate(2));
  EXPECT_EQ(2, a.count_);
  EXPECT_EQ(2, a.capacity_);
  a.reuse();
  OK(a.prepare_allocate(10, allocator));
  EXPECT_EQ(10, a.count_);
  EXPECT_EQ(10, a.capacity_);
  a.reset();
  OK(a.prepare_allocate(15, allocator));
  EXPECT_EQ(15, a.count_);
  EXPECT_EQ(15, a.capacity_);
  a.reuse();
  OK(a.prepare_allocate(10));
  EXPECT_EQ(10, a.count_);
  EXPECT_EQ(15, a.capacity_);
  a.reset();
  OK(a.prepare_allocate(10));
  for (int64_t i = 0; i < a.count(); i++) {
    EXPECT_EQ(-1, a.at(i).a_);
    a.at(i).a_ = i;
  }
  OK(a.pop_back(obj));
  EXPECT_EQ(9, a.count_);
  EXPECT_EQ(10, a.capacity_);
  EXPECT_EQ(9, obj.a_);

  OK(a.remove(2));
  EXPECT_EQ(8, a.count_);
  EXPECT_EQ(10, a.capacity_);
  int64_t idx = 0;
  for (int64_t i = 0; i < a.count(); i++) {
    if (i == 2) {
      // do nothing
    } else {
      EXPECT_EQ(i, a.at(idx).a_);
      idx++;
    }
  }

  A *ptr = a.alloc_place_holder();
  EXPECT_EQ(a.data_ + 8, ptr);
  ptr = a.alloc_place_holder();
  EXPECT_EQ(a.data_ + 9, ptr);
  ptr = a.alloc_place_holder();
  EXPECT_EQ(NULL, ptr);
  a.pop_back();
  a.pop_back();

  ASSERT_EQ(OB_ERR_UNEXPECTED, a.reserve(11));
  OK(a.reserve(9));
  EXPECT_EQ(8, a.count_);
  EXPECT_EQ(10, a.capacity_);
  a.reset();
  OK(a.reserve(12));
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(12, a.capacity_);

  OK(a.assign(b));
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(12, a.capacity_);
  OK(a.assign(b));
  EXPECT_EQ(0, a.count_);
  EXPECT_EQ(12, a.capacity_);
  OK(b.prepare_allocate(8));
  OK(a.assign(b));
  EXPECT_EQ(8, a.count_);
  EXPECT_EQ(12, a.capacity_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, a.assign(b));
  b.reset();
  OK(b.prepare_allocate(15));
  a.reuse();
  OK(a.assign(b));
  EXPECT_EQ(15, a.count_);
  EXPECT_EQ(15, a.capacity_);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

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

#define UNITTEST_DEBUG

#include "lib/guard/ob_shared_guard.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/guard/ob_weak_guard.h"
#include "lib/container/ob_se_array.h"
#include <gtest/gtest.h>
#include <iostream>
#include <vector>

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;

function::DefaultFunctionAllocator &default_allocator1 = function::DefaultFunctionAllocator::get_default_allocator();
guard::DefaultSharedGuardAllocator &default_allocator2 = guard::DefaultSharedGuardAllocator::get_default_allocator();
guard::DefaultUniqueGuardAllocator &default_allocator3 = guard::DefaultUniqueGuardAllocator::get_default_allocator();

struct TestObj {
  TestObj() { ++total_alive_num; }
  ~TestObj() { --total_alive_num; }
  int test() { return 123; }
  static int total_alive_num;
};

struct TestObj2 {
  TestObj2(int a, const double &b) : a_(a), b_(b) { ++total_alive_num; }
  ~TestObj2() { --total_alive_num; }
  const double &get_b() { return b_; }
  static int total_alive_num;
  int a_;
  const double &b_;
};

class TestObGuard: public ::testing::Test
{
public:
  TestObGuard() {};
  virtual ~TestObGuard() {};
  virtual void SetUp() { };
  virtual void TearDown() { 
    ASSERT_EQ(default_allocator1.total_alive_num, 0);
    ASSERT_EQ(default_allocator2.total_alive_num, 0);
    ASSERT_EQ(default_allocator3.total_alive_num, 0);
    ASSERT_EQ(TestObj::total_alive_num, 0);
    ASSERT_EQ(TestObj2::total_alive_num, 0);
  };
  template <typename T>
  guard::BlockPtr<T> &get_block_ptr(ObSharedGuard<T> &ptr) { return ptr.block_ptr_; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObGuard);
};

int TestObj::total_alive_num = 0;
int TestObj2::total_alive_num = 0;

// ObUniqueGuard

// 测试默认构造函数的行为
TEST_F(TestObGuard, ob_unqiue_guard_default_construction) {
  ObUniqueGuard<TestObj> ptr;
  ASSERT_EQ(false, ptr.is_valid());
}

// 测试普通构造函数的行为
TEST_F(TestObGuard, ob_unqiue_guard_general_construction) {
  TestObj *ptr = new TestObj();
  ObUniqueGuard<TestObj> ptr1;
  ASSERT_EQ(OB_SUCCESS, ptr1.assign(ptr, [](TestObj* ptr){ delete ptr; }));
  ASSERT_EQ(true, ptr1.is_valid());
}

// 测试拷贝构造函数的行为
TEST_F(TestObGuard, ob_unqiue_guard_copy_construction) {
  TestObj *ptr = new TestObj();
  ObUniqueGuard<TestObj> ptr1;
  ASSERT_EQ(OB_SUCCESS, ptr1.assign(ptr, [](TestObj* ptr){ delete ptr; }));
  ASSERT_EQ(true, ptr1.is_valid());
  ObUniqueGuard<TestObj> ptr2(ptr1);
  ASSERT_EQ(false, ptr1.is_valid());
  ASSERT_EQ(true, ptr2.is_valid());
  ptr2.~ObUniqueGuard();
  ASSERT_EQ(false, ptr2.is_valid());
}

// 测试赋值运算符的行为
TEST_F(TestObGuard, ob_unqiue_guard_equal) {
  TestObj *ptr = new TestObj();
  ObUniqueGuard<TestObj> ptr1;
  ASSERT_EQ(OB_SUCCESS, ptr1.assign(ptr, [](TestObj* ptr){ delete ptr; }));
  ASSERT_EQ(true, ptr1.is_valid());
  ObUniqueGuard<TestObj> ptr2;
  ASSERT_EQ(true, ptr1.is_valid());
  ASSERT_EQ(false, ptr2.is_valid());
  ptr2 = ptr1;
  ASSERT_EQ(false, ptr1.is_valid());
  ASSERT_EQ(true, ptr2.is_valid());
  //ptr2 = ptr2;
  ASSERT_EQ(false, ptr1.is_valid());
  ASSERT_EQ(true, ptr2.is_valid());
  //ptr1 = ptr1;
  ASSERT_EQ(false, ptr1.is_valid());
  ptr1 = ptr2;
  ASSERT_EQ(true, ptr1.is_valid());
  ASSERT_EQ(false, ptr2.is_valid());
  ptr1.~ObUniqueGuard();
  ASSERT_EQ(false, ptr1.is_valid());
}

// 测试assgin的行为
TEST_F(TestObGuard, ob_unqiue_guard_assign) {
  ObUniqueGuard<TestObj> ptr1;
  TestObj *ptr = new TestObj();
  ASSERT_EQ(OB_SUCCESS, ptr1.assign(ptr, [](TestObj* ptr){ delete ptr; }));
  ASSERT_EQ(true, ptr1.is_valid());
  ASSERT_EQ(ptr, ptr1.get_ptr());
  ObUniqueGuard<TestObj> ptr2;
  ASSERT_EQ(OB_SUCCESS, ptr2.assign(ptr1));
  ASSERT_EQ(false, ptr1.is_valid());
  ASSERT_EQ(true, ptr2.is_valid());
  ObUniqueGuard<TestObj> ptr3;
  ASSERT_EQ(OB_SUCCESS, ptr3.assign(new TestObj(), [](TestObj* ptr){ delete ptr; }));
  ASSERT_EQ(OB_SUCCESS, ptr3.assign(ptr2));
  ASSERT_EQ(false, ptr1.is_valid());
  ASSERT_EQ(false, ptr2.is_valid());
  ASSERT_EQ(true, ptr3.is_valid());
}

// 测试reset的行为
TEST_F(TestObGuard, ob_unqiue_guard_reset) {
  ObUniqueGuard<TestObj> ptr1;
  ASSERT_EQ(OB_SUCCESS, ptr1.assign(new TestObj(), [](TestObj* ptr){ delete ptr; }));
  ASSERT_EQ(true, ptr1.is_valid());
  ASSERT_NE(nullptr, ptr1.get_ptr());
  ptr1.reset();
  ASSERT_EQ(false, ptr1.is_valid());
  ASSERT_EQ(nullptr, ptr1.get_ptr());
}

// 测试指针的使用行为
TEST_F(TestObGuard, ob_unqiue_guard_use) {
  ObUniqueGuard<TestObj> ptr1;
  ASSERT_EQ(OB_SUCCESS, ptr1.assign(new TestObj(), [](TestObj* ptr){ delete ptr; }));
  ASSERT_EQ(123, ptr1->test());
  ASSERT_EQ(123, (*ptr1).test());
}

// 测试make函数
TEST_F(TestObGuard, ob_unqiue_guard_make) {
  ObUniqueGuard<TestObj> ptr1;
  ASSERT_EQ(OB_SUCCESS, ob_make_unique<TestObj>(ptr1));
  auto temp_ptr = ptr1.get_ptr();
  ObNullAllocator bad_allocator;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ob_alloc_unique<TestObj>(ptr1, bad_allocator));
  ASSERT_EQ(temp_ptr, ptr1.get_ptr());
  ASSERT_EQ(OB_SUCCESS, ob_make_unique<TestObj>(ptr1));// new ptr
  ASSERT_NE(temp_ptr, ptr1.get_ptr());

  ObUniqueGuard<TestObj2> ptr2;
  double arg = 1.0;
  ASSERT_EQ(OB_SUCCESS, ob_make_unique<TestObj2>(ptr2, 1, arg));
  ASSERT_EQ(&arg, &(ptr2.get_ptr()->get_b()));
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ob_alloc_unique<TestObj2>(ptr2, bad_allocator, 1, arg));
  ASSERT_EQ(&arg, &(ptr2.get_ptr()->get_b()));
}

// ObSharedGuard

// 测试默认构造函数
TEST_F(TestObGuard, ob_shared_guard_default_construction) {
  ObSharedGuard<TestObj> shared_ptr;
  ASSERT_EQ(false, shared_ptr.is_valid());
}

// 测试assign
TEST_F(TestObGuard, ob_shared_guard_assign) {
  ObNullAllocator bad_allocator;
  auto ptr = new TestObj();
  ObSharedGuard<TestObj> shared_ptr;
  ASSERT_EQ(OB_SUCCESS, shared_ptr.assign(ptr, [](TestObj* ptr){ delete ptr; }));// assign ObSharedGuard
  ASSERT_EQ(ptr, get_block_ptr(shared_ptr).data_ptr_);
  ASSERT_EQ(true, shared_ptr.is_valid());
  ObSharedGuard<TestObj> shared_ptr2;
  auto ptr2 = new TestObj();
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, shared_ptr.assign(ptr2, [](TestObj* ptr){ delete ptr; }, bad_allocator));// assign failed cause bad alloc
  delete ptr2;
  ObSharedGuard<TestObj> shared_ptr3;
  ASSERT_EQ(OB_SUCCESS, shared_ptr3.assign(shared_ptr));
  ASSERT_EQ(ptr, get_block_ptr(shared_ptr3).data_ptr_);
  ObSharedGuard<TestObj> shared_ptr4;
  ASSERT_EQ(true, (shared_ptr4 = shared_ptr3).is_valid());
  ASSERT_EQ(ptr, get_block_ptr(shared_ptr4).data_ptr_);
}

// 测试拷贝构造函数
TEST_F(TestObGuard, ob_shared_guard_copy_construction) {
  ObSharedGuard<TestObj> shared_ptr1;
  ASSERT_EQ(OB_SUCCESS, shared_ptr1.assign(new TestObj(), [](TestObj* ptr){ delete ptr; }));
  {
    ObSharedGuard<TestObj> shared_ptr2(shared_ptr1);
    ASSERT_EQ(TestObj::total_alive_num, 1);
  }
  ASSERT_EQ(TestObj::total_alive_num, 1);
}

// 测试拷贝赋值运算符
TEST_F(TestObGuard, ob_shared_guard_equal) {
  ObSharedGuard<TestObj> shared_ptr1;
  ASSERT_EQ(OB_SUCCESS, shared_ptr1.assign(new TestObj(), [](TestObj* ptr){ delete ptr; }));
  {
    ObSharedGuard<TestObj> shared_ptr2;
    shared_ptr2 = shared_ptr1;
    ASSERT_EQ(TestObj::total_alive_num, 1);
  }
  ObSharedGuard<TestObj> shared_ptr3;
  shared_ptr1 = shared_ptr3;
  ASSERT_EQ(TestObj::total_alive_num, 0);
}

struct BigObj {
  char data[4096];
};

// 测试unique_gaurd到shared_guard的转化
TEST_F(TestObGuard, ob_shared_guard_from_uique_guard) {
  BigObj big;
  ObNullAllocator bad_allocator;
  ObUniqueGuard<TestObj> uniq_ptr;
  ObSharedGuard<TestObj> shared_ptr;
  // invalid unique -> shared
  ASSERT_EQ(OB_INVALID_ARGUMENT, shared_ptr.assign(uniq_ptr));
  auto ptr = new TestObj();
  // make a valid shared
  ASSERT_EQ(OB_SUCCESS, shared_ptr.assign(ptr, [](TestObj *ptr){ delete ptr; }));
  ASSERT_EQ(ptr, shared_ptr.get_ptr());
  auto ptr2 = new TestObj();
  // assign shared with bad allocator
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, shared_ptr.assign(ptr2, [](TestObj *ptr){ delete ptr; }, bad_allocator));
  delete ptr2;
  ASSERT_EQ(ptr, shared_ptr.get_ptr());// 强异常安全保证
  auto ptr3 = new TestObj();
  // make a valid unique
  ASSERT_EQ(OB_SUCCESS, uniq_ptr.assign(ptr3, [big](TestObj *ptr){ UNUSED(big); delete ptr; }));
  // valid unique -> valid shared, with bad allocator
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, shared_ptr.assign(uniq_ptr, bad_allocator));
  ASSERT_EQ(ptr, shared_ptr.get_ptr());// 强异常安全保证
  ASSERT_EQ(true, uniq_ptr.is_valid());
  // valid unique -> valid shared
  ASSERT_EQ(OB_SUCCESS, shared_ptr.assign(uniq_ptr));
  ASSERT_EQ(false, uniq_ptr.is_valid());
  ASSERT_EQ(ptr3, shared_ptr.get_ptr());
  // make a maked unique
  ASSERT_EQ(OB_SUCCESS, ob_make_unique<TestObj>(uniq_ptr));
  ASSERT_EQ(true, uniq_ptr.is_valid());
  // maked unique -> valid shared
  ASSERT_EQ(OB_SUCCESS, shared_ptr.assign(uniq_ptr));
  ASSERT_NE(ptr3, shared_ptr.get_ptr());
  ASSERT_EQ(false, uniq_ptr.is_valid());
  shared_ptr.~ObSharedGuard();
}

// 测试make
TEST_F(TestObGuard, ob_shared_guard_make_and_alloc) {
  ObNullAllocator bad_allocator;
  double a = 1.0;
  ObSharedGuard<TestObj2> shared_ptr;
  ASSERT_EQ(OB_SUCCESS, ob_make_shared<TestObj2>(shared_ptr, 1, a));
  ASSERT_EQ(&a, &shared_ptr->get_b());
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ob_alloc_shared<TestObj2>(shared_ptr, bad_allocator));
  auto ptr = shared_ptr.get_ptr();
  ASSERT_EQ(&a, &shared_ptr->get_b());// 强异常安全保证
  ASSERT_EQ(OB_SUCCESS, ob_alloc_shared<TestObj2>(shared_ptr, default_allocator1));
  new(shared_ptr.get_ptr()) TestObj2(1, a);
  ASSERT_NE(ptr, shared_ptr.get_ptr());
}

// ObWeakGuard

// 默认构造函数
TEST_F(TestObGuard, ob_weak_guard_default_construction) {
  ObWeakGuard<TestObj> weak_gaurd;
  ASSERT_EQ(false, weak_gaurd.is_valid());
}

// 升级构造函数
TEST_F(TestObGuard, ob_weak_guard_upgrade_construction) {
  ObSharedGuard<TestObj> shared_ptr;
  ASSERT_EQ(OB_SUCCESS, ob_make_shared<TestObj>(shared_ptr));
  ObWeakGuard<TestObj> weak_gaurd(shared_ptr);
  ASSERT_EQ(true, weak_gaurd.is_valid());
  ASSERT_EQ(shared_ptr.get_ptr(), weak_gaurd.upgrade().get_ptr());
}

// 拷贝构造函数
TEST_F(TestObGuard, ob_weak_guard_copy_construction) {
  ObSharedGuard<TestObj> shared_ptr;
  ASSERT_EQ(OB_SUCCESS, ob_make_shared<TestObj>(shared_ptr));
  ObWeakGuard<TestObj> weak_gaurd(shared_ptr);
  ASSERT_EQ(true, weak_gaurd.is_valid());
  ASSERT_EQ(shared_ptr.get_ptr(), weak_gaurd.upgrade().get_ptr());
  ObWeakGuard<TestObj> weak_gaurd2(weak_gaurd);
  ASSERT_EQ(shared_ptr.get_ptr(), weak_gaurd2.upgrade().get_ptr());
  weak_gaurd2.reset();
  ObWeakGuard<TestObj> weak_gaurd3(weak_gaurd2);
  ASSERT_EQ(false, weak_gaurd2.is_valid());
  ASSERT_EQ(false, weak_gaurd3.is_valid());
}

// 拷贝赋值运算符
TEST_F(TestObGuard, ob_weak_guard_copy_equal) {
  ObSharedGuard<TestObj> shared_ptr;
  ASSERT_EQ(OB_SUCCESS, ob_make_shared<TestObj>(shared_ptr));
  ObWeakGuard<TestObj> weak_gaurd = shared_ptr;// 隐式构造
  ObWeakGuard<TestObj> weak_gaurd2;
  weak_gaurd2 = shared_ptr;// 隐式构造后赋值
  weak_gaurd2 = weak_gaurd;// 同类型赋值，无隐式构造
  ObWeakGuard<TestObj> weak_gaurd3;
  ASSERT_EQ(OB_SUCCESS, weak_gaurd.assign(weak_gaurd3));
  ASSERT_EQ(OB_SUCCESS, weak_gaurd.assign(shared_ptr));
}

// lock operation
TEST_F(TestObGuard, ob_weak_guard_lock) {
  {
    ObWeakGuard<TestObj> weak_gaurd;
    ASSERT_EQ(false, weak_gaurd.upgrade().is_valid());
    {
      ObSharedGuard<TestObj> shared_ptr;
      ASSERT_EQ(OB_SUCCESS, ob_make_shared<TestObj>(shared_ptr));
      weak_gaurd = shared_ptr;
    }// shared ptr release
    ASSERT_EQ(1, default_allocator2.total_alive_num);
    ObSharedGuard<TestObj> shared_ptr2 = weak_gaurd.upgrade();
    ASSERT_EQ(false, shared_ptr2.is_valid());
    ObWeakGuard<TestObj> weak_gaurd2 = weak_gaurd;
    ASSERT_EQ(1, default_allocator2.total_alive_num);
    weak_gaurd2.~ObWeakGuard();
    weak_gaurd.~ObWeakGuard();
    ASSERT_EQ(0, default_allocator2.total_alive_num);
  }
  {
    ObWeakGuard<TestObj> weak_gaurd;
    {
      ObSharedGuard<TestObj> shared_ptr;
      ASSERT_EQ(OB_SUCCESS, shared_ptr.assign(new TestObj(), [](TestObj*ptr) { delete ptr; }));
      weak_gaurd = shared_ptr;
    }// shared ptr release
    ASSERT_EQ(1, default_allocator2.total_alive_num);
    ObSharedGuard<TestObj> shared_ptr2 = weak_gaurd.upgrade();
    ASSERT_EQ(false, shared_ptr2.is_valid());
    ObWeakGuard<TestObj> weak_gaurd2 = weak_gaurd;
    ASSERT_EQ(1, default_allocator2.total_alive_num);
    weak_gaurd2.~ObWeakGuard();
    weak_gaurd.~ObWeakGuard();
    ASSERT_EQ(0, default_allocator2.total_alive_num);
  }
}

}// namespace oceanbase
}// namespace unittest

int main(int argc, char **argv)
{
  system("rm -rf test_ob_guard.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_guard.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
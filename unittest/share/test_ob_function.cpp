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

#include "lib/function/ob_function.h"
#include "lib/container/ob_array.h"
#include <gtest/gtest.h>
#include <iostream>
#include <vector>

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;

function::DebugRecorder &recorder = function::DebugRecorder::get_instance();
function::DefaultFunctionAllocator &default_allocator = function::DefaultFunctionAllocator::get_default_allocator();

class TestObFunction: public ::testing::Test
{
public:
  TestObFunction() {};
  virtual ~TestObFunction() {};
  virtual void SetUp() { recorder.reset(); };
  virtual void TearDown() { ASSERT_EQ(default_allocator.total_alive_num, 0); };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObFunction);
};

// 以下为C++中目前(截止到C++20标准)支持的所有可能的调用形式
// 1，普通函数
int func1(double arg1, const float &arg2) {
  return (arg1 + arg2);
}

// 2，裸函数指针
int (*func2)(double, const float &) = func1;

// 3, 类成员函数和静态成员函数
struct Example {
  // 3.1 类成员函数
  int func3(double arg1, const float &arg2) {
    return (arg1 + arg2);
  }
  // 3.2 静态成员函数
  static int func4(double arg1, const float &arg2) {
    return (arg1 + arg2);
  }
};

// 4, 仿函数
class Func5 {
public:
  int operator()(double arg1, const float &arg2) {
    return (arg1 + arg2) * ratio;
  }
private:
  static constexpr int ratio = 1;// 成员状态将影响调用过程
} func5;

// 5, lambda表达式
auto func6 = [](double arg1, const float &arg2) -> int {
  return (arg1 + arg2);
};

TEST_F(TestObFunction, lvalue_implicit_construction) {
  ObArray<ObFunction<int(double, const float&)>> func_array;
  ASSERT_EQ(OB_SUCCESS, func_array.push_back(func1));
  ASSERT_EQ(OB_SUCCESS, func_array.push_back(func2));
  ASSERT_EQ(OB_SUCCESS, func_array.push_back(&Example::func4));
  ASSERT_EQ(OB_SUCCESS, func_array.push_back(func5));
  ASSERT_EQ(OB_SUCCESS, func_array.push_back(func6));

  float temp = 1.0f;
  for (int64_t idx = 0; idx < func_array.count(); ++idx) {
    ObFunction<int(double, const float&)> func = func_array.at(idx);
    ASSERT_EQ(3, func(2.0, temp));
  }
}

TEST_F(TestObFunction, rvalue_implicit_construction) {
  ObArray<ObFunction<int(double, const float&)>> func_array;
  ASSERT_EQ(OB_SUCCESS, func_array.push_back([](double a, const float& b) -> int {
    return a + b;
  }));

  auto create_rvalue_function = []() { return Func5(); };
  ASSERT_EQ(OB_SUCCESS, func_array.push_back(create_rvalue_function()));

  float temp = 1.0f;
  for (int64_t idx = 0; idx < func_array.count(); ++idx) {
    ObFunction<int(double, const float&)> func = func_array.at(idx);
    ASSERT_EQ(3, func(2.0, temp));
  }
}

TEST_F(TestObFunction, perfect_forwarding) {
  float temp = 1.0f;
  ObFunction<float*(float &)> func = [](float &a) -> float* { return &a; };
  ASSERT_EQ(&temp, func(temp));
}

TEST_F(TestObFunction, construction) {
  ObFunction<int()> f1 = [](){ return 1; };
  ObFunction<int()> f2 = f1;
  ObFunction<int()> f3((ObFunction<int()>(f1)));
}

class TestAllocator1 : public ObIAllocator {
public:
  void *alloc(int64_t size) override {
    UNUSED(size);
    return nullptr;
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override {
    UNUSEDx(size, attr);
    return nullptr;
  }
  void free(void *ptr) override {
    UNUSED(ptr);
  }
} test_allocator1;

class TestAllocator2 : public ObIAllocator {
public:
  void *alloc(int64_t size) override {
    return ob_malloc(size, "");
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override {
    UNUSED(ptr);
  }
} test_allocator2;

TEST_F(TestObFunction, alloc) {
  float temp = 1.0f;
  // implicit construct then move construct
  ObFunction<float*(float &)> f1 = [](float &a) -> float* { return &a; };
  ASSERT_EQ(true, f1.is_valid());
  ASSERT_EQ(&temp, f1(temp));
  ObFunction<float*(float &)> f2([](float &a) -> float* { return &a; }, test_allocator1);
  ASSERT_EQ(true, f2.is_valid());// alloc invalid, but not used
  ObFunction<float*(float &)> f3(test_allocator2);
  f3 = f1;
  ASSERT_EQ(true, f3.is_valid());
  ObFunction<float*(float &)> f4(test_allocator1);
  f4 = std::move(f1);
  ASSERT_EQ(true, f4.is_valid());
}

TEST_F(TestObFunction, standard_style) {
  ObFunction<void(int)> f = [](int) {};
  if (f.is_valid()) {// 检查构造是否成功
    f(0);
  }
}

TEST_F(TestObFunction, ob_style) {
  ObFunction<void(int)> f;
  if (OB_SUCCESS == f.assign([](int){})) {// 检查赋值是否成功
    f(0);
  }
}

#define JUDGE_RECORDER(v1,v2,v3,v4,v5,v6,v7,v8,v9)\
ASSERT_EQ(recorder.function_default_construct_time, v1);\
ASSERT_EQ(recorder.function_copy_construct_time, v2);\
ASSERT_EQ(recorder.function_move_construct_time, v3);\
ASSERT_EQ(recorder.function_general_construct_time, v4);\
ASSERT_EQ(recorder.function_copy_equal_time, v5);\
ASSERT_EQ(recorder.function_move_equal_time, v6);\
ASSERT_EQ(recorder.function_general_equal_time, v7);\
ASSERT_EQ(recorder.derived_construct_time, v8);\
ASSERT_EQ(recorder.function_base_assign_time, v9);\
ASSERT_EQ(recorder.function_copy_assign_time, v2 + v5);\
ASSERT_EQ(recorder.function_move_assign_time, v3 + v6);\
ASSERT_EQ(recorder.function_general_assign_time, v4 + v7);\
recorder.reset();

TEST_F(TestObFunction, construct_path) {
  // implicit general construction
  ObFunction<int(double, const float&)> f1 = func1;
  JUDGE_RECORDER(0,0,0,1,0,0,0,1,0);
  // default construction and general assign
  ObFunction<int(double, const float&)> f2;
  f2 = func2;
  JUDGE_RECORDER(1,0,0,0,0,0,1,1,0);
  // just general assign
  f2 = Func5();
  JUDGE_RECORDER(0,0,0,0,0,0,1,1,0);
  // copy assign
  f2 = f1;
  JUDGE_RECORDER(0,0,0,0,1,0,0,1,1);
  // move assign
  f2 = std::move(f1);
  JUDGE_RECORDER(0,0,0,0,0,1,0,1,1);
  // copy construction
  ObFunction<int(double, const float&)> f3 = f2;
  JUDGE_RECORDER(0,1,0,0,0,0,0,1,1);
  // move construction
  ObFunction<int(double, const float&)> f4 = std::move(f3);
  JUDGE_RECORDER(0,0,1,0,0,0,0,1,1);
}

struct BigObj {
  unsigned char val[41];
} ;

struct SmallObj {
  unsigned char val[40];
};

// rvalue: small = big
//         big = small
//         small = small
//         big = big
// lvalue: small = big
//         big = small
//         small = small
//         big = big
TEST_F(TestObFunction, big_and_small_obj) {
  BigObj big;
  big.val[0] = 0;
  SmallObj small;
  ObFunction<int(void)> f1 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f2 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f3 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f4 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f5 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f6 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f7 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f8 = [big](){ return int(big.val[0]); };// big obj
  ObFunction<int(void)> f9 = [small](){ return int(small.val[0]); };// small obj
  ObFunction<int(void)> f10 = [small](){ return int(small.val[0]); };// small obj
  ObFunction<int(void)> f11 = [small](){ return int(small.val[0]); };// small obj
  ObFunction<int(void)> f12 = [small](){ return int(small.val[0]); };// small obj
  ObFunction<int(void)> f13 = [small](){ return int(small.val[0]); };// small obj
  ObFunction<int(void)> f14 = [small](){ return int(small.val[0]); };// small obj
  ObFunction<int(void)> f15 = [small](){ return int(small.val[0]); };// small obj
  ObFunction<int(void)> f16 = [small](){ return int(small.val[0]); };// small obj

  recorder.reset();
  // rvalue equal
  f9 = std::move(f1);// small = big
  JUDGE_RECORDER(0,0,0,0,0,1,0,1,1);
  f2 = std::move(f10);// big = small
  JUDGE_RECORDER(0,0,0,0,0,1,0,1,1);
  f3 = std::move(f4);// big = big
  JUDGE_RECORDER(0,0,0,0,0,1,0,0,0);
  f11 = std::move(f12);// small = small
  JUDGE_RECORDER(0,0,0,0,0,1,0,1,1);
  // lvalue equal
  f13 = f5;// small = big
  JUDGE_RECORDER(0,0,0,0,1,0,0,1,1);
  f6 = f14;// big = small
  JUDGE_RECORDER(0,0,0,0,1,0,0,1,1);
  f7 = f8;// big = big
  JUDGE_RECORDER(0,0,0,0,1,0,0,1,1);
  f15 = f16;// small = small
  JUDGE_RECORDER(0,0,0,0,1,0,0,1,1);
  // rvalue construct
  ObFunction<int(void)> f17 = std::move(f9);//construct big
  JUDGE_RECORDER(0,0,1,0,0,0,0,0,0);
  ObFunction<int(void)> f18 = std::move(f2);//construct small
  JUDGE_RECORDER(0,0,1,0,0,0,0,1,1);
  // lvalue construct
  ObFunction<int(void)> f19 = f17;
  JUDGE_RECORDER(0,1,0,0,0,0,0,1,1);
  ObFunction<int(void)> f20 = f18;
  JUDGE_RECORDER(0,1,0,0,0,0,0,1,1);

}

struct TestContructDestruct
{
  TestContructDestruct(int64_t &count) : count_(count) { ++count_; }
  TestContructDestruct(const TestContructDestruct &rhs) : count_(rhs.count_) { ++count_; }
  ~TestContructDestruct() { --count_; }
  void operator()() {}
  int64_t &count_;
};
TEST_F(TestObFunction, test_construct_destruct)
{
  int64_t count = 0;
  TestContructDestruct test(count);
  ASSERT_EQ(count, 1);
  {
    ObFunction<void()> f = test;
    ASSERT_EQ(count, 2);
  }
  ASSERT_EQ(count, 1);
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_function.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_function.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
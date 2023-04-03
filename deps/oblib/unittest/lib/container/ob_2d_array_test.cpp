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

#include "lib/container/ob_2d_array.h"
#include "lib/objectpool/ob_pool.h"
#include "lib/utility/ob_test_util.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class Ob2DArrayTest: public ::testing::Test
{
  public:
    Ob2DArrayTest();
    virtual ~Ob2DArrayTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    Ob2DArrayTest(const Ob2DArrayTest &other);
    Ob2DArrayTest& operator=(const Ob2DArrayTest &other);
  protected:
    // data members
};

Ob2DArrayTest::Ob2DArrayTest()
{
}

Ob2DArrayTest::~Ob2DArrayTest()
{
}

void Ob2DArrayTest::SetUp()
{
}

void Ob2DArrayTest::TearDown()
{
}

TEST_F(Ob2DArrayTest, basic_test)
{
  const int64_t block_size = sizeof(int) * 1024;
  Ob2DArray<int, block_size> arr;
  Ob2DArray<int, block_size> arr2;
  _OB_LOG(INFO, "sizeof(2darray)=%ld", sizeof(arr));
  const int N = 1024*32+1;
  for (int round = 0; round < 10; ++round)
  {
    ASSERT_EQ(0, arr.count());
    for (int i = 0; i < N; ++i)
    {
      ASSERT_EQ(OB_SUCCESS, arr.push_back(i));
    }
    ASSERT_EQ(N, arr.count());
    for (int i = 0; i < N; ++i)
    {
      ASSERT_EQ(i, arr.at(i));
    }
    ASSERT_EQ(1024*33, arr.get_capacity());
    // test copy
    ASSERT_EQ(OB_SUCCESS, arr2.assign(arr));
    for (int i = 0; i < N; ++i)
    {
      ASSERT_EQ(i, arr2.at(i));
    }

    ASSERT_EQ(OB_SUCCESS, arr.reserve(2 * (N-1)));
    ASSERT_EQ(1024*32*2, arr.get_capacity());
    //ASSERT_EQ(1024*32*2, arr.count());

    arr.reset();

    ASSERT_EQ(OB_SUCCESS, arr.reserve((N-1)));
    ASSERT_EQ(1024*32, arr.get_capacity());
    //ASSERT_EQ(1024*32, arr.count());

  }
  _OB_LOG(INFO, "done");
}

// TEST_F(Ob2DArrayTest, 2DSEArray_test)
// {
//   typedef ObSEArray<int32_t, 4,
//                     ObWrapperAllocator,
//                     false,
//                     ObArrayDefaultCallBack<int32_t>,
//                     NotImplementItemEncode<int32_t>,
//                     Ob2DArray<int32_t,
//                               ObWrapperAllocator,
//                               false,
//                               ObSEArray<char*, 16> > > Se2DArray;
//   const int64_t block_size = sizeof(int) * 1024;
//   ObArenaAllocator block_allocator(1);
//   Se2DArray arr(block_size, ObWrapperAllocator(&block_allocator));
//   Se2DArray arr2(block_size, ObWrapperAllocator(&block_allocator));
//   _OB_LOG(INFO, "sizeof(2darray)=%ld", sizeof(arr));
//   const int N = 16*1024*32+1;
//   for (int round = 0; round < 10; ++round)
//   {
//     ASSERT_EQ(0, arr.count());
//     for (int i = 0; i < N; ++i)
//     {
//       ASSERT_EQ(OB_SUCCESS, arr.push_back(i));
//     }
//     ASSERT_EQ(N, arr.count());
//     for (int i = 0; i < N; ++i)
//     {
//       ASSERT_EQ(i, arr.at(i));
//     }
//     //ASSERT_EQ(1024*33, arr.get_capacity());
//     // test copy
//     ASSERT_EQ(OB_SUCCESS, arr2.assign(arr));
//     arr.reset();
//     for (int i = 0; i < N; ++i)
//     {
//       ASSERT_EQ(i, arr2.at(i));
//     }
//     // test copy constructor
//     ObSEArray<int, 16> arr3;
//     ASSERT_EQ(OB_SUCCESS, arr3.assign(arr2));
//     for (int i = 0; i < N; ++i)
//     {
//       ASSERT_EQ(i, arr3.at(i));
//     }
//     // pop back
//     int j = 0;
//     for (int i = 0; i < N; ++i)
//     {
//       ASSERT_EQ(OB_SUCCESS, arr3.pop_back(j));
//       ASSERT_EQ(N-i-1, j);
//     }
//   }
//   _OB_LOG(INFO, "done2");
// }

TEST_F(Ob2DArrayTest, remove)
{
  Ob2DArray<int64_t> arr;
  OK(arr.push_back(1));
  OK(arr.push_back(2));
  OK(arr.push_back(3));
  ASSERT_EQ(3, arr.count());
  ASSERT_EQ(OB_ARRAY_OUT_OF_RANGE, arr.remove(4));
  OK(arr.remove(1));
  ASSERT_EQ(2, arr.count());
  ASSERT_EQ(1, arr.at(0));
  ASSERT_EQ(3, arr.at(1));
}

#if 0
TEST_F(Ob2DArrayTest, swap)
{
  const int64_t block_size = sizeof(int) * 1024;
  Ob2DArray<int, block_size> arr1;
  Ob2DArray<int, block_size> arr2;
  const int len1 = 1024*32+1;
  const int len2 = 1024+1;
  for (int round = 0; round < 10; ++round)
  {
    ASSERT_EQ(0, arr1.count());
    ASSERT_EQ(0, arr2.count());

    for (int i = 0; i < len1; ++i)
    {
      ASSERT_EQ(OB_SUCCESS, arr1.push_back(i));
    }
    ASSERT_EQ(len1, arr1.count());

    for (int i = 0; i < len2; ++i)
    {
      ASSERT_EQ(OB_SUCCESS, arr2.push_back(-i));
    }
    ASSERT_EQ(len2, arr2.count());

    for (int i = 0; i < len1; ++i)
    {
      ASSERT_EQ(i, arr1.at(i));
    }
    for (int i = 0; i < len2; ++i)
    {
      ASSERT_EQ(-i, arr2.at(i));
    }

    OK(arr1.swap(arr2));

    ASSERT_EQ(len1, arr2.count());
    ASSERT_EQ(len2, arr1.count());

    for (int i = 0; i < len1; ++i)
    {
      ASSERT_EQ(i, arr2.at(i));
    }
    for (int i = 0; i < len2; ++i)
    {
      ASSERT_EQ(-i, arr1.at(i));
    }

    arr1.reset();
    arr2.reset();
  }
  _OB_LOG(INFO, "done");
}
#endif

// performance_test parameters
using obj_type = int*;
const int array_size = 100000;
const int access_index_num = 100000 * 500;
const int access_run = 1;
const int ptr_array_capacity = 1000;
const int block_size = OB_MALLOC_BIG_BLOCK_SIZE;
using Tested2DArray = Ob2DArray<obj_type, block_size,
                                ModulePageAllocator,
                                false,
                                ObSEArray<obj_type *, ptr_array_capacity,
                                          ModulePageAllocator, false>>;

#include <stdio.h>      /* printf, scanf, NULL */
#include <stdlib.h>     /* malloc, free, rand */
#include <chrono>
#include <deque>
using namespace std;

#if 0
TEST_F(Ob2DArrayTest, performance_test)
{
  int* indices = static_cast<int*>(malloc(sizeof(int) * access_index_num));
  ASSERT_NE(indices, nullptr);
  for (int i=0; i<access_index_num; i++)
    indices[i]=rand() % array_size;

  decltype(chrono::steady_clock::now()) time_start, time_end;

  long sum = 0;

  OB_LOG(WARN, "parameters:",
         K(sizeof(obj_type)),
         K(array_size), K(access_index_num), K(access_run),
         K(ptr_array_capacity), K(block_size));

  obj_type* c_array = static_cast<obj_type*>(malloc(sizeof(obj_type) * array_size));
  ASSERT_NE(c_array, nullptr);

  time_start = chrono::steady_clock::now();
  for(int i=0; i<access_run; i++){
    for(int j=0; j<access_index_num; j++){
      int index = indices[j];
      sum += (long) c_array[index];
    }
  }
  time_end = chrono::steady_clock::now();
  _OB_LOG(WARN, "%s time: %ld ms", "c_array",
          chrono::duration_cast<chrono::milliseconds>(time_end - time_start).count());

  Tested2DArray ob_2d_array;
  for(int i=0; i<array_size; i++)
    ob_2d_array.push_back(0);
  ASSERT_EQ(array_size, ob_2d_array.count());

  time_start = chrono::steady_clock::now();
  for(int i=0; i<access_run; i++){
    for(int j=0; j<access_index_num; j++){
      int index = indices[j];
      sum += (long) ob_2d_array.at(index);
    }
  }
  time_end = chrono::steady_clock::now();
  _OB_LOG(WARN, "%s time: %ld ms", "ob_array (div to bit shift)",
          chrono::duration_cast<chrono::milliseconds>(time_end - time_start).count());

  deque<obj_type> cpp_deque(array_size, 0);

  time_start = chrono::steady_clock::now();
  for(int i=0; i<access_run; i++){
    for(int j=0; j<access_index_num; j++){
      int index = indices[j];
      sum += (long) cpp_deque[index];
    }
  }
  time_end = chrono::steady_clock::now();
  _OB_LOG(WARN, "%s time: %ld ms", "cpp_deque",
          chrono::duration_cast<chrono::milliseconds>(time_end - time_start).count());

  for(int i=0; i<array_size; i++){
    ASSERT_EQ(c_array[i], ob_2d_array.at(i));
    ASSERT_EQ(c_array[i], cpp_deque[i]);
  }

  OB_LOG(WARN, "sum:", K(sum));
  _OB_LOG(INFO, "done");
}
#endif

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

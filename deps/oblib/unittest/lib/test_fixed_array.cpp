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
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{
class ObTestFiexedArray : public ::testing::Test
{
public:
  ObTestFiexedArray() {}
  ~ObTestFiexedArray() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObTestFiexedArray);
};
class Testfun
{
public:
  Testfun(ObIArray<int> &array)
  {
    UNUSED(array);
  }
  ~Testfun() {}
private:
  common::ObSEArray<int, 5> data_;
};

TEST(ObTestFiexedArray, push_back)
{
  // do init only once
  int64_t capacity = 100;
  ObMalloc alloc;
  ObFixedArray<ObObj, ObMalloc> obj_array2(alloc, capacity);
  ObObj tmp2;
  EXPECT_EQ(OB_SUCCESS, obj_array2.push_back(tmp2));
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array2.reserve(200));
  EXPECT_EQ(OB_SUCCESS, obj_array2.push_back(tmp2));
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array2.reserve(200));
  EXPECT_EQ(2, obj_array2.count());

  ObFixedArray<ObObj> obj_array(alloc);
  const int64_t N = 3000;
  ObObj tmp;
  ObArray<int> tmp_array;
  Testfun testfun(tmp_array);
  EXPECT_EQ(OB_NOT_INIT, obj_array.push_back(tmp));
  EXPECT_EQ(OB_SUCCESS, obj_array.reserve(N));
  EXPECT_EQ(OB_SUCCESS, obj_array.reserve(300));
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array.reserve(N+N));

  for (int64_t i = 0; i < N/2; i++) {
    tmp.set_int(i);
    EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  }
  tmp.set_int(N/2);
  EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  for (int64_t i = 0; i <= N/2; i++) {
    ObObj result;
    EXPECT_EQ(OB_SUCCESS, obj_array.pop_back(result));
    EXPECT_EQ(N/2 - i, result.get_int());
  }
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, obj_array.pop_back(tmp));
  EXPECT_EQ(OB_SUCCESS, obj_array.prepare_allocate(N/4));
  obj_array.at(N/4 - 3).set_int(100);
  EXPECT_EQ(obj_array.at(N/4 - 3).get_int(), 100);
  //obj_array.at(N/4 + 3).set_int(100);
  //EXPECT_EQ(obj_array.at(N/4 + 3).get_int(), 100);
  EXPECT_EQ(OB_SUCCESS, obj_array.prepare_allocate(N/2 + 100));
  ObObj null_obj;
  obj_array.at(1504).set_int(100);
  EXPECT_EQ(obj_array.at(N/2 + 4).get_int(), 100);
  EXPECT_EQ(OB_ARRAY_OUT_OF_RANGE, obj_array.at(N/2 + 120, tmp));

  EXPECT_EQ(OB_SUCCESS, obj_array.prepare_allocate(N/2 + 200));
  EXPECT_EQ(OB_SUCCESS, obj_array.at(N/2 + 120, tmp));
  EXPECT_EQ(null_obj, tmp);
}

TEST(ObTestFiexedArray, destory)
{
  ObArenaAllocator allocator;
  ObFixedArray<ObObj, ObArenaAllocator > obj_array(allocator);
  const int64_t N = 3000;
  ObObj tmp;
  EXPECT_EQ(OB_NOT_INIT, obj_array.push_back(tmp));
  obj_array.reserve(N);
  for (int64_t i = 0; i < N; i++) {
    tmp.set_int(i);
    EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  }
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array.push_back(tmp));
  EXPECT_EQ(N * sizeof(ObObj), allocator.used());
  obj_array.destroy();
  EXPECT_EQ(N * sizeof(ObObj), allocator.used());
  allocator.reset();
  EXPECT_EQ(0, allocator.used());
  EXPECT_EQ(0, allocator.total());
}

TEST(ObTestFiexedArray, serialize)
{
  ObArenaAllocator allocator;
  ObFixedArray<ObObj, ObArenaAllocator > obj_array(allocator);
  const int64_t N = 100;
  ObObj tmp;
  obj_array.reserve(N);
  for (int64_t i = 0; i < N; i++) {
    tmp.set_int(i);
    EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  }
  char buf[1024];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, obj_array.serialize(buf, 1024, pos));
  ObFixedArray<ObObj, ObArenaAllocator > obj_array_to(allocator);
  EXPECT_EQ(pos, obj_array.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, obj_array_to.deserialize(buf, 1024, pos));
  for (int64_t i = 0; i < obj_array_to.count(); i++) {
    EXPECT_EQ(OB_SUCCESS, obj_array_to.at(i, tmp));
    EXPECT_EQ(tmp.get_int(), i);
  }
}
TEST(ObTestFiexedArray, serialize2)
{
  ObArenaAllocator allocator;
  ObFixedArray<ObObj, ObArenaAllocator > obj_array(allocator);
  const int64_t N = 100;
  ObObj tmp;
  obj_array.reserve(N);
  for (int64_t i = 0; i < N; i++) {
    tmp.set_int(i);
    EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  }
  char buf[1024];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, obj_array.serialize(buf, 1024, pos));
  ObSArray<ObObj> obj_array_to;
  EXPECT_EQ(pos, obj_array.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, obj_array_to.deserialize(buf, 1024, pos));
  for (int64_t i = 0; i < obj_array_to.count(); i++) {
    EXPECT_EQ(OB_SUCCESS, obj_array_to.at(i, tmp));
    EXPECT_EQ(tmp.get_int(), i);
  }
}
TEST(ObTestFiexedArray, serialize3)
{
  ObArenaAllocator allocator;
  ObSArray<ObObj> obj_array;
  const int64_t N = 100;
  ObObj tmp;
  obj_array.reserve(N);
  for (int64_t i = 0; i < N; i++) {
    tmp.set_int(i);
    EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  }
  char buf[1024];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, obj_array.serialize(buf, 1024, pos));
  ObFixedArray<ObObj, ObArenaAllocator > obj_array_to(allocator);
  EXPECT_EQ(pos, obj_array.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, obj_array_to.deserialize(buf, 1024, pos));
  for (int64_t i = 0; i < obj_array_to.count(); i++) {
    EXPECT_EQ(OB_SUCCESS, obj_array_to.at(i, tmp));
    EXPECT_EQ(tmp.get_int(), i);
  }
}
TEST(ObTestFiexedArray, other)
{
  int ret = 0;
  ObArray<int> tmp_array;
  Testfun testfun(tmp_array);
  OB_LOG(WARN, "return size", K(sizeof(testfun)), K(sizeof(Testfun)));
  common::ObObj test;
  test.set_int(4);
  common::ObObj &ref = test;
  common::ObObj *ptr = &ref;
  OB_LOG(WARN, "print referenct", K(&test), K(&ref), K(*ptr));
}

TEST(ObTestFiexedArray, assign)
{
  ObMalloc alloc;
  ObFixedArray<int, ObMalloc> fa1(alloc);
  ObFixedArray<int, ObMalloc> fa2(alloc);
  ObFixedArray<int, ObMalloc> fa3;
  ObArray<int> a1;
  ASSERT_EQ(OB_SUCCESS, fa2.init(1));
  ASSERT_EQ(OB_SUCCESS, fa2.push_back(1));
  ASSERT_EQ(OB_SUCCESS, a1.push_back(2));
  ASSERT_NE(OB_SUCCESS, fa3.assign(a1));
  ASSERT_EQ(OB_SUCCESS, fa1.assign(fa2));

  ASSERT_EQ(1, fa1.at(0));
  ASSERT_EQ(OB_SUCCESS, fa1.assign(a1));
  ASSERT_EQ(2, fa1.at(0));

  fa1 = fa2;
  ASSERT_EQ(OB_SUCCESS, fa1.get_copy_assign_ret());
  ASSERT_EQ(1, fa1.at(0));
}

}
}

int main(int argc, char **argv)
{
   OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_fixed_array.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

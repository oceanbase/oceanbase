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

#include <cstring>

#include "sql/engine/expr/ob_infix_expression.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{
  class ObTestSqlFixedArray: public ::testing::Test
  {
  public:    
    ObTestSqlFixedArray() {}
    ~ObTestSqlFixedArray() {}
    virtual void SetUp() {}
    virtual void TearDown() {}
  private:
    DISALLOW_COPY_AND_ASSIGN(ObTestSqlFixedArray);
  };

TEST(ObTestSqlFixedArray, push_back)
{
  int64_t capacity = 100;
  ObMalloc alloc;
  ObSqlFixedArray<ObObj> obj_array;
  ObObj tmp2;
  EXPECT_EQ(OB_SUCCESS, obj_array.init(capacity, alloc));
  EXPECT_EQ(0, obj_array.count());
  EXPECT_TRUE(obj_array.empty());

  EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp2));
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array.reserve(200, alloc));
  EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp2));
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array.reserve(200, alloc));
  EXPECT_EQ(2, obj_array.count());

  ObSqlFixedArray<ObObj> obj_array2;
  EXPECT_EQ(OB_NOT_INIT, obj_array2.push_back(tmp2));
  EXPECT_EQ(OB_SUCCESS, obj_array2.init(capacity, alloc));
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array2.reserve(200, alloc));
  EXPECT_EQ(0, obj_array2.count());
  EXPECT_TRUE(obj_array2.empty());
}

TEST(ObTestSqlFixedArray, to_string)
{
  ObSqlFixedArray<ObObj> obj_array;
  ObSqlFixedArray<int64_t> int_array;
  ObObj tmp;
  ObMalloc alloc;
    
  EXPECT_EQ(OB_SUCCESS, obj_array.init(20, alloc));
  EXPECT_EQ(OB_SUCCESS, int_array.init(20, alloc));

  tmp.set_int(1);
  EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  tmp.set_int(2);
  EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  tmp.set_int(3);
  EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));

  char obj_buf[256];
  int obj_pos = 0;
  obj_pos = obj_array.to_string(obj_buf, 256);

  EXPECT_EQ(0, std::strncmp("[{\"BIGINT\":1}, {\"BIGINT\":2}, {\"BIGINT\":3}]", obj_buf, obj_pos));

  EXPECT_EQ(OB_SUCCESS, int_array.push_back(1));
  EXPECT_EQ(OB_SUCCESS, int_array.push_back(2));
  EXPECT_EQ(OB_SUCCESS, int_array.push_back(3));

  char int_buf[256];
  int int_pos = 0;
  int_pos = int_array.to_string(int_buf, 256);
  EXPECT_EQ(0, std::strncmp("[1, 2, 3]", int_buf, int_pos));
}

TEST(ObTestSqlFixedArray, destroy)
{
  ObArenaAllocator allocator;
  ObSqlFixedArray<ObObj> obj_array;
  ObObj tmp;
  const int64_t N = 3000;
  EXPECT_EQ(OB_NOT_INIT, obj_array.push_back(tmp));
  EXPECT_EQ(OB_SUCCESS, obj_array.reserve(N, allocator));
    
  for (int64_t i = 0; i < N; i++) {
    tmp.set_int(i);
    EXPECT_EQ(OB_SUCCESS, obj_array.push_back(tmp));
  }
  EXPECT_EQ(OB_SIZE_OVERFLOW, obj_array.push_back(tmp));
  EXPECT_EQ(N, obj_array.count());
  EXPECT_TRUE(N * sizeof(ObObj) <= allocator.used());
  obj_array.destroy(allocator);
  EXPECT_TRUE(N * sizeof(ObObj) <= allocator.used());
  allocator.reset();
  EXPECT_EQ(0, allocator.used());
  EXPECT_EQ(0, allocator.total());
}
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}

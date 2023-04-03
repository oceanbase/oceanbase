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

#include "gtest/gtest.h"
#include "sql/plan_cache/ob_id_manager_allocator.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObIdManagerAllocatorTest : public ::testing::Test
{
public:
  ObIdManagerAllocatorTest();
  virtual ~ObIdManagerAllocatorTest();
  void SetUp();
  void TearDown();
private:
  DISALLOW_COPY_AND_ASSIGN(ObIdManagerAllocatorTest);
};

ObIdManagerAllocatorTest::ObIdManagerAllocatorTest()
{
}

ObIdManagerAllocatorTest::~ObIdManagerAllocatorTest()
{
};

void ObIdManagerAllocatorTest::SetUp()
{
}

void ObIdManagerAllocatorTest::TearDown()
{
}

TEST_F(ObIdManagerAllocatorTest, basic_test)
{
  const int64_t size_cnt = 5;
  const int64_t loop_cnt = 1024;
  int64_t sizes[size_cnt] = { 1, 128, 1024, 1024 * 6, 1024 * 1024 };
  void *ptr[loop_cnt];

  for(int64_t i = 0; i < 3; ++i) {
    ObIdManagerAllocator alloc_impl;
    alloc_impl.init(sizes[i], 0, OB_SYS_TENANT_ID);
    for (int64_t idx = 0; idx < size_cnt; ++idx) {
      for (int64_t loop = 0; loop < loop_cnt; ++loop) {
        ptr[loop] = NULL;
        ptr[loop] = alloc_impl.alloc(sizes[idx]);
        ASSERT_TRUE(NULL != ptr[loop]);
      }
      for(int64_t loop = 0; loop <loop_cnt; ++loop) {
        alloc_impl.free(ptr[loop]);
        ptr[loop] = NULL;
      }
    }
  }
}

TEST_F(ObIdManagerAllocatorTest, failure_test)
{
  const int64_t size_cnt = 5;
  const int64_t loop_cnt = 1024;
  int64_t sizes[size_cnt] = { 1, 128, 1024, 1024 * 6, 1024 * 1024 };
  void *ptr[loop_cnt];

  for(int64_t i = 0; i < size_cnt; ++i) {
    ObIdManagerAllocator alloc_impl;
    for (int64_t idx = 0; idx < size_cnt; ++idx) {
      for (int64_t loop = 0; loop < loop_cnt; ++loop) {
        ptr[loop] = NULL;
        ptr[loop] = alloc_impl.alloc(sizes[idx]);
        ASSERT_TRUE(NULL == ptr[loop]);
      }
      for(int64_t loop = 0; loop <loop_cnt; ++loop) {
        alloc_impl.free(ptr[loop]);
        ptr[loop] = NULL;
      }
    }
  }
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

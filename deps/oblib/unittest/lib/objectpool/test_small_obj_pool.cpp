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

#include "common/ob_field.h"            // ObParamedSelectItemCtx
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool
#include "lib/coro/testing.h"

using namespace oceanbase::common;

class ObSmallObjPoolTest : public ::testing::Test
{
public:
  typedef ObParamedSelectItemCtx TestObj;
  static const int64_t THREAD_NUM = 10;
  static const int64_t RUN_TIME_SEC = 10;
  static const int64_t FIXED_COUNT = 1024;
  static const int64_t TEST_COUNT = 2 * FIXED_COUNT;
  static const int64_t SLEEP_TIME = 8000;

public:
  ObSmallObjPoolTest();
  virtual ~ObSmallObjPoolTest();
  virtual void SetUp();
  virtual void TearDown();

public:
  void run();

public:
  pthread_t threads_[THREAD_NUM];
  ObSmallObjPool<TestObj> pool_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSmallObjPoolTest);
};
ObSmallObjPoolTest::ObSmallObjPoolTest()
{
  (void)memset(threads_, 0, sizeof(threads_));
}

ObSmallObjPoolTest::~ObSmallObjPoolTest()
{
}

void ObSmallObjPoolTest::SetUp()
{
}

void ObSmallObjPoolTest::TearDown()
{
}

void ObSmallObjPoolTest::run()
{
  int64_t end_time = ObTimeUtility::current_time() + RUN_TIME_SEC * 1000000;
  TestObj *obj_array[TEST_COUNT];

  while (true) {
    for (int64_t index = 0; index < TEST_COUNT; index++) {
      obj_array[index] = NULL;
      EXPECT_EQ(OB_SUCCESS, pool_.alloc(obj_array[index]));
      EXPECT_TRUE(NULL != obj_array[index]);
    }

    for (int64_t index = TEST_COUNT - 1; index >= 0; index--) {
      EXPECT_EQ(OB_SUCCESS, pool_.free(obj_array[index]));
      obj_array[index] = NULL;
    }

    int64_t left_time = end_time - ObTimeUtility::current_time();

    if (left_time <= 0) break;

    ::usleep(SLEEP_TIME);
  }
}

TEST_F(ObSmallObjPoolTest, basic_test)
{
  int64_t fixed_count = FIXED_COUNT;
  ObSmallObjPool<TestObj> pool;
  TestObj *obj = NULL;
  TestObj *obj_array[TEST_COUNT];

  // Test the init/destroy function
  EXPECT_EQ(OB_INVALID_ARGUMENT, pool.init(-1));
  EXPECT_EQ(OB_SUCCESS, pool.init(FIXED_COUNT));
  pool.destroy();

  EXPECT_EQ(OB_SUCCESS, pool.init());
  pool.destroy();

  EXPECT_EQ(OB_SUCCESS, pool.init(FIXED_COUNT));
  ASSERT_EQ(0, pool.get_free_count());
  ASSERT_EQ(0, pool.get_alloc_count());
  ASSERT_EQ(fixed_count, pool.get_fixed_count());
  pool.destroy();

  // Test the alloc/free function
  obj = NULL;
  EXPECT_EQ(OB_NOT_INIT, pool.alloc(obj));
  EXPECT_EQ(NULL, obj);
  EXPECT_EQ(OB_NOT_INIT, pool.free(obj));

  ASSERT_EQ(OB_SUCCESS, pool.init(FIXED_COUNT));

  for (int64_t index = 0; index < TEST_COUNT; index++) {
    int64_t alloc_count = index + 1;
    obj_array[index] = NULL;
    EXPECT_EQ(OB_SUCCESS, pool.alloc(obj_array[index]));
    EXPECT_TRUE(NULL != obj_array[index]);
    ASSERT_EQ(0, pool.get_free_count());
    ASSERT_EQ(alloc_count, pool.get_alloc_count());
  }

  for (int64_t index = TEST_COUNT - 1; index >= 0; index--) {
    EXPECT_EQ(OB_SUCCESS, pool.free(obj_array[index]));
    obj_array[index] = NULL;

    int64_t alloc_count = index >= FIXED_COUNT ? index : FIXED_COUNT;
    int64_t free_count = index >= FIXED_COUNT ? 0 : FIXED_COUNT - index;
    ASSERT_EQ(free_count, pool.get_free_count());
    ASSERT_EQ(alloc_count, pool.get_alloc_count());
  }

  // Test exception free
  // 1. All elements are placed in free_list, at this time allocating elements and releasing elements will operate the elements in free_list
  ASSERT_EQ(fixed_count, pool.get_free_count());
  ASSERT_EQ(fixed_count, pool.get_alloc_count());
  obj = NULL;
  EXPECT_EQ(OB_SUCCESS, pool.alloc(obj));
  EXPECT_TRUE(NULL != obj);
  EXPECT_EQ(OB_SUCCESS, pool.free(obj));
  EXPECT_NE(OB_SUCCESS, pool.free(obj));  // Repeated release, push free_list will fail
  obj = NULL;

  // 2. All the elements in the free_list are allocated, and then the allocation and release of the elements are dynamically allocated and released
  for (int64_t index = 0; index < FIXED_COUNT; index++) {
    obj_array[index] = NULL;
    EXPECT_EQ(OB_SUCCESS, pool.alloc(obj_array[index]));
  }
  ASSERT_EQ(0, pool.get_free_count());
  ASSERT_EQ(fixed_count, pool.get_alloc_count());
  obj = NULL;
  EXPECT_EQ(OB_SUCCESS, pool.alloc(obj));
  EXPECT_TRUE(NULL != obj);
  EXPECT_EQ(OB_SUCCESS, pool.free(obj));
  EXPECT_NE(OB_SUCCESS, pool.free(obj));  // Repeated release will reference invalid memory
}

TEST_F(ObSmallObjPoolTest, multiple_thread)
{
  ASSERT_EQ(OB_SUCCESS, pool_.init(FIXED_COUNT));

  cotesting::FlexPool([this]{
    run();
  }, THREAD_NUM).start();
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

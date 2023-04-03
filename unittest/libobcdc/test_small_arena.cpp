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
#include "ob_log_utils.h"       // current_time
#include "ob_small_arena.h"

#include "lib/allocator/ob_concurrent_fifo_allocator.h"   // ObConcurrentFIFOAllocator

#define ALLOC_AND_CHECK(size) ALLOC_ALIGN_AND_CHECK(sizeof(void*), size)

#define ALLOC_ALIGN_AND_CHECK(align_size, size) \
    do { \
      int64_t alloc_size = (size); \
      int64_t align = (align_size); \
      int64_t max_small_size = MAX_SMALL_ALLOC_SIZE(align); \
      void *ptr = sa.alloc_aligned(alloc_size, align); \
\
      ASSERT_TRUE(NULL != ptr); \
      EXPECT_EQ(0, reinterpret_cast<int64_t>(ptr) & (align - 1)); \
\
      if (alloc_size > max_small_size) { \
        large_alloc_count++; \
      } else { \
        small_alloc_count++; \
      } \
\
      EXPECT_EQ(small_alloc_count, sa.get_small_alloc_count()); \
      EXPECT_EQ(large_alloc_count, sa.get_large_alloc_count()); \
\
      ((char *)ptr)[alloc_size - 1] = 'a'; \
    } while (0)

#define MAX_SMALL_ALLOC_SIZE(align) (MAX_SMALL_ALLOC_SIZE_WITHOUT_ALIGN - align + 1)

namespace oceanbase
{
namespace libobcdc
{

static const int64_t SMALL_ARENA_PAGE_SIZE = 1024;
static const int64_t MAX_SMALL_ALLOC_SIZE_WITHOUT_ALIGN = SMALL_ARENA_PAGE_SIZE - ObSmallArena::SMALL_PAGE_HEADER_SIZE;
static const int64_t PAGE_SIZE = 1024;

using namespace common;

// TODO: add multi thread test
class TestSmallArena : public ::testing::Test
{
public:
  TestSmallArena() {}
  ~TestSmallArena() {}

  virtual void SetUp();
  virtual void TearDown();

public:
  ObConcurrentFIFOAllocator large_allocator_;
  static const uint64_t tenant_id_ = 0;
};

void TestSmallArena::SetUp()
{
  const static int64_t LARGE_PAGE_SIZE = (1LL << 26);
  const static int64_t LARGE_TOTAL_LIMIT = (1LL << 34);
  const static int64_t LARGE_HOLD_LIMIT = LARGE_PAGE_SIZE;
  ASSERT_EQ(OB_SUCCESS, large_allocator_.init(LARGE_TOTAL_LIMIT, LARGE_HOLD_LIMIT, LARGE_PAGE_SIZE));

  srandom((unsigned int)get_timestamp());
}

void TestSmallArena::TearDown()
{
  large_allocator_.destroy();
}

TEST_F(TestSmallArena, smoke_test)
{
  ObSmallArena sa;
  int64_t small_alloc_count = 0;
  int64_t large_alloc_count = 0;

  sa.set_allocator(PAGE_SIZE, large_allocator_);

  ALLOC_AND_CHECK(8);
  ALLOC_AND_CHECK(16);
  ALLOC_AND_CHECK(256);
  ALLOC_AND_CHECK(512);
  ALLOC_AND_CHECK(17 + 8);
  ALLOC_AND_CHECK(17 + 16);
  ALLOC_AND_CHECK(17 + 256);
  ALLOC_AND_CHECK(17 + 512);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 512);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 512);

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;

  ALLOC_AND_CHECK(8);
  ALLOC_AND_CHECK(16);
  ALLOC_AND_CHECK(256);
  ALLOC_AND_CHECK(512);
  ALLOC_AND_CHECK(17 + 8);
  ALLOC_AND_CHECK(17 + 16);
  ALLOC_AND_CHECK(17 + 256);
  ALLOC_AND_CHECK(17 + 512);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE + 17 + 512);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_AND_CHECK((1<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 512);

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;
}

TEST_F(TestSmallArena, alloc_small)
{
  static const int64_t TEST_COUNT = 10;
  int64_t max_alloc_size = MAX_SMALL_ALLOC_SIZE(8);
  int64_t small_alloc_count = 0;
  int64_t large_alloc_count = 0;
  ObSmallArena sa;

  sa.set_allocator(PAGE_SIZE, large_allocator_);

  for (int i = 0; i < TEST_COUNT; i++) {
    ALLOC_AND_CHECK(8);
    ALLOC_AND_CHECK(16);
    ALLOC_AND_CHECK(256);
    ALLOC_AND_CHECK(512);
    ALLOC_AND_CHECK(17 + 8);
    ALLOC_AND_CHECK(17 + 16);
    ALLOC_AND_CHECK(17 + 256);
    ALLOC_AND_CHECK(17 + 512);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 15);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 16);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 17);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 64);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 67);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 128);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 129);

    ALLOC_AND_CHECK(random() % (max_alloc_size + 1));
  }

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;

  for (int i = 0; i < TEST_COUNT; i++) {
    ALLOC_AND_CHECK(8);
    ALLOC_AND_CHECK(16);
    ALLOC_AND_CHECK(256);
    ALLOC_AND_CHECK(512);
    ALLOC_AND_CHECK(17 + 8);
    ALLOC_AND_CHECK(17 + 16);
    ALLOC_AND_CHECK(17 + 256);
    ALLOC_AND_CHECK(17 + 512);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 15);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 16);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 17);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 64);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 67);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 128);
    ALLOC_AND_CHECK(SMALL_ARENA_PAGE_SIZE - 129);

    ALLOC_AND_CHECK(random() % (max_alloc_size + 1));
  }

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;
}

TEST_F(TestSmallArena, alloc_large)
{
  static const int64_t TEST_COUNT = 10;
  int64_t max_alloc_size = (1 << 22);
  int64_t min_alloc_size = MAX_SMALL_ALLOC_SIZE(8) + 1;
  int64_t small_alloc_count = 0;
  int64_t large_alloc_count = 0;
  ObSmallArena sa;

  sa.set_allocator(PAGE_SIZE, large_allocator_);

  for (int i = 0; i < TEST_COUNT; i++) {
    ALLOC_AND_CHECK(min_alloc_size + 0);
    ALLOC_AND_CHECK(min_alloc_size + 1);
    ALLOC_AND_CHECK(min_alloc_size + 2);
    ALLOC_AND_CHECK(min_alloc_size + 4);
    ALLOC_AND_CHECK(min_alloc_size + 8);
    ALLOC_AND_CHECK(min_alloc_size + 16);
    ALLOC_AND_CHECK(min_alloc_size + 256);
    ALLOC_AND_CHECK(min_alloc_size + 512);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 8);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 16);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 256);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 512);
    ALLOC_AND_CHECK(min_alloc_size + 1 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 2 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 3 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 4 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 5 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 6 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 7 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK((1LL << 10) + 11);
    ALLOC_AND_CHECK((1LL << 12) + 13);
    ALLOC_AND_CHECK((1LL << 18) + 17);
    ALLOC_AND_CHECK((1LL << 19) + 19);
    ALLOC_AND_CHECK((1LL << 20) + 7);
    ALLOC_AND_CHECK((1LL << 21) + 3);

    ALLOC_AND_CHECK((random() % (max_alloc_size)) + min_alloc_size);
  }

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;

  for (int i = 0; i < TEST_COUNT; i++) {
    ALLOC_AND_CHECK(min_alloc_size + 0);
    ALLOC_AND_CHECK(min_alloc_size + 1);
    ALLOC_AND_CHECK(min_alloc_size + 2);
    ALLOC_AND_CHECK(min_alloc_size + 4);
    ALLOC_AND_CHECK(min_alloc_size + 8);
    ALLOC_AND_CHECK(min_alloc_size + 16);
    ALLOC_AND_CHECK(min_alloc_size + 256);
    ALLOC_AND_CHECK(min_alloc_size + 512);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 8);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 16);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 256);
    ALLOC_AND_CHECK(min_alloc_size + 17 + 512);
    ALLOC_AND_CHECK(min_alloc_size + 1 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 2 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 3 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 4 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 5 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 6 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK(min_alloc_size + 7 * SMALL_ARENA_PAGE_SIZE);
    ALLOC_AND_CHECK((1LL << 10) + 11);
    ALLOC_AND_CHECK((1LL << 12) + 13);
    ALLOC_AND_CHECK((1LL << 18) + 17);
    ALLOC_AND_CHECK((1LL << 19) + 19);
    ALLOC_AND_CHECK((1LL << 20) + 7);
    ALLOC_AND_CHECK((1LL << 21) + 3);

    ALLOC_AND_CHECK((random() % (max_alloc_size)) + min_alloc_size);
  }

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;
}

TEST_F(TestSmallArena, alloc_align)
{
  int64_t small_alloc_count = 0;
  int64_t large_alloc_count = 0;
  ObSmallArena sa;

  sa.set_allocator(PAGE_SIZE, large_allocator_);

  ALLOC_ALIGN_AND_CHECK(1, 4);
  ALLOC_ALIGN_AND_CHECK(16, 8);
  ALLOC_ALIGN_AND_CHECK(32, 16);
  ALLOC_ALIGN_AND_CHECK(64, 256);
  ALLOC_ALIGN_AND_CHECK(128, 512);
  ALLOC_ALIGN_AND_CHECK(16, 17 + 8);
  ALLOC_ALIGN_AND_CHECK(32, 17 + 16);
  ALLOC_ALIGN_AND_CHECK(64, 17 + 256);
  ALLOC_ALIGN_AND_CHECK(128, 17 + 512);
  ALLOC_ALIGN_AND_CHECK(16, SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_ALIGN_AND_CHECK(32, SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_ALIGN_AND_CHECK(64, SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_ALIGN_AND_CHECK(128, SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_ALIGN_AND_CHECK(16, SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_ALIGN_AND_CHECK(32, SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_ALIGN_AND_CHECK(64, SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_ALIGN_AND_CHECK(128, SMALL_ARENA_PAGE_SIZE + 17 + 512);
  ALLOC_ALIGN_AND_CHECK(16, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_ALIGN_AND_CHECK(32, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_ALIGN_AND_CHECK(64, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_ALIGN_AND_CHECK(128, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_ALIGN_AND_CHECK(16, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_ALIGN_AND_CHECK(32, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_ALIGN_AND_CHECK(64, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_ALIGN_AND_CHECK(128, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 512);

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;

  ALLOC_ALIGN_AND_CHECK(1, 4);
  ALLOC_ALIGN_AND_CHECK(16, 8);
  ALLOC_ALIGN_AND_CHECK(32, 16);
  ALLOC_ALIGN_AND_CHECK(64, 256);
  ALLOC_ALIGN_AND_CHECK(128, 512);
  ALLOC_ALIGN_AND_CHECK(16, 17 + 8);
  ALLOC_ALIGN_AND_CHECK(32, 17 + 16);
  ALLOC_ALIGN_AND_CHECK(64, 17 + 256);
  ALLOC_ALIGN_AND_CHECK(128, 17 + 512);
  ALLOC_ALIGN_AND_CHECK(16, SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_ALIGN_AND_CHECK(32, SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_ALIGN_AND_CHECK(64, SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_ALIGN_AND_CHECK(128, SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_ALIGN_AND_CHECK(16, SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_ALIGN_AND_CHECK(32, SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_ALIGN_AND_CHECK(64, SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_ALIGN_AND_CHECK(128, SMALL_ARENA_PAGE_SIZE + 17 + 512);
  ALLOC_ALIGN_AND_CHECK(16, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 8);
  ALLOC_ALIGN_AND_CHECK(32, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 16);
  ALLOC_ALIGN_AND_CHECK(64, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 256);
  ALLOC_ALIGN_AND_CHECK(128, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 512);
  ALLOC_ALIGN_AND_CHECK(16, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 8);
  ALLOC_ALIGN_AND_CHECK(32, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 16);
  ALLOC_ALIGN_AND_CHECK(64, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 256);
  ALLOC_ALIGN_AND_CHECK(128, (1LL<<21) + SMALL_ARENA_PAGE_SIZE + 17 + 512);

  sa.reset();
  small_alloc_count = 0;
  large_alloc_count = 0;
}

TEST_F(TestSmallArena, init_err)
{
  void *ptr = NULL;
  ObSmallArena sa;
  ptr = sa.alloc(8); EXPECT_TRUE(NULL == ptr);

  sa.set_allocator(-1, large_allocator_);
  ptr = sa.alloc(8); EXPECT_TRUE(NULL == ptr);

  sa.reset();
}

TEST_F(TestSmallArena, invalid_args)
{
  void *ptr = NULL;
  ObSmallArena sa;
  sa.set_allocator(PAGE_SIZE, large_allocator_);
  ptr = sa.alloc(-1); EXPECT_TRUE(NULL == ptr);
  ptr = sa.alloc_aligned(1,3); EXPECT_TRUE(NULL == ptr);
  ptr = sa.alloc_aligned(1, 1024); EXPECT_TRUE(NULL == ptr);
  sa.reset();
}

} // namespace libobcdc
} // ns oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

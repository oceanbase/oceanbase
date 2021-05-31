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
#include "lib/allocator/page_arena.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

struct MyPageAllocator : public ObIAllocator {
  void* alloc(const int64_t sz)
  {
    alloc_count_++;
    return ob_malloc(sz);
  }
  void free(void* p)
  {
    free_count_++;
    ob_free(p);
  }
  void freed(const int64_t sz)
  {
    UNUSED(sz);
  }
  void set_label(const lib::ObLabel& label)
  {
    UNUSED(label);
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    UNUSED(tenant_id);
  }
  lib::ObLabel get_label() const
  {
    return 0;
  }

  static int64_t alloc_count_;
  static int64_t free_count_;
};

typedef PageArena<char, MyPageAllocator> MyModuleArena;
int64_t MyPageAllocator::alloc_count_;
int64_t MyPageAllocator::free_count_;

#define CHECK(expect_ac, expect_fc)              \
  do {                                           \
    int64_t& ac = MyPageAllocator::alloc_count_; \
    int64_t& fc = MyPageAllocator::free_count_;  \
    EXPECT_EQ(expect_ac, ac);                    \
    EXPECT_EQ(expect_fc, fc);                    \
  } while (0)

#define RESET()                                  \
  do {                                           \
    int64_t& ac = MyPageAllocator::alloc_count_; \
    int64_t& fc = MyPageAllocator::free_count_;  \
    EXPECT_EQ(ac, fc);                           \
    ac = fc = 0;                                 \
  } while (0)

TEST(TestPageArena, Basic)
{
  {
    CHECK(0, 0);
    MyModuleArena ma;
    // Weird!! Default page size is 8K-32, and meta size for each page
    // is 32 bytes. So if we alloc 8K-32-31 which less than 8K-32-32
    // it will allocate a default normal page, and a big page.
    ma.alloc(8192 - 32 - 31);
    CHECK(2, 0);
    ma.free();
    CHECK(2, 2);
    RESET();

    // If we allocate memory with size 8K-32-32, it can be hold in the
    // first normal page.
    ma.alloc(8192 - 32 - 32);
    CHECK(1, 0);
  }
  CHECK(1, 1);
  RESET();
}

TEST(TestPageArena, Tracer1)
{
  MyModuleArena ma;
  CHECK(0, 0);
  ma.alloc(10);
  CHECK(1, 0);

  EXPECT_TRUE(ma.set_tracer());
  ma.alloc(8192);
  ma.alloc(8192);
  ma.alloc(8192);
  CHECK(4, 0);

  constexpr auto N = 8192 - 32 - 32;
  for (int i = 0; i < N; i++) {
    ma.alloc(1);
  }
  CHECK(5, 0);

  EXPECT_TRUE(ma.revert_tracer());
  CHECK(5, 4);
  ma.free();
  RESET();
}

TEST(TestPageArena, Tracer2)
{
  MyModuleArena ma;
  CHECK(0, 0);

  for (int i = 0; i < 2; i++) {
    EXPECT_TRUE(ma.set_tracer());
    CHECK(3 * i, 3 * i);

    // some small allocates
    constexpr auto N = 8192 - 32 - 32;
    for (int i = 0; i < N; i++) {
      ma.alloc(1);
    }
    CHECK(1 + 3 * i, 3 * i);

    // some big allocates
    ma.alloc(8192);
    ma.alloc(8192);

    EXPECT_TRUE(ma.revert_tracer());
    CHECK(3 + 3 * i, 3 + 3 * i);
  }
  ma.free();
  RESET();
}

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

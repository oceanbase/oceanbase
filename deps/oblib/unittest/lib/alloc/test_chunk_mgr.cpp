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
#define private public
#define protected public
#include "lib/resource/achunk_mgr.h"
#undef protected
#undef private

using namespace oceanbase::lib;
using namespace oceanbase::common;

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestChunkMgr : public ::testing::Test, public AChunkMgr {
public:
  virtual void SetUp()
  {}

  virtual void TearDown()
  {}
};

TEST_F(TestChunkMgr, FreeListBasic)
{
  {
    AChunk* chunk = alloc_chunk(0);
    free_chunk(chunk);
    EXPECT_EQ(1, free_list_.get_pushes());
  }
  {
    AChunk* chunk = alloc_chunk(0);
    free_chunk(chunk);
    EXPECT_EQ(2, free_list_.get_pushes());
    EXPECT_EQ(1, free_list_.get_pops());
  }
  {
    AChunk* chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
    free_chunk(chunk);
    EXPECT_EQ(3, free_list_.get_pushes());
    EXPECT_EQ(2, free_list_.get_pops());
  }
}

TEST_F(TestChunkMgr, FreeListManyChunk)
{
  AChunk* chunks[1024] = {};
  for (int i = 0; i < 1024; i++) {
    chunks[i] = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
  }
  for (int i = 0; i < 1024; i++) {
    free_chunk(chunks[i]);
  }
  EXPECT_EQ(500, free_list_.get_pushes());
  EXPECT_EQ(0, free_list_.get_pops());

  for (int i = 0; i < 1024; i++) {
    chunks[i] = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
  }
  for (int i = 0; i < 1024; i++) {
    free_chunk(chunks[i]);
  }
  EXPECT_EQ(500 * 2, free_list_.get_pushes());
  EXPECT_EQ(500, free_list_.get_pops());
}

TEST_F(TestChunkMgr, Hybird)
{
#define CLEAR_FREE_LIST            \
  while (free_list_.count() > 0) { \
    free_list_.pop();              \
  }

  // When large_space <LARGE_CHUNK_FREE_SPACE(2G), the large memory block is directly unmapped
  {
    CLEAR_FREE_LIST;
    limit_ = LARGE_CHUNK_FREE_SPACE;
    auto* chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE * 2);
    ASSERT_NE(nullptr, chunk);
    hold_ = limit_ - 1;
    int hold_before = hold_;
    free_chunk(chunk);
    ASSERT_EQ(0, free_list_.count());
    ASSERT_EQ(INTACT_ACHUNK_SIZE * 2, hold_before - hold_);
  }

  // When large_space> LARGE_CHUNK_FREE_SPACE(2G), the large memory block is directly split into multiple 2M and placed
  // in the freelist
  {
    CLEAR_FREE_LIST;
    limit_ = LARGE_CHUNK_FREE_SPACE;
    auto* chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE * 2);
    ASSERT_NE(nullptr, chunk);
    hold_ = 0;
    free_chunk(chunk);
    ASSERT_EQ(2, free_list_.count());
    ASSERT_EQ(0, hold_);
  }

  // When large_space> LARGE_CHUNK_FREE_SPACE(2G), the large memory block is directly divided into multiple 2M and
  // placed in the freelist. The unmapped part of the freelist is directly unmapped
  {
    CLEAR_FREE_LIST;
    limit_ = LARGE_CHUNK_FREE_SPACE;
    auto* chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE * 3);
    ASSERT_NE(nullptr, chunk);
    hold_ = 0;
    free_list_.set_max_chunk_cache_cnt(1);
    free_chunk(chunk);
    ASSERT_EQ(1, free_list_.count());
    ASSERT_EQ(2 * INTACT_ACHUNK_SIZE, -hold_);
  }

  // When large memory cannot be applied, squeeze out more than 2M from the freelist
  {
    CLEAR_FREE_LIST;
    free_list_.set_max_chunk_cache_cnt(2);
    limit_ = 2 * LARGE_CHUNK_FREE_SPACE;
    hold_ = 0;
    auto* chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
    ASSERT_NE(nullptr, chunk);
    auto* chunk2 = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
    ASSERT_NE(nullptr, chunk2);
    ASSERT_EQ(2 * INTACT_ACHUNK_SIZE, hold_);
    ASSERT_EQ(0, free_list_.count());
    free_chunk(chunk);
    ASSERT_EQ(2 * INTACT_ACHUNK_SIZE, hold_);
    ASSERT_EQ(1, free_list_.count());
    // free 2M, put directly into the freelist
    hold_ = LARGE_CHUNK_FREE_SPACE + 1;
    free_chunk(chunk2);
    ASSERT_EQ(LARGE_CHUNK_FREE_SPACE + 1, hold_);
    ASSERT_EQ(2, free_list_.count());

    // free 4M and above, limit-hold <LARGE_CHUNK_FREE_SPACE, unmap directly
    CLEAR_FREE_LIST;
    chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE * 2);
    ASSERT_NE(nullptr, chunk);
    hold_ = LARGE_CHUNK_FREE_SPACE + 1;
    free_chunk(chunk);
    ASSERT_EQ(LARGE_CHUNK_FREE_SPACE + 1 - INTACT_ACHUNK_SIZE * 2, hold_);
    ASSERT_EQ(0, free_list_.count());

    // free 4M and above, limit-hold> LARGE_CHUNK_FREE_SPACE, split into freelist
    CLEAR_FREE_LIST;
    chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE * 2);
    ASSERT_NE(nullptr, chunk);
    hold_ = 0;
    free_chunk(chunk);
    ASSERT_EQ(0, hold_);
    ASSERT_EQ(2, free_list_.count());

    // free 4M and above, limit-hold> LARGE_CHUNK_FREE_SPACE, split into freelist, unmap the part that cannot be put
    CLEAR_FREE_LIST;
    free_list_.set_max_chunk_cache_cnt(1);
    chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE * 3);
    ASSERT_NE(nullptr, chunk);
    hold_ = 0;
    free_chunk(chunk);
    ASSERT_EQ(INTACT_ACHUNK_SIZE * 2, -hold_);
    ASSERT_EQ(1, free_list_.count());

    // freelist 2, alloc 3
    CLEAR_FREE_LIST;
    free_list_.set_max_chunk_cache_cnt(2);
    chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
    chunk2 = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
    free_chunk(chunk);
    free_chunk(chunk2);
    hold_ = 2 * LARGE_CHUNK_FREE_SPACE - INTACT_ACHUNK_SIZE;
    auto* big_chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE * 3);
    ASSERT_NE(nullptr, big_chunk);
    ASSERT_EQ(2 * LARGE_CHUNK_FREE_SPACE, hold_);
    ASSERT_EQ(0, free_list_.count());
  }
}

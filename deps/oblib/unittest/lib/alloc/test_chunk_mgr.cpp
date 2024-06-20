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

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestChunkMgr
    : public ::testing::Test, public AChunkMgr
{
public:
  TestChunkMgr()
  {}
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
  virtual int madvise(void *addr, size_t length, int advice) override
  {
    if (need_fail_) return -1;
    madvise_len_ = length;
    return AChunkMgr::madvise(addr, length, advice);
  }
  bool need_fail_ = false;
  int madvise_len_ = 0;
};

TEST_F(TestChunkMgr, NormalChunk)
{
  int NORMAL_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  int normal_hold = AChunkMgr::hold(NORMAL_SIZE);
  int large_hold = AChunkMgr::hold(LARGE_SIZE);
  {
    int64_t hold = 0;
    AChunk *chunks[8] = {};
    // direct alloc 2M
    for (int i = 0; i < 8; ++i) {
      chunks[i] = alloc_chunk(NORMAL_SIZE);
      hold += normal_hold;
    }
    // create 2M-cache
    for (int i = 0; i < 8; ++i) {
      free_chunk(chunks[i]);
      chunks[i] = NULL;
    }
    // alloc chunk from 2M-cache
    for (int i = 0; i < 8; ++i) {
      chunks[i] = alloc_chunk(NORMAL_SIZE);
    }
    EXPECT_EQ(8, slots_[0]->get_pushes());
    EXPECT_EQ(8, slots_[0]->get_pops());
    EXPECT_EQ(0, slots_[1]->get_pushes());
    EXPECT_EQ(0, slots_[1]->get_pops());
    EXPECT_EQ(hold, hold_);
    // alloc chunk by wash 4M-cache
    {
      AChunk *chunk = alloc_chunk(LARGE_SIZE);
      EXPECT_TRUE(NULL != chunk);
      hold += large_hold;
      free_chunk(chunk);
      EXPECT_EQ(hold, hold_);
      set_limit(hold);
      chunk = alloc_chunk(NORMAL_SIZE);
      EXPECT_TRUE(NULL != chunk);
      hold -= (large_hold - normal_hold);
      EXPECT_EQ(8, slots_[0]->get_pushes());
      EXPECT_EQ(8, slots_[0]->get_pops());
      EXPECT_EQ(1, slots_[1]->get_pushes());
      EXPECT_EQ(1, slots_[1]->get_pops());
      EXPECT_EQ(hold, hold_);
    }
  }
}

TEST_F(TestChunkMgr, LargeChunk)
{
  int NORMAL_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  int BIG_SIZE = INTACT_ACHUNK_SIZE * 2 + 100;
  int normal_hold = AChunkMgr::hold(NORMAL_SIZE);
  int large_hold = AChunkMgr::hold(LARGE_SIZE);
  int big_hold = AChunkMgr::hold(BIG_SIZE);
  {
    int64_t hold = 0;
    AChunk *chunks[8] = {};
    // direct alloc 4M
    for (int i = 0; i < 8; ++i) {
      chunks[i] = alloc_chunk(LARGE_SIZE);
      hold += large_hold;
    }
    // create 4M-cache
    for (int i = 0; i < 8; ++i) {
      free_chunk(chunks[i]);
    }
    // alloc chunk from self-cache(4M-cache)
    for (int i = 0; i < 8; ++i) {
      chunks[i] = alloc_chunk(LARGE_SIZE);
    }
    EXPECT_EQ(8, slots_[1]->get_pushes());
    EXPECT_EQ(8, slots_[1]->get_pops());
    EXPECT_EQ(hold, hold_);

    // alloc chunk by wash 6M-cache and 2M-cache
    {
      AChunk *chunk = alloc_chunk(BIG_SIZE);
      EXPECT_TRUE(NULL != chunk);
      hold += big_hold;
      free_chunk(chunk);
      chunk = alloc_chunk(NORMAL_SIZE);
      EXPECT_TRUE(NULL != chunk);
      hold += normal_hold;
      free_chunk(chunk);
      EXPECT_EQ(1, slots_[0]->get_pushes());
      EXPECT_EQ(0, slots_[0]->get_pops());
      EXPECT_EQ(1, slots_[2]->get_pushes());
      EXPECT_EQ(0, slots_[2]->get_pops());
      EXPECT_EQ(hold, hold_);
      set_limit(hold);
      chunk = alloc_chunk(LARGE_SIZE);
      hold += (large_hold - big_hold);
      EXPECT_TRUE(NULL != chunk);
      chunk = alloc_chunk(LARGE_SIZE);
      hold += (large_hold - normal_hold);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(1, slots_[0]->get_pushes());
      EXPECT_EQ(1, slots_[0]->get_pops());
      EXPECT_EQ(1, slots_[2]->get_pushes());
      EXPECT_EQ(1, slots_[2]->get_pops());
      EXPECT_EQ(hold, hold_);
    }
  }
}

TEST_F(TestChunkMgr, HugeChunk)
{
  int NORMAL_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  int HUGE_SIZE = INTACT_ACHUNK_SIZE * 10 + 100;
  {
    int64_t hold = 0;
    {
      AChunk *chunks[8][2] = {};
      for (int i = 0; i < 8; ++i) {
        chunks[i][0] = alloc_chunk(NORMAL_SIZE);
        chunks[i][1] = alloc_chunk(LARGE_SIZE);
        hold += chunks[i][0]->hold();
        hold += chunks[i][1]->hold();
      }
      for (int i = 0; i < 8; ++i) {
        free_chunk(chunks[i][0]);
        free_chunk(chunks[i][1]);
      }
      EXPECT_EQ(8, slots_[0]->get_pushes());
      EXPECT_EQ(0, slots_[0]->get_pops());
      EXPECT_EQ(8, slots_[1]->get_pushes());
      EXPECT_EQ(0, slots_[1]->get_pops());
      EXPECT_EQ(hold, hold_);
    }

    // direct alloc huge
    {
      AChunk *chunk = alloc_chunk(HUGE_SIZE);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(0, slots_[0]->get_pops());
      EXPECT_EQ(0, slots_[1]->get_pops());
      hold += chunk->hold();
      EXPECT_EQ(hold, hold_);
    }

    // wash alloc
    {
      set_limit(hold);
      AChunk *chunk = alloc_chunk(HUGE_SIZE);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_NE(0, slots_[0]->hold());
      EXPECT_EQ(0, slots_[1]->hold());

      chunk = alloc_chunk(slots_[0]->hold());
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(0, slots_[0]->hold());
    }
  }
}

TEST_F(TestChunkMgr, BorderCase_advise_shrink)
{
  int ps = get_page_size();
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100 + ps * 3;
  auto *chunk = alloc_chunk(LARGE_SIZE);
  // pollute chunk
  memset(chunk->data_, 0xaa, chunk->hold());
  EXPECT_EQ(0, slots_[1]->get_pushes());
  int64_t orig_chunk_hold = chunk->hold();
  int64_t orig_hold = hold_;
  free_chunk(chunk);
  EXPECT_EQ(1, slots_[1]->get_pushes());
  EXPECT_EQ(0, slots_[1]->get_pops());
  chunk = alloc_chunk(LARGE_SIZE - ps * 3);
  EXPECT_EQ(1, slots_[1]->get_pops());
  EXPECT_EQ(madvise_len_, ps * 3);
  EXPECT_FALSE(0 == chunk->data_[0] && 0 == memcmp(chunk->data_, chunk->data_ + 1, chunk->hold() - 1));
  EXPECT_EQ(orig_chunk_hold - chunk->hold(), orig_hold - hold_);
}

TEST_F(TestChunkMgr, BorderCase_advise_expand)
{
  int ps = get_page_size();
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  auto *chunk = alloc_chunk(LARGE_SIZE);
  // pollute chunk
  memset(chunk->data_, 0xaa, chunk->hold());
  EXPECT_EQ(0, slots_[1]->get_pushes());
  int64_t orig_chunk_hold = chunk->hold();
  int64_t orig_hold = hold_;
  free_chunk(chunk);
  EXPECT_EQ(1, slots_[1]->get_pushes());
  EXPECT_EQ(0, slots_[1]->get_pops());
  chunk = alloc_chunk(LARGE_SIZE + ps * 3);
  EXPECT_EQ(1, slots_[1]->get_pops());
  EXPECT_FALSE(0 == chunk->data_[0] && 0 == memcmp(chunk->data_, chunk->data_ + 1, chunk->hold() - 1));
  EXPECT_EQ(orig_chunk_hold - (int64_t)chunk->hold(), orig_hold - hold_);
}

TEST_F(TestChunkMgr, BorderCase_advise_fail)
{
  int ps = get_page_size();
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100 + ps * 3;
  auto *chunk = alloc_chunk(LARGE_SIZE);
  // pollute chunk
  memset(chunk->data_, 0xaa, chunk->hold());
  EXPECT_EQ(0, slots_[1]->get_pushes());
  int64_t orig_chunk_hold = chunk->hold();
  int64_t orig_hold = hold_;
  free_chunk(chunk);
  EXPECT_EQ(1, slots_[1]->get_pushes());
  EXPECT_EQ(0, slots_[1]->get_pops());
  need_fail_ = true;
  chunk = alloc_chunk(LARGE_SIZE - ps * 3);
  EXPECT_EQ(1, slots_[1]->get_pushes());
  EXPECT_EQ(1, slots_[1]->get_pops());
  // check remap happened
  EXPECT_TRUE(0 == chunk->data_[0] && 0 == memcmp(chunk->data_, chunk->data_ + 1, chunk->hold() - 1));
  EXPECT_EQ(orig_chunk_hold - (int64_t)chunk->hold(), orig_hold - hold_);
}

TEST_F(TestChunkMgr, alloc_co_chunk)
{
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  int NORMAL_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  {
    AChunk *chunk = alloc_chunk(NORMAL_SIZE);
    free_chunk(chunk);
    chunk = alloc_chunk(LARGE_SIZE);
    free_chunk(chunk);
  }
  EXPECT_EQ(1, slots_[1]->get_pushes());
  EXPECT_EQ(0, slots_[1]->get_pops());
  EXPECT_EQ(1, slots_[0]->get_pushes());
  EXPECT_EQ(0, slots_[0]->get_pops());
  set_limit(hold_);
  auto *chunk = alloc_co_chunk(NORMAL_SIZE);
  EXPECT_TRUE(chunk != NULL);
  EXPECT_EQ(1, slots_[1]->get_pops());
  chunk = alloc_co_chunk(NORMAL_SIZE);
  EXPECT_TRUE(chunk != NULL);
  EXPECT_EQ(1, slots_[0]->get_pops());
}

TEST_F(TestChunkMgr, FreeListBasic)
{
  {
    AChunk *chunk = alloc_chunk(0);
    free_chunk(chunk);
    EXPECT_EQ(1, slots_[0]->get_pushes());
  }
  {
    AChunk *chunk = alloc_chunk(0);
    free_chunk(chunk);
    EXPECT_EQ(2, slots_[0]->get_pushes());
    EXPECT_EQ(1, slots_[0]->get_pops());
  }
  {
    AChunk *chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
    free_chunk(chunk);
    EXPECT_EQ(3, slots_[0]->get_pushes());
    EXPECT_EQ(2, slots_[0]->get_pops());
  }
}

TEST_F(TestChunkMgr, sync_wash)
{
  set_limit(1LL<<30);
  int NORMAL_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  slots_[0]->set_max_chunk_cache_size(1LL<<30);
  slots_[1]->set_max_chunk_cache_size(1LL<<30);
  AChunk *chunks[16][2] = {};
  for (int i = 0; i < 16; ++i) {
    chunks[i][0] = alloc_chunk(NORMAL_SIZE);
    chunks[i][1] = alloc_chunk(LARGE_SIZE);
  }
  for (int i = 0; i < 16; ++i) {
    for (int j = 0; j < 2; ++j) {
      free_chunk(chunks[i][j]);
      chunks[i][j] = NULL;
    }
  }
  int64_t hold = get_freelist_hold();
  EXPECT_EQ(hold, hold_);
  EXPECT_EQ(16, slots_[0]->count());
  EXPECT_EQ(16, slots_[1]->count());
  int64_t washed_size = sync_wash();
  EXPECT_EQ(hold, washed_size);
  EXPECT_EQ(0, hold_);
  EXPECT_EQ(0, slots_[0]->count());
  EXPECT_EQ(0, slots_[1]->count());
}
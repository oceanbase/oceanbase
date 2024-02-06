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
  {
    large_free_list_.set_max_chunk_cache_size(AChunkList::DEFAULT_MAX_CHUNK_CACHE_SIZE);
  }
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
  int normal_hold = 0;
  {
    int64_t hold = 0;
    AChunk *chunks[1024] = {};
    for (int i = 0; i < 1024; i++) {
      chunks[i] = alloc_chunk(NORMAL_SIZE);
      normal_hold = chunks[i]->hold();
      hold += normal_hold;
    }
    set_max_chunk_cache_size(hold - normal_hold);
    for (int i = 0; i < 1024; i++) {
      free_chunk(chunks[i]);
    }
    EXPECT_EQ(1023, free_list_.get_pushes());
    EXPECT_EQ(0, free_list_.get_pops());
    EXPECT_EQ(0, large_free_list_.get_pushes());
    EXPECT_EQ(0, large_free_list_.get_pops());
    hold -= normal_hold;
    EXPECT_EQ(hold, hold_);

    // direct alloc 4M
    {
      auto *chunk = alloc_chunk(LARGE_SIZE);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(0, free_list_.get_pops());
      EXPECT_EQ(0, large_free_list_.get_pops());
      hold += chunk->hold();
      EXPECT_EQ(hold, hold_);
    }

    // wash alloc
    {
      set_limit(hold);
      auto *chunk = alloc_chunk(LARGE_SIZE);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(2, free_list_.get_pops());
      hold = hold - normal_hold * 2 + chunk->hold();
      EXPECT_EQ(hold, hold_);
    }
  }
}

TEST_F(TestChunkMgr, LargeChunk)
{
  int NORMAL_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  int large_hold = 0;
  {
    int64_t hold = 0;
    AChunk *chunks[1024] = {};
    for (int i = 0; i < 1024; i++) {
      chunks[i] = alloc_chunk(LARGE_SIZE);
      large_hold = chunks[i]->hold();
      hold += large_hold;
    }
    set_max_large_chunk_cache_size(hold - large_hold);
    for (int i = 0; i < 1024; i++) {
      free_chunk(chunks[i]);
    }
    EXPECT_EQ(1023, large_free_list_.get_pushes());
    EXPECT_EQ(0, large_free_list_.get_pops());
    EXPECT_EQ(0, free_list_.get_pushes());
    EXPECT_EQ(0, free_list_.get_pops());
    hold -= large_hold;
    EXPECT_EQ(hold, hold_);

    // direct alloc 2M
    {
      auto *chunk = alloc_chunk(NORMAL_SIZE);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(0, free_list_.get_pops());
      EXPECT_EQ(0, large_free_list_.get_pops());
      hold += chunk->hold();
      EXPECT_EQ(hold, hold_);
    }

    // wash alloc
    {
      set_limit(hold);
      auto *chunk = alloc_chunk(NORMAL_SIZE);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(1, large_free_list_.get_pops());
      hold = hold - large_hold + chunk->hold();
      EXPECT_EQ(hold, hold_);
    }
  }
}

TEST_F(TestChunkMgr, HugeChunk)
{
  int NORMAL_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int LARGE_SIZE = INTACT_ACHUNK_SIZE + 100;
  int HUGE_SIZE = INTACT_ACHUNK_SIZE * 3;
  set_limit(20L<<30);
  int huge_hold = 0;
  {
    int64_t hold = 0;
    {
      int64_t temp_hold = 0;
      AChunk *chunks[1024] = {};
      for (int i = 0; i < 1024; i++) {
        chunks[i] = alloc_chunk(LARGE_SIZE);
        hold += chunks[i]->hold();
        temp_hold += chunks[i]->hold();
      }
      set_max_large_chunk_cache_size(temp_hold);
      for (int i = 0; i < 1024; i++) {
        free_chunk(chunks[i]);
      }
      EXPECT_EQ(1024, large_free_list_.get_pushes());
      EXPECT_EQ(0, large_free_list_.get_pops());
      EXPECT_EQ(0, free_list_.get_pushes());
      EXPECT_EQ(0, free_list_.get_pops());
      EXPECT_EQ(hold, hold_);
    }

    {
      int64_t temp_hold = 0;
      AChunk *chunks[1024] = {};
      for (int i = 0; i < 1024; i++) {
        chunks[i] = alloc_chunk(NORMAL_SIZE);
        hold += chunks[i]->hold();
        temp_hold += chunks[i]->hold();
      }
      set_max_chunk_cache_size(temp_hold);
      for (int i = 0; i < 1024; i++) {
        free_chunk(chunks[i]);
      }
      EXPECT_EQ(1024, free_list_.get_pushes());
      EXPECT_EQ(0, free_list_.get_pops());
      EXPECT_EQ(1024, large_free_list_.get_pushes());
      EXPECT_EQ(0, large_free_list_.get_pops());
      EXPECT_EQ(hold, hold_);
    }

    // direct alloc huge
    {
      auto *chunk = alloc_chunk(HUGE_SIZE);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(0, free_list_.get_pops());
      EXPECT_EQ(0, large_free_list_.get_pops());
      hold += chunk->hold();
      EXPECT_EQ(hold, hold_);
    }

    // wash alloc
    {
      set_limit(hold);
      auto *chunk = alloc_chunk(free_list_.hold() - 100);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(0, free_list_.hold());
      EXPECT_NE(0, large_free_list_.hold());

      chunk = alloc_chunk(large_free_list_.hold() - 100);
      EXPECT_TRUE(NULL != chunk);
      EXPECT_EQ(0, large_free_list_.hold());
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
  EXPECT_EQ(0, large_free_list_.get_pushes());
  int64_t orig_chunk_hold = chunk->hold();
  int64_t orig_hold = hold_;
  free_chunk(chunk);
  EXPECT_EQ(1, large_free_list_.get_pushes());
  EXPECT_EQ(0, large_free_list_.get_pops());
  chunk = alloc_chunk(LARGE_SIZE - ps * 3);
  EXPECT_EQ(1, large_free_list_.get_pops());
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
  EXPECT_EQ(0, large_free_list_.get_pushes());
  int64_t orig_chunk_hold = chunk->hold();
  int64_t orig_hold = hold_;
  free_chunk(chunk);
  EXPECT_EQ(1, large_free_list_.get_pushes());
  EXPECT_EQ(0, large_free_list_.get_pops());
  chunk = alloc_chunk(LARGE_SIZE + ps * 3);
  EXPECT_EQ(1, large_free_list_.get_pops());
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
  EXPECT_EQ(0, large_free_list_.get_pushes());
  int64_t orig_chunk_hold = chunk->hold();
  int64_t orig_hold = hold_;
  free_chunk(chunk);
  EXPECT_EQ(1, large_free_list_.get_pushes());
  EXPECT_EQ(0, large_free_list_.get_pops());
  need_fail_ = true;
  chunk = alloc_chunk(LARGE_SIZE - ps * 3);
  EXPECT_EQ(1, large_free_list_.get_pushes());
  EXPECT_EQ(1, large_free_list_.get_pops());
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
  EXPECT_EQ(1, large_free_list_.get_pushes());
  EXPECT_EQ(0, large_free_list_.get_pops());
  EXPECT_EQ(1, free_list_.get_pushes());
  EXPECT_EQ(0, free_list_.get_pops());
  set_limit(hold_);
  auto *chunk = alloc_co_chunk(NORMAL_SIZE);
  EXPECT_TRUE(chunk != NULL);
  EXPECT_EQ(1, free_list_.get_pops());
  chunk = alloc_co_chunk(NORMAL_SIZE);
  EXPECT_TRUE(chunk != NULL);
  EXPECT_EQ(1, large_free_list_.get_pops());
}

TEST_F(TestChunkMgr, FreeListBasic)
{
  {
    AChunk *chunk = alloc_chunk(0);
    free_chunk(chunk);
    EXPECT_EQ(1, free_list_.get_pushes());
  }
  {
    AChunk *chunk = alloc_chunk(0);
    free_chunk(chunk);
    EXPECT_EQ(2, free_list_.get_pushes());
    EXPECT_EQ(1, free_list_.get_pops());
  }
  {
    AChunk *chunk = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
    free_chunk(chunk);
    EXPECT_EQ(3, free_list_.get_pushes());
    EXPECT_EQ(2, free_list_.get_pops());
  }
}

TEST_F(TestChunkMgr, FreeListManyChunk)
{
  AChunk *chunks[1024] = {};
  for (int i = 0; i < 1024; i++) {
    chunks[i] = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
  }
  for (int i = 0; i < 1024; i++) {
    free_chunk(chunks[i]);
  }
  EXPECT_EQ(AChunkList::DEFAULT_MAX_CHUNK_CACHE_SIZE/INTACT_ACHUNK_SIZE, free_list_.get_pushes());
  EXPECT_EQ(0, free_list_.get_pops());

  for (int i = 0; i < 1024; i++) {
    chunks[i] = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
  }
  for (int i = 0; i < 1024; i++) {
    free_chunk(chunks[i]);
  }
  EXPECT_EQ(2* AChunkList::DEFAULT_MAX_CHUNK_CACHE_SIZE/INTACT_ACHUNK_SIZE, free_list_.get_pushes());
  EXPECT_EQ(AChunkList::DEFAULT_MAX_CHUNK_CACHE_SIZE/INTACT_ACHUNK_SIZE, free_list_.get_pops());
}

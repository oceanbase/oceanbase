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
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


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
  EXPECT_EQ(500, free_list_.get_pushes());
  EXPECT_EQ(0, free_list_.get_pops());

  for (int i = 0; i < 1024; i++) {
    chunks[i] = alloc_chunk(OB_MALLOC_BIG_BLOCK_SIZE);
  }
  for (int i = 0; i < 1024; i++) {
    free_chunk(chunks[i]);
  }
  EXPECT_EQ(500*2, free_list_.get_pushes());
  EXPECT_EQ(500, free_list_.get_pops());
}

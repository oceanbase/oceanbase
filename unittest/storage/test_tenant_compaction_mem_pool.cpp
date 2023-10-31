/*
 * test_tenant_compaction_mem_pool.cpp
 * Author:
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <thread>

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "storage/compaction/ob_compaction_memory_pool.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/compaction/ob_compaction_memory_context.h"


namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace compaction;

class TestTenantCompactionMemPool : public ::testing::Test
{
public:
  TestTenantCompactionMemPool();
  virtual ~TestTenantCompactionMemPool() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();

  void batch_alloc_blocks(
      ObTenantCompactionMemPool *mem_pool,
      ObCompactionBufferBlock *buffers,
      int64_t batch_size);
  void batch_free_blocks(
      ObTenantCompactionMemPool *mem_pool,
      ObCompactionBufferBlock *buffers,
      int64_t start_idx,
      int64_t end_idx);

private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObTenantCompactionMemPool *mem_pool_;
};

TestTenantCompactionMemPool::TestTenantCompactionMemPool()
  : tenant_id_(1),
    tenant_base_(tenant_id_),
    mem_pool_(nullptr)
{
}

void TestTenantCompactionMemPool::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestTenantCompactionMemPool::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTenantCompactionMemPool::SetUp()
{
  int ret = OB_SUCCESS;

  mem_pool_ = OB_NEW(ObTenantCompactionMemPool, ObModIds::TEST);

  tenant_base_.set(mem_pool_);
  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(tenant_id_, MTL_ID());
  ret = mem_pool_->init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(mem_pool_, MTL(ObTenantCompactionMemPool *));
}

void TestTenantCompactionMemPool::TearDown()
{
  mem_pool_->destroy();
  ObTenantEnv::set_tenant(nullptr);
}

void TestTenantCompactionMemPool::batch_alloc_blocks(
    ObTenantCompactionMemPool *mem_pool,
    ObCompactionBufferBlock *buffers,
    int64_t batch_size)
{
  for (int64_t i = 0; i < batch_size; ++i) {
    ObCompactionBufferBlock &cur_block = buffers[i];
    mem_pool->alloc(mem_pool->get_block_size(), cur_block);
    ASSERT_TRUE(!cur_block.empty());
  }
}

void TestTenantCompactionMemPool::batch_free_blocks(
    ObTenantCompactionMemPool *mem_pool,
    ObCompactionBufferBlock *buffers,
    int64_t start_idx,
    int64_t end_idx)
{
  for (int64_t i = start_idx; i < end_idx; ++i) {
    ObCompactionBufferBlock &cur_block = buffers[i];
    ASSERT_TRUE(!cur_block.empty());

    mem_pool->free(cur_block);
  }
}



namespace unittest
{

TEST_F(TestTenantCompactionMemPool, basic_test)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  EXPECT_EQ(0, mem_pool->chunk_list_.get_size());
}


TEST_F(TestTenantCompactionMemPool, alloc_and_free_test)
{
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  const int64_t curr_total_block_num = mem_pool->get_total_block_num();
  EXPECT_EQ(0, curr_total_block_num);

  // alloc 8 blocks
  ObCompactionBufferBlock buffers[8];
  batch_alloc_blocks(mem_pool, buffers, 8);
  EXPECT_EQ(8, mem_pool->get_total_block_num());
  EXPECT_EQ(8, mem_pool->used_block_num_);

  // free 8 blocks
  batch_free_blocks(mem_pool, buffers, 0, 8);
  EXPECT_EQ(8, mem_pool->get_total_block_num());
  EXPECT_EQ(0, mem_pool->used_block_num_);


  // alloc 1 block
  ObCompactionBufferBlock cur_block;
  mem_pool->alloc(mem_pool->get_block_size(), cur_block);
  EXPECT_EQ(ObCompactionBufferBlock::CHUNK_TYPE, cur_block.type_);
  EXPECT_EQ(8, mem_pool->get_total_block_num());
  EXPECT_EQ(1, mem_pool->used_block_num_);

  // free 1 block
  mem_pool->free(cur_block);
  EXPECT_EQ(8, mem_pool->get_total_block_num());
  EXPECT_EQ(0, mem_pool->used_block_num_);
}


TEST_F(TestTenantCompactionMemPool, expand_test)
{
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  const int64_t curr_total_block_num = mem_pool->get_total_block_num();
  ASSERT_TRUE(0 == curr_total_block_num);

  // alloc 8 blocks
  ObCompactionBufferBlock buffers[8];
  batch_alloc_blocks(mem_pool, buffers, 8);
  EXPECT_EQ(8, mem_pool->get_total_block_num());
  EXPECT_EQ(8, mem_pool->used_block_num_);

  // alloc another 8 blocks, should expand
  ObCompactionBufferBlock buffers2[8];
  batch_alloc_blocks(mem_pool, buffers2, 8);
  EXPECT_EQ(16, mem_pool->get_total_block_num());
  EXPECT_EQ(16, mem_pool->used_block_num_);

  // free blocks
  batch_free_blocks(mem_pool, buffers, 0, 8);
  batch_free_blocks(mem_pool, buffers2, 0, 8);
  EXPECT_EQ(0, mem_pool->used_block_num_);
}


TEST_F(TestTenantCompactionMemPool, max_alloc_test)
{
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  int64_t curr_total_block_num = mem_pool->get_total_block_num();
  ASSERT_TRUE(0 == curr_total_block_num);

  const int64_t max_block_num = mem_pool->max_block_num_;

  ObCompactionBufferBlock buffers[128];
  batch_alloc_blocks(mem_pool, buffers, max_block_num);
  EXPECT_EQ(max_block_num, mem_pool->get_total_block_num());
  EXPECT_EQ(max_block_num, mem_pool->used_block_num_);

  // alloc 1 block, should be allocated by piece allocator
  ObCompactionBufferBlock tmp_block;
  mem_pool->alloc(mem_pool->get_block_size(), tmp_block);
  EXPECT_EQ(ObCompactionBufferBlock::PIECE_TYPE, tmp_block.type_);
  mem_pool->free(tmp_block);

  ObCompactionBufferWriter buffer_writer("test");
  buffer_writer.ensure_space(ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE);
  EXPECT_EQ(max_block_num, mem_pool->get_total_block_num());
  EXPECT_EQ(max_block_num, mem_pool->used_block_num_);
  EXPECT_EQ(ObCompactionBufferBlock::PIECE_TYPE, buffer_writer.block_.type_);

  mem_pool->free(buffer_writer.block_);

  // free all blocks
  batch_free_blocks(mem_pool, buffers, 0, max_block_num);
  EXPECT_EQ(0, mem_pool->used_block_num_);
}


TEST_F(TestTenantCompactionMemPool, shrink_test)
{
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  int64_t curr_total_block_num = mem_pool->get_total_block_num();
  ASSERT_TRUE(0 == curr_total_block_num);

  const int64_t max_block_num = mem_pool->max_block_num_;

  ObCompactionBufferBlock buffers[128];
  batch_alloc_blocks(mem_pool, buffers, max_block_num);
  EXPECT_EQ(max_block_num, mem_pool->get_total_block_num());
  EXPECT_EQ(max_block_num, mem_pool->used_block_num_);

  ObCompactionBufferBlock tmp_block;
  mem_pool->alloc_chunk(tmp_block);
  ASSERT_TRUE(tmp_block.empty()); // reached the up limit
  EXPECT_EQ(max_block_num, mem_pool->get_total_block_num());
  EXPECT_EQ(max_block_num, mem_pool->used_block_num_);

  // first free
  int64_t end_idx = max_block_num / 4;
  batch_free_blocks(mem_pool, buffers, 0, end_idx);
  EXPECT_EQ(max_block_num - end_idx, mem_pool->used_block_num_);
  mem_pool->try_shrink(); // nothing happen
  EXPECT_EQ(max_block_num, mem_pool->total_block_num_);

  // second free
  batch_free_blocks(mem_pool, buffers, end_idx, max_block_num);
  EXPECT_EQ(max_block_num, mem_pool->get_total_block_num());
  mem_pool->try_shrink(); // gc half of blocks
  ASSERT_TRUE(max_block_num > mem_pool->get_total_block_num());
}


TEST_F(TestTenantCompactionMemPool, writer_basic_test)
{
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  int64_t curr_total_block_num = mem_pool->get_total_block_num();
  ASSERT_TRUE(0 == curr_total_block_num);

  ObCompactionBufferWriter buffer_writer("test");
  int64_t alloc_size = ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE / 2;

  buffer_writer.ensure_space(alloc_size);
  EXPECT_EQ(alloc_size, buffer_writer.capacity_);
  EXPECT_EQ(ObCompactionBufferBlock::PIECE_TYPE, buffer_writer.block_.type_);


  alloc_size = ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE;
  buffer_writer.ensure_space(alloc_size);
  EXPECT_EQ(alloc_size, buffer_writer.capacity_);
  EXPECT_EQ(ObCompactionBufferBlock::CHUNK_TYPE, buffer_writer.block_.type_);


  alloc_size = ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE * 2;
  buffer_writer.ensure_space(alloc_size);
  EXPECT_EQ(alloc_size, buffer_writer.capacity_);
  EXPECT_EQ(ObCompactionBufferBlock::PIECE_TYPE, buffer_writer.block_.type_);
}


TEST_F(TestTenantCompactionMemPool, writer_write_test)
{
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  int64_t curr_total_block_num = mem_pool->get_total_block_num();
  ASSERT_TRUE(0 == curr_total_block_num);

  ObCompactionBufferWriter buffer_writer("test");
  int64_t alloc_size = ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE / 2;

  buffer_writer.ensure_space(alloc_size);
  EXPECT_EQ(alloc_size, buffer_writer.capacity_);
  EXPECT_EQ(ObCompactionBufferBlock::PIECE_TYPE, buffer_writer.block_.type_);

  char write_str[] = "LEBRON is GOAT!";
  int64_t str_len = sizeof(write_str);
  buffer_writer.write(write_str, str_len);
  EXPECT_EQ(str_len, buffer_writer.pos_);

  alloc_size = ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE;
  buffer_writer.ensure_space(alloc_size);
  EXPECT_EQ(alloc_size, buffer_writer.capacity_);
  EXPECT_EQ(alloc_size, buffer_writer.block_.buffer_size_);
  EXPECT_EQ(str_len, buffer_writer.pos_);
  EXPECT_EQ(ObCompactionBufferBlock::CHUNK_TYPE, buffer_writer.block_.type_);

  char read_str[64];
  MEMCPY(read_str, buffer_writer.data_, buffer_writer.pos_);
  EXPECT_EQ(0, MEMCMP(write_str, read_str, str_len));

  alloc_size = ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE * 2;
  buffer_writer.ensure_space(alloc_size);
  EXPECT_EQ(alloc_size, buffer_writer.capacity_);
  EXPECT_EQ(ObCompactionBufferBlock::PIECE_TYPE, buffer_writer.block_.type_);
  EXPECT_EQ(str_len, buffer_writer.pos_);

  char read_str2[64];
  MEMCPY(read_str2, buffer_writer.data_, buffer_writer.pos_);
  EXPECT_EQ(0, MEMCMP(write_str, read_str2, str_len));
}


TEST_F(TestTenantCompactionMemPool, monitor_buffer_test)
{
  ObTenantCompactionMemPool *mem_pool = MTL(ObTenantCompactionMemPool *);
  ASSERT_TRUE(NULL != mem_pool);
  int64_t curr_total_block_num = mem_pool->get_total_block_num();
  ASSERT_TRUE(0 == curr_total_block_num);

  ObArenaAllocator inner_arena;
  ObTabletMergeDagParam param;
  ObCompactionMemoryContext mem_ctx(param, inner_arena);

  ObCompactionBufferWriter buffer_writer("test");
  buffer_writer.ref_mem_ctx_ = &mem_ctx;

  int64_t alloc_size = ObCompactionBufferChunk::DEFAULT_BLOCK_SIZE;
  buffer_writer.ensure_space(alloc_size);
  EXPECT_EQ(alloc_size, buffer_writer.capacity_);

  mem_ctx.mem_click();
  ASSERT_TRUE(0 != mem_ctx.get_total_mem_peak());
}


} // namespace unittest
} // namespace oceanbase


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -f test_compaction_mem_pool.log*");
  OB_LOGGER.set_file_name("test_compaction_mem_pool.log", true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_max_file_size(256*1024*1024);
  return RUN_ALL_TESTS();
}
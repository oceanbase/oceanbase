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
#include <sys/mman.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/alloc/ob_memory_placeholder.h"
#include "lib/rc/context.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

TEST(TestMallocAllocator, create_and_add_tenant_allocator)
{
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  const int64_t limit = 10 * 1000 * 1000;
  const uint64_t small_tenant_id = 200;
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(small_tenant_id));
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(small_tenant_id, limit));
  ASSERT_EQ(limit, malloc_allocator->get_tenant_limit(small_tenant_id));
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(small_tenant_id));
  ASSERT_EQ(limit, malloc_allocator->get_tenant_limit(small_tenant_id));

  const uint64_t big_tenant_id = 20000 + 10000;
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(big_tenant_id));
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(big_tenant_id, limit));
  ASSERT_EQ(limit, malloc_allocator->get_tenant_limit(big_tenant_id));
  ASSERT_EQ(OB_ENTRY_EXIST, malloc_allocator->create_and_add_tenant_allocator(big_tenant_id));
  ASSERT_EQ(limit, malloc_allocator->get_tenant_limit(big_tenant_id));

  const uint64_t tenant_id = 20000;
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
  auto tenant_allocator = malloc_allocator->get_tenant_ctx_allocator(big_tenant_id, ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_TRUE(NULL != tenant_allocator);

}

TEST(TestMallocAllocator, idle)
{
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  const uint64_t tenant_id = 201;
  const uint64_t ctx_id = 1;

  ASSERT_EQ(OB_TENANT_NOT_EXIST, malloc_allocator->set_tenant_ctx_idle(tenant_id, ctx_id,
                                                                       OB_MALLOC_BIG_BLOCK_SIZE));

  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
  auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, ctx_id);
  ASSERT_TRUE(NULL != ta);
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(tenant_id, 1024 * 1024 * 1024));

  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_ctx_idle(tenant_id, ctx_id,
                                                              OB_MALLOC_BIG_BLOCK_SIZE));
}

TEST(TestMallocAllocator, ob_malloc_align)
{
  void *ptr = ob_malloc_align(1, 4, "test");
  ASSERT_TRUE(ptr != NULL);
  ASSERT_EQ(0, (int64_t)ptr % 16);

  ptr = ob_malloc_align(4096, 4, "test");
  ASSERT_TRUE(ptr != NULL);
  ASSERT_EQ(0, (int64_t)ptr % 4096);
}

// Helper function: get private page_size_
static int64_t get_sys_page_size()
{
  return sysconf(_SC_PAGESIZE);
}

// Helper function: calculate chunk_placeholder_size
static int64_t get_chunk_placeholder_size()
{
  const int64_t CHUNK_ALLOC_SIZE = ACHUNK_SIZE - AOBJECT_META_SIZE;
  int64_t page_size = get_sys_page_size();
  return (CHUNK_ALLOC_SIZE / page_size - 1) * page_size;
}

TEST(TestObMemoryPlaceholder, Basic)
{
  int64_t tenant_id = 1002;
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  malloc_allocator->create_and_add_tenant_allocator(tenant_id);
  int64_t start_hold = malloc_allocator->get_tenant_hold(tenant_id);

  MemoryContext mem_ctx;
  ContextParam param;
  param.set_mem_attr(tenant_id, "placeholder_mem", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(mem_ctx, param));

  ObMemoryPlaceholder placeholder(tenant_id);
  placeholder.set_allocator(
      static_cast<ObAllocator *>(&mem_ctx->get_malloc_allocator()));

  // 0. Without allocator: memory_placeholder_sync should fail with OB_ALLOCATE_MEMORY_FAILED
  {
    ObMemoryPlaceholder unbound(tenant_id);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, unbound.memory_placeholder_sync(1));
  }

  // 1. Initial state
  EXPECT_EQ(0, placeholder.get_placeholder_size());

  // 2. Request 0 size
  EXPECT_EQ(OB_SUCCESS, placeholder.memory_placeholder_sync(0));
  EXPECT_EQ(0, placeholder.get_placeholder_size());

  // 3. Request invalid size
  EXPECT_EQ(OB_INVALID_ARGUMENT, placeholder.memory_placeholder_sync(-1));

  // 4. Request size less than one chunk
  int64_t chunk_hold_size = get_chunk_placeholder_size();
  int64_t small_size = chunk_hold_size / 2;
  EXPECT_EQ(OB_SUCCESS, placeholder.memory_placeholder_sync(small_size));
  EXPECT_EQ(chunk_hold_size, placeholder.get_placeholder_size());

  // Verify tenant hold increased
  int64_t current_hold = malloc_allocator->get_tenant_hold(tenant_id);
  EXPECT_GT(current_hold, start_hold);

  // 5. Request exactly one chunk size
  EXPECT_EQ(OB_SUCCESS, placeholder.memory_placeholder_sync(chunk_hold_size));
  EXPECT_EQ(chunk_hold_size, placeholder.get_placeholder_size());

  // 6. Request size larger than one chunk (e.g. 1.5 chunks -> 2 chunks)
  EXPECT_EQ(OB_SUCCESS, placeholder.memory_placeholder_sync(chunk_hold_size + small_size));
  EXPECT_EQ(2 * chunk_hold_size, placeholder.get_placeholder_size());

  // 7. Expand to 5 chunks
  EXPECT_EQ(OB_SUCCESS, placeholder.memory_placeholder_sync(5 * chunk_hold_size));
  EXPECT_EQ(5 * chunk_hold_size, placeholder.get_placeholder_size());
  current_hold = malloc_allocator->get_tenant_hold(tenant_id);
  EXPECT_NEAR(current_hold, 5 * chunk_hold_size, 1024 * 1024 * 0.5);

  // 8. Shrink to 3 chunks
  EXPECT_EQ(OB_SUCCESS, placeholder.memory_placeholder_sync(3 * chunk_hold_size));
  EXPECT_EQ(3 * chunk_hold_size, placeholder.get_placeholder_size());

  // 9. Shrink to 0
  EXPECT_EQ(OB_SUCCESS, placeholder.memory_placeholder_sync(0));
  EXPECT_EQ(0, placeholder.get_placeholder_size());

  placeholder.destroy();
  DESTROY_CONTEXT(mem_ctx);
}

int main(int argc, char *argv[])
{
  signal(49, SIG_IGN);
  OB_LOGGER.set_file_name("t.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

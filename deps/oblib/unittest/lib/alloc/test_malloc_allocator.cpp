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
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_malloc.h"

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
  ta->set_tenant_memory_mgr();
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

int main(int argc, char *argv[])
{
  signal(49, SIG_IGN);
  OB_LOGGER.set_file_name("t.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

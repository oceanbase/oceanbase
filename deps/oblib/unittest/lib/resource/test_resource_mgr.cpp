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
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/container/ob_array.h"
#include "lib/resource/ob_resource_mgr.h"
#undef private
namespace oceanbase
{
using namespace common;
namespace lib
{
TEST(TestTenantMemoryMgr, basic)
{
  ObTenantMemoryMgr memory_mgr(1);
  const int64_t limit = 1 * 1024 * 1024 * 1024;
  memory_mgr.set_limit(1 * 1024 * 1024 * 1024); //1G;
  ASSERT_TRUE(NULL ==  memory_mgr.alloc_chunk(-1, ObMemAttr()));
  ObMemAttr attr;
  attr.tenant_id_ = 2;
  ASSERT_TRUE(NULL == memory_mgr.alloc_chunk(1024, attr));
  attr.tenant_id_ = 1;
  attr.ctx_id_ = 0;

  // alloc, then free
  const int64_t alloc_size = ACHUNK_SIZE;
  const int64_t aligned_size = CHUNK_MGR.aligned(alloc_size);
  const int64_t max_alloc_count = limit / aligned_size;
  ObArray<void *> chunks;
  for (int64_t i = 0; i < max_alloc_count; ++i) {
    ObMemAttr attr;
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    AChunk *chunk = NULL;
    chunk = memory_mgr.alloc_chunk(alloc_size, attr);
    ASSERT_TRUE(NULL != chunk);
    ASSERT_EQ(OB_SUCCESS, chunks.push_back(chunk));
  }
  ASSERT_EQ(limit, memory_mgr.get_sum_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_rpc_hold());
  ASSERT_EQ(limit / 2, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ(limit / 2, memory_mgr.get_ctx_hold_bytes()[1]);
  for (int64_t i = 2; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }

  AChunk * chunk = NULL;
  chunk = memory_mgr.alloc_chunk(alloc_size, attr);
  ASSERT_TRUE(NULL == chunk);
  for (int64_t i = 0; i < chunks.count(); ++i) {
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_rpc_hold());
  for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }
  chunks.reuse();

  // 10 cache alloc and free
  for (int64_t i = 0; i < max_alloc_count - 10; ++i) {
    ObMemAttr attr;
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    AChunk *chunk = NULL;
    chunk = memory_mgr.alloc_chunk(alloc_size, attr);
    ASSERT_TRUE(NULL != chunk);
    ASSERT_EQ(OB_SUCCESS, chunks.push_back(chunk));
  }
  ObArray<void *> cache_mbs;
  for (int64_t i = 0; i < 10; ++i) {
    void *ptr = NULL;
    ptr = memory_mgr.alloc_cache_mb(alloc_size);
    ASSERT_TRUE(NULL != ptr);
    ASSERT_EQ(OB_SUCCESS, cache_mbs.push_back(ptr));
  }
  ASSERT_EQ(limit, memory_mgr.get_sum_hold());
  ASSERT_EQ(aligned_size * 10, memory_mgr.get_cache_hold());
  ASSERT_EQ(10, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_rpc_hold());
  ASSERT_EQ((max_alloc_count - 10) * aligned_size /2, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ((max_alloc_count - 10) * aligned_size /2, memory_mgr.get_ctx_hold_bytes()[1]);
  for (int64_t i = 2; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }
  chunk = NULL;
  chunk = memory_mgr.alloc_chunk(alloc_size, attr);
  ASSERT_TRUE(NULL == chunk);
  for (int64_t i = 0; i < chunks.count(); ++i) {
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  for (int64_t i = 0; i < cache_mbs.count(); ++i) {
    memory_mgr.free_cache_mb(cache_mbs.at(i));
  }
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_rpc_hold());
  for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }
  chunks.reuse();
  cache_mbs.reuse();
}

class FakeCacheWasher : public ObICacheWasher
{
public:
  FakeCacheWasher(const uint64_t tenant_id, const int64_t mb_size)
    : tenant_id_(tenant_id), mb_size_(mb_size), mb_blocks_(NULL)
  {
  }
  virtual ~FakeCacheWasher() {}

  int erase_cache(const uint64_t ) override { return 0; }
  int alloc_mb(ObTenantMemoryMgr &mgr)
  {
    int ret = OB_SUCCESS;
    void *ptr = NULL;
    if (NULL == (ptr = mgr.alloc_cache_mb(mb_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "alloc_cache_mb failed", K(ret), K_(tenant_id), K_(mb_size));
    } else {
      ObCacheMemBlock *block = new (ptr) ObCacheMemBlock();
      block->next_ = mb_blocks_;
      mb_blocks_ = block;
    }
    return ret;
  }
  int sync_wash_mbs(const uint64_t tenant_id, const int64_t wash_size,
                    ObCacheMemBlock *&wash_blocks)
  {
    UNUSED(tenant_id);
    int ret = OB_SUCCESS;
    int64_t left_to_washed = wash_size;
    ObCacheMemBlock *washed_blocks = NULL;
    while (OB_SUCC(ret) && left_to_washed > 0) {
      if (NULL == mb_blocks_) {
        ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
        LIB_LOG(WARN, "free block not enough", K(ret), K(wash_size));
      } else {
        ObCacheMemBlock *free_block = mb_blocks_;
        mb_blocks_ = free_block->next_;
        free_block->next_ = washed_blocks;
        washed_blocks = free_block;
        left_to_washed -= CHUNK_MGR.aligned(mb_size_);
      }
    }
    if (OB_SUCC(ret)) {
      wash_blocks = washed_blocks;
    }
    return ret;
  }
  void free_mbs(ObTenantMemoryMgr &mgr)
  {
    ObCacheMemBlock *mb = mb_blocks_;
    while (NULL != mb) {
      mb_blocks_ = mb_blocks_->next_;
      mgr.free_cache_mb(mb);
      mb = mb_blocks_;
    }
  }
private:
  uint64_t tenant_id_;
  int64_t mb_size_;
  ObCacheMemBlock *mb_blocks_;
};

TEST(TestTenantMemoryMgr, sync_wash)
{
  int ret = OB_SUCCESS;
  const int64_t limit = 2L * 1024L * 1024L * 1024L;
  oceanbase::lib::set_memory_limit(limit);
  FakeCacheWasher washer(1, ACHUNK_SIZE);
  ObTenantMemoryMgr memory_mgr(1);
  ObArray<void *> chunks;
  chunks.reserve(512);
  const int64_t tenant_limit = 2 * limit;
  memory_mgr.set_limit(tenant_limit);
  int64_t mb_count = 0;
  while (OB_SUCC(washer.alloc_mb(memory_mgr))) {
    ++mb_count;
  }
  ret = OB_SUCCESS;
  // check cache stat
  const int64_t aligned_size = CHUNK_MGR.aligned(ACHUNK_SIZE);
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_cache_hold());
  ASSERT_EQ(mb_count, memory_mgr.get_cache_item_count());
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_sum_hold());

  memory_mgr.set_cache_washer(washer);
  int64_t alloc_count = 0;
  ObMemAttr attr;
  attr.tenant_id_ = 1;
  attr.ctx_id_ = 1;
  while (OB_SUCC(ret)) {
    void *ptr = NULL;
    if (NULL == (ptr = memory_mgr.alloc_chunk(ACHUNK_SIZE, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "alloc_chunk failed", K(ret));
    } else if (OB_FAIL(chunks.push_back(ptr))) {
      LIB_LOG(WARN, "push_back failed", K(ret));
    } else {
      ++alloc_count;
    }
  }
  // check stat, left one mb in cache
  mb_count -= alloc_count;
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_cache_hold());
  ASSERT_EQ(mb_count, memory_mgr.get_cache_item_count());
  ASSERT_EQ(aligned_size * (mb_count + alloc_count), memory_mgr.get_sum_hold());

  for (int64_t i = 0; i < chunks.count(); ++i) {
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  for (int64_t i = 0; i < mb_count; ++i) {
    washer.free_mbs(memory_mgr);
  }
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[1]);
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
}

TEST(TestTenantMemoryMgr, DISABLED_large_sync_wash)
{
  int ret = OB_SUCCESS;
  const int64_t limit = 1L * 1024L * 1024L * 1024L;
  const int64_t aligned_size = CHUNK_MGR.aligned(ACHUNK_SIZE);
  oceanbase::lib::set_memory_limit(limit);
  FakeCacheWasher washer(1, ACHUNK_SIZE);
  ObTenantMemoryMgr memory_mgr(1);
  ObArray<void *> chunks;
  chunks.reserve(512);
  const int64_t tenant_limit = 2 * limit;
  memory_mgr.set_limit(tenant_limit);
  int64_t mb_count = 0;
  while (OB_SUCC(washer.alloc_mb(memory_mgr))) {
    ++mb_count;
  }
  ret = OB_SUCCESS;

  memory_mgr.set_cache_washer(washer);
  int64_t alloc_count = 0;
  ObMemAttr attr;
  attr.tenant_id_ = 1;
  attr.ctx_id_ = 1;
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_sum_hold());
  while (OB_SUCC(ret)) {
    void *ptr = NULL;
    if (NULL == (ptr = memory_mgr.alloc_chunk(ACHUNK_SIZE * 2, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "alloc_chunk failed", K(ret));
    } else if (OB_FAIL(chunks.push_back(ptr))) {
      LIB_LOG(WARN, "push_back failed", K(ret));
    } else {
      ++alloc_count;
    }
  }
  // left 4 mb
  ASSERT_EQ((mb_count - 2 * alloc_count) * aligned_size, memory_mgr.get_cache_hold());
  ASSERT_EQ((mb_count - 2 * alloc_count) , memory_mgr.get_cache_item_count());

  ASSERT_TRUE(mb_count - 2 * alloc_count <= 4);

  for (int64_t i = 0; i < chunks.count(); ++i) {
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  washer.free_mbs(memory_mgr);
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[1]);
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
}

TEST(TestResourceMgr, basic)
{
  ObResourceMgr mgr;
  FakeCacheWasher washer(1, ACHUNK_SIZE);
  ObTenantResourceMgrHandle tenant_mgr;
  ObTenantResourceMgrHandle tenant_mgrs[ObResourceMgr::MAX_TENANT_COUNT];
  ASSERT_EQ(OB_NOT_INIT, mgr.set_cache_washer(washer));
  ASSERT_EQ(OB_NOT_INIT, mgr.get_tenant_resource_mgr(1, tenant_mgr));
  ASSERT_EQ(OB_SUCCESS, mgr.init());

  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.get_tenant_resource_mgr(OB_INVALID_ID, tenant_mgr));
  ASSERT_EQ(OB_INIT_TWICE, mgr.init());

  ASSERT_EQ(OB_SUCCESS, mgr.set_cache_washer(washer));
  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr.get_tenant_resource_mgr(i, tenant_mgrs[i]));
    ASSERT_EQ(i, tenant_mgrs[i].get_memory_mgr()->get_tenant_id());
  }

  ObArray<void *> mbs;
  for (int64_t i = 0; i < 10; ++i) {
    void *ptr = NULL;
    ptr = tenant_mgrs[i].get_memory_mgr()->alloc_cache_mb(ACHUNK_SIZE);
    ASSERT_TRUE(NULL != ptr);
    ASSERT_EQ(OB_SUCCESS, mbs.push_back(ptr));
  }

  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(CHUNK_MGR.aligned(ACHUNK_SIZE), tenant_mgrs[i].get_memory_mgr()->get_cache_hold());
    ASSERT_EQ(i, tenant_mgrs[i].get_memory_mgr()->get_tenant_id());
  }
  for (int64_t i = 0; i < 10; ++i) {
    tenant_mgrs[i].get_memory_mgr()->free_cache_mb(mbs.at(i));
  }

  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    ObTenantResourceMgrHandle tenant_mgr;
    ASSERT_EQ(OB_SUCCESS, mgr.get_tenant_resource_mgr(i, tenant_mgr));
    ASSERT_EQ(0, tenant_mgr.get_memory_mgr()->get_cache_hold());
  }
  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    tenant_mgrs[i].reset();
  }

  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    ObTenantResourceMgr *resource_mgr = NULL;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr.get_tenant_resource_mgr_unsafe(i, resource_mgr));
  }
  mgr.destroy();
}
}//end namespace lib
}//end namespace oceanbase

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

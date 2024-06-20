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
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#include "lib/alloc/object_mgr.h"
#undef private
#include "lib/alloc/alloc_func.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/coro/testing.h"
#include "lib/random/ob_random.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_array.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

TEST(TestTenantAllocator, CtxAlloc)
{
  CHUNK_MGR.set_max_chunk_cache_size(1<<20);
  ObTenantCtxAllocator ta(123, 1);
  ta.set_tenant_memory_mgr();
  ta.set_limit(INT64_MAX);
  ObMemAttr attr(123, "TenantCtxAlloc", 1);
  const int64_t hold = get_memory_hold();

  cout << "current hold: " << hold << endl;

  set_memory_limit(hold + (3<<20));
  EXPECT_TRUE(NULL != ta.alloc(1, attr));

  ta.print_memory_usage();
}

TEST(TestTenantAllocator, SysLimit)
{
  ObTenantCtxAllocator ta(324);
  ta.set_tenant_memory_mgr();
  ta.set_limit(INT64_MAX);
  ObMemAttr attr(324, "TenantCtxAlloc");
  const int64_t hold = get_memory_hold();

  cout << "current hold: " << hold << endl;

  set_memory_limit(hold);
  EXPECT_EQ(NULL, ta.alloc(1, attr));

  set_memory_limit(hold + (1<<20));
  EXPECT_EQ(NULL, ta.alloc(1, attr));

  set_memory_limit(hold + (3<<20));
  EXPECT_TRUE(NULL != ta.alloc(1, attr));
}

TEST(TestTenantAllocator, TenantLimit)
{
  ObTenantCtxAllocator ta(324);
  ta.set_tenant_memory_mgr();
  ta.set_limit(INT64_MAX);
  ObMemAttr attr(324, "TenantCtxAlloc");
  const int64_t hold = get_memory_hold();

  cout << "current hold: " << hold << endl;

  set_memory_limit(hold);  // now, we can't allocate new memory from system
  EXPECT_FALSE(NULL != ta.alloc(1, attr));

  attr.prio_ = OB_HIGH_ALLOC;
  EXPECT_FALSE(NULL != ta.alloc(1, attr));

  ob_set_urgent_memory((2 << 20) - 1);
  EXPECT_FALSE(NULL != ta.alloc(1, attr));

  ob_set_urgent_memory(2 << 20);
  EXPECT_TRUE(NULL != ta.alloc(1, attr));
  EXPECT_TRUE(NULL != ta.alloc(1, attr));

  attr.prio_ = OB_NORMAL_ALLOC;
  EXPECT_FALSE(NULL != ta.alloc(2 << 20, attr));

  attr.prio_ = OB_HIGH_ALLOC;
  EXPECT_TRUE(NULL != ta.alloc(1, attr));
}

TEST(TestTenantAllocator, SetMemoryMgrTwice)
{
  ObTenantCtxAllocator ta(324);
  ASSERT_EQ(OB_SUCCESS, ta.set_tenant_memory_mgr());
  ASSERT_EQ(OB_INIT_TWICE, ta.set_tenant_memory_mgr());
}

TEST(TestTenantAllocator, ctx_limit)
{
  CHUNK_MGR.set_limit(1L * 1024L * 1024L * 1024L);
  const uint64_t tenant_id = 1001;
  const uint64_t ctx_id = 1;
  ObTenantCtxAllocator ta(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ta.set_tenant_memory_mgr());
  ta.set_limit(INT64_MAX);
  ObTenantCtxAllocator ctx_ta(tenant_id, ctx_id);
  ASSERT_EQ(OB_SUCCESS, ctx_ta.set_tenant_memory_mgr());

  ObMemAttr attr;
  attr.tenant_id_ = tenant_id;
  attr.ctx_id_ = ctx_id;
  const int64_t size = 512 * 1024;
  const int64_t limit = 30 * size;
  int64_t alloced = 0;
  ctx_ta.set_limit(limit);
  while (true) {
    if (NULL == ctx_ta.alloc(size, attr)) {
      break;
    } else {
      alloced += size;
    }
  }
  cout << "alloced: " << alloced << endl;
  cout << "limit: " << limit << endl;
  cout << "hold: " << ctx_ta.get_hold() << endl;
  ASSERT_TRUE(alloced > limit / 2 && alloced <= limit);
  ASSERT_TRUE(ctx_ta.get_hold() > limit / 2 && ctx_ta.get_hold() <= limit);
}

TEST(TestTenantAllocator, reserve)
{
  CHUNK_MGR.set_limit(1L * 1024L * 1024L * 1024L);
  const uint64_t tenant_id = 1003;
  const uint64_t ctx_id = 1;
  ObTenantCtxAllocator ta(tenant_id, ctx_id);
  ASSERT_EQ(OB_SUCCESS, ta.set_tenant_memory_mgr());
  const int64_t limit = 1L * 1024L * 1024L * 1024L;
  ta.set_limit(limit);

  ASSERT_EQ(nullptr, ta.head_chunk_.next_);
  const int64_t reserve_size = 2 * INTACT_ACHUNK_SIZE;
  int ret = ta.set_idle(reserve_size, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ta.chunk_cnt_, reserve_size / INTACT_ACHUNK_SIZE);
  int64_t chunk_cnt = 0;
  AChunk *chunk = &ta.head_chunk_;
  while (chunk->next_ != nullptr) {
    chunk_cnt++;
    chunk = chunk->next_;
  }
  ASSERT_EQ(ta.chunk_cnt_, chunk_cnt);
  ObMemAttr attr(tenant_id, "TenantCtxAlloc",  ctx_id);
  void *ptr = ta.alloc(1, attr);
  ASSERT_NE(nullptr, ptr);
  ASSERT_EQ(ta.chunk_cnt_, chunk_cnt - 1);
  int64_t total_alloc_size = 0;
  int64_t alloc_size = 512;
  while (true) {
    void *ptr = ta.alloc(alloc_size, attr);
    ASSERT_NE(nullptr, ptr);
    total_alloc_size += alloc_size;
    if (total_alloc_size > reserve_size) {
      ASSERT_EQ(0, ta.chunk_cnt_);
      break;
    }
  }
  chunk_cnt = ta.chunk_cnt_;
  ret = ta.set_idle(reserve_size * 2, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(chunk_cnt, ta.chunk_cnt_);

  chunk_cnt = ta.chunk_cnt_;
  ret = ta.set_idle(reserve_size * 4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(chunk_cnt, ta.chunk_cnt_);

  ret = ta.set_idle(reserve_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(chunk_cnt, ta.chunk_cnt_);
}

TEST(TestTenantAllocator, set_idle)
{
  CHUNK_MGR.set_limit(1L * 1024L * 1024L * 1024L);
  const uint64_t tenant_id = 1004;
  const uint64_t ctx_id = 1;
  ObTenantCtxAllocator ta(tenant_id, ctx_id);
  ASSERT_EQ(OB_SUCCESS, ta.set_tenant_memory_mgr());
  const int64_t limit = 1L * 1024L * 1024L * 1024L;
  ta.set_limit(limit);
  ObMemAttr attr(tenant_id, "TenantCtxAlloc", ctx_id);
  void *ptr = ta.alloc(INTACT_ACHUNK_SIZE / 2, attr);
  ASSERT_NE(nullptr, ptr);
  int64_t hold = ta.get_hold();
  ta.free(ptr);
  ASSERT_EQ(0, ta.chunk_cnt_);
  ASSERT_LT(ta.get_hold(), hold);

  const int64_t alloc_cnt = 10;
  void *ptrs[alloc_cnt];
  for (int i = 0; i < alloc_cnt; ++i) {
    ptr = ta.alloc(INTACT_ACHUNK_SIZE / 2, attr);
    ASSERT_NE(nullptr, ptr);
    ptrs[i] = ptr;
  }
  hold = ta.get_hold();
  const int64_t idle_size = alloc_cnt * INTACT_ACHUNK_SIZE;
  int ret = ta.set_idle(idle_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < alloc_cnt; ++i) {
    ta.free(ptrs[i]);
  }
  ASSERT_EQ(10, ta.chunk_cnt_);
  ASSERT_EQ(ta.get_hold(), hold);
  ret = ta.set_idle(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, ta.chunk_cnt_);
  ASSERT_LT(ta.get_hold(), hold);
}

TEST(TestTenantAllocator, idle)
{
  CHUNK_MGR.set_limit(1L * 1024L * 1024L * 1024L);
  const uint64_t tenant_id = 1005;
  const uint64_t ctx_id = 1;
  ObTenantCtxAllocator ta(tenant_id, ctx_id);
  ASSERT_EQ(OB_SUCCESS, ta.set_tenant_memory_mgr());
  const int64_t limit = 1L * 1024L * 1024L * 1024L;
  ta.set_limit(limit);

  // > limit
  ASSERT_EQ(OB_INVALID_ARGUMENT, ta.set_idle(limit + INTACT_ACHUNK_SIZE));
  // %2M != 0
  // ASSERT_EQ(OB_INVALID_ARGUMENT, ta.set_idle(INTACT_ACHUNK_SIZE + 1));
  const int64_t init_size = 10 * INTACT_ACHUNK_SIZE;
  const int64_t idle_size = 50 * INTACT_ACHUNK_SIZE;
  int ret = ta.set_idle(init_size, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ta.set_idle(idle_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t chunk_cnt = init_size/INTACT_ACHUNK_SIZE;
  ASSERT_EQ(ta.chunk_cnt_, chunk_cnt);
  int64_t hold = ta.get_hold();
  const int64_t alloc_size = 1024 * 100;
  ObMemAttr attr(tenant_id, "TenantCtxAlloc", ctx_id);
  int k  = 0;
  ObArray<void *> ptrs;
  while (hold == ta.get_hold()) {
    k++;
    void *ptr = ta.alloc(alloc_size, attr);
    ASSERT_NE(nullptr, ptr);
    ptrs.push_back(ptr);
  }
  ASSERT_GT(k, init_size / alloc_size / 2);
  ASSERT_LT(hold, ta.get_hold());
  ASSERT_EQ(0, ta.chunk_cnt_);

  while (ta.get_hold() < idle_size * 2) {
    void * ptr = ta.alloc(alloc_size, attr);
    ASSERT_NE(nullptr, ptr);
    ptrs.push_back(ptr);
  }

  ASSERT_EQ(0, ta.chunk_cnt_);
  int64_t last_chunk_cnt = ta.chunk_cnt_;
  int64_t i = 0;
  for (; i < ptrs.size() && ta.chunk_cnt_ == last_chunk_cnt; i++) {
    last_chunk_cnt = ta.chunk_cnt_;
    ta.free(ptrs[i]);
  }
  ASSERT_TRUE(i != 0 && i != ptrs.size());
  hold = ta.get_hold();
  for (; i < ptrs.size(); i++) {
    ta.free(ptrs[i]);
    ASSERT_EQ(ta.get_hold(), hold);
  }
  chunk_cnt = idle_size/INTACT_ACHUNK_SIZE;
  ASSERT_EQ(ta.chunk_cnt_, chunk_cnt);

  ret = ta.set_idle(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, ta.get_hold());
}

TEST(TestTenantAllocator, chunk_free_list_push_pop_concurrency)
{
  CHUNK_MGR.set_limit(1L * 1024L * 1024L * 1024L);
  const uint64_t tenant_id = 1002;
  const uint64_t ctx_id = 1;
  ObTenantCtxAllocator ta(tenant_id, ctx_id);
  ASSERT_EQ(OB_SUCCESS, ta.set_tenant_memory_mgr());
  ta.set_limit(INT64_MAX);

  ASSERT_EQ(nullptr, ta.head_chunk_.next_);

  int chunk_num = 0;
  int th_num = 32;
  cotesting::FlexPool([&ta, &chunk_num]() {
    int64_t start_time = ObTimeUtility::current_time();
    while (ObTimeUtility::current_time() < start_time + 10 * 1000 * 1000)
    {
      AChunk *chunk = nullptr;
      if (0 == ObRandom::rand(0, 1)) {
        chunk = ta.pop_chunk();
      } else {
        chunk = new AChunk();
        ATOMIC_INC(&chunk_num);
      }
      if (chunk != nullptr) {
        usleep(1000);
        ta.push_chunk(chunk);
      }
    }
  }, th_num).start();
  AChunk *chunk = nullptr;
  while ((chunk = ta.pop_chunk()) != nullptr)
  {
    delete chunk;
    chunk_num--;
  }
  ASSERT_EQ(0, chunk_num);
}

TEST(TestTenantAllocator, sub_ctx_id)
{
  uint64_t tenant_id = 1010;
  uint64_t ctx_id = 0;
  ObMemAttr mem_attr(tenant_id, "TestSubCtx", ctx_id);
  auto malloc_allocator = ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
  ObTenantCtxAllocator *ta = NULL;
  {
    auto guard = malloc_allocator->get_tenant_ctx_allocator(tenant_id, ctx_id);
    ta = guard.ref_allocator();
  }
  ObjectMgr &obj_mgr = ta->obj_mgr_;
  ObjectMgr *obj_mgrs = ta->obj_mgrs_;

  int size = 1<<20;
  void *ptrs[ObSubCtxIds::MAX_SUB_CTX_ID];
  memset(&ptrs, 0, sizeof(ptrs));
  for (int i = 0; i < ObSubCtxIds::MAX_SUB_CTX_ID; ++i) {
    mem_attr.sub_ctx_id_ = i;
    ptrs[i] = ob_malloc(size, mem_attr);
    ASSERT_EQ(true, NULL != ptrs[i]);
    if (NULL != ptrs[i]) {
      AObject *obj = reinterpret_cast<AObject*>((char*)ptrs[i] - AOBJECT_HEADER_SIZE);
      AChunk *chunk = AChunk::ptr2chunk(obj);
      ASSERT_EQ(&obj_mgr.root_mgr_.bs_, chunk->block_set_);
      ABlock *block = chunk->ptr2blk(obj);
      ASSERT_EQ(&obj_mgrs[i].root_mgr_.os_, block->obj_set_);
    }
  }

  if (ObSubCtxIds::MAX_SUB_CTX_ID > 2 &&
      NULL != ptrs[0] && NULL != ptrs[1]) {
    AObject *obj_0 = reinterpret_cast<AObject*>((char*)ptrs[0] - AOBJECT_HEADER_SIZE);
    AObject *obj_1 = reinterpret_cast<AObject*>((char*)ptrs[1] - AOBJECT_HEADER_SIZE);
    ASSERT_NE(AChunk::ptr2chunk(obj_0)->ptr2blk(obj_0)->obj_set_,
              AChunk::ptr2chunk(obj_1)->ptr2blk(obj_1)->obj_set_);
  }

  mem_attr.sub_ctx_id_ = ObSubCtxIds::MAX_SUB_CTX_ID + 1;
  void *ptr = ob_malloc(size, mem_attr);
  ASSERT_EQ(true, NULL == ptr);

  int64_t wash_size = ta->sync_wash(INT64_MAX);
  ASSERT_NE(0, wash_size);
  ptr = ob_malloc(size, ObMemAttr(tenant_id, "TestSubCtx", ctx_id));
  ASSERT_EQ(true, NULL != ptr);
  ASSERT_EQ(wash_size, ta->sync_wash(INT64_MAX) * ObSubCtxIds::MAX_SUB_CTX_ID);
  ob_free(ptr);
  for (int i = 0; i < ObSubCtxIds::MAX_SUB_CTX_ID; ++i) {
    if (NULL != ptrs[i]) {
      ob_free(ptrs[i]);
    }
  }

  ASSERT_EQ(OB_SUCCESS, malloc_allocator->recycle_tenant_allocator(tenant_id));

  uint64_t tenant_id_1 = 1011;
  ObMemAttr mem_attr_1(tenant_id_1, "TestSubCtx", ctx_id);
  mem_attr_1.sub_ctx_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id_1));
  ASSERT_EQ(true, NULL != ob_malloc(size, mem_attr_1));
  ASSERT_NE(OB_SUCCESS, malloc_allocator->recycle_tenant_allocator(tenant_id_1));
}

TEST(TestTenantAllocator, MERGE_RESERVE_CTX)
{
  const uint64_t tenant_id = 1002;
  ObMallocAllocator* malloc_allocator = ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
  void *ptr_0 = ob_malloc(100L<<10, ObMemAttr(tenant_id, "Test", 0));
  void *ptr_1 = ob_malloc(100L<<10, ObMemAttr(tenant_id, "Test", ObCtxIds::MERGE_RESERVE_CTX_ID));
  malloc_allocator->sync_wash(tenant_id, 0, INT64_MAX);
  AChunk *chunk_0 = AChunk::ptr2chunk(ptr_0);
  AChunk *chunk_1 = AChunk::ptr2chunk(ptr_1);
  ASSERT_NE(0, chunk_0->washed_size_);
  ASSERT_EQ(0, chunk_1->washed_size_);
}

int main(int argc, char *argv[])
{
  signal(49, SIG_IGN);
  OB_LOGGER.set_file_name("t.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

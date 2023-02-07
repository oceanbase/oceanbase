/**
 * Copyright (c) 2022 OceanBase
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
#include "lib/alloc/ob_malloc_allocator.h"
#undef private

using namespace oceanbase::lib;
using namespace oceanbase::common;

class TestMallocAllocator : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    // cleanup
    ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
    for (int i = OB_USER_TENANT_ID + 1; i < ObMallocAllocator::PRESERVED_TENANT_COUNT; i++) {
      malloc_allocator->allocators_[i] = NULL;
    }
    malloc_allocator->unrecycled_allocators_ = NULL;
  }
  virtual void TearDown() {}
};

int64_t g_ctx_id = ObCtxIds::DEFAULT_CTX_ID;

TEST(TestMallocAllocator, reserved_tenant)
{
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  for (int64_t tenant_id = 1; tenant_id < OB_USER_TENANT_ID; tenant_id++) {
    // lazy create
    void *ptr = malloc_allocator->alloc(100, ObMemAttr(tenant_id));
    ASSERT_NE(NULL, (long)ptr);
    auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id);
    ASSERT_TRUE(NULL != ta);
    ASSERT_EQ(tenant_id, ta->get_tenant_id());
    ASSERT_EQ(g_ctx_id, ta->get_ctx_id());
    ASSERT_EQ(OB_OP_NOT_ALLOW, malloc_allocator->recycle_tenant_allocator(tenant_id));
  }
}

TEST(TestMallocAllocator, user_tenant)
{
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  int64_t tenant_ids[] = {1001,
                          1001 + ObMallocAllocator::PRESERVED_TENANT_COUNT,
                          1002 + ObMallocAllocator::PRESERVED_TENANT_COUNT * 2,
                          1003 + ObMallocAllocator::PRESERVED_TENANT_COUNT * 3};
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_ids); i++) {
    int64_t tenant_id = tenant_ids[i];
    auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id);
    ASSERT_TRUE(NULL == ta);
    ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
    ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id);
    ASSERT_EQ(tenant_id, ta->get_tenant_id());
    ASSERT_EQ(1, ta->get_ref_cnt());
    {
      auto  ta2 = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id);
      ASSERT_EQ(2, ta2->get_ref_cnt());
    }
    ASSERT_EQ(1, ta->get_ref_cnt());
  }
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_ids); i++) {
    int64_t tenant_id = tenant_ids[i];
    ObTenantCtxAllocator *allocator = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id).ref_allocator();
    ASSERT_EQ(0, allocator->get_ref_cnt());
    ObMemAttr attr(tenant_id, nullptr, g_ctx_id);
    if (i == 0) {
      auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id);

      // has references
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));
      void *ptr = ta->alloc(100, attr);
      ASSERT_NE((long)ptr, NULL);

      // has unfree
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));

      // has no unfree, still has references
      ta->free(ptr);
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));

      // has no references && unfree
      ta.~ObTenantCtxAllocatorGuard();

      ASSERT_EQ(OB_SUCCESS, malloc_allocator->recycle_tenant_allocator(tenant_id));
      ASSERT_TRUE(NULL == malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id));
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, malloc_allocator->recycle_tenant_allocator(tenant_id));
    } else if (i == 1) {
      auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id);

      // has references
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));
      void *ptr = ta->alloc(100, attr);
      ASSERT_NE((long)ptr, NULL);

      // has unfree
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));

      // has no references, still has unfree
      ta.~ObTenantCtxAllocatorGuard();
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));

      // has no unfree && references
      malloc_allocator->get_tenant_ctx_allocator_unrecycled(tenant_id, g_ctx_id)->free(ptr);

      ASSERT_EQ(OB_SUCCESS, malloc_allocator->recycle_tenant_allocator(tenant_id));
      ASSERT_TRUE(NULL ==  malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id));
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, malloc_allocator->recycle_tenant_allocator(tenant_id));

      ObTenantCtxAllocator *last_allocator = allocator;
      // shouldn't reused if not exists in unrecycled_list
      ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
      ObTenantCtxAllocator *cur_allocator = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id).ref_allocator();
      ASSERT_NE(last_allocator, cur_allocator);
      // alloc pieces to make recycling fails
      cur_allocator->alloc(100, attr);
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));
      last_allocator = cur_allocator;

      // should reuse if exists in unrecycled_list
      ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
      ASSERT_EQ(last_allocator,  malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id).ref_allocator());
    } else if (i == 2) {
      // test idle cleanup
      ObTenantCtxAllocatorGuard ta;
      ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id);
      // alloc pieces to make recycling fails
      ta->alloc(100, attr);
      int64_t idle_size = 100L << 20;
      ta->set_idle(idle_size, true/*reserved*/);
      ASSERT_GE(ta->get_hold(), idle_size);
      ASSERT_NE(ta->chunk_cnt_, 0);
      ta.revert();
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));
      ASSERT_TRUE(NULL == malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id));
      // recycle will also search the unrecyled_list
      ASSERT_EQ(OB_ERROR, malloc_allocator->recycle_tenant_allocator(tenant_id));
      ta = malloc_allocator->get_tenant_ctx_allocator_unrecycled(tenant_id, g_ctx_id);
      ASSERT_LT(ta->get_hold(), idle_size);
      ASSERT_EQ(ta->chunk_cnt_, 0);
    } else {
      ASSERT_EQ(OB_SUCCESS, malloc_allocator->recycle_tenant_allocator(tenant_id));
      ASSERT_TRUE(NULL == malloc_allocator->get_tenant_ctx_allocator(tenant_id, g_ctx_id));
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, malloc_allocator->recycle_tenant_allocator(tenant_id));
    }
  }
}

TEST(TestMallocAllocator, allocator_guard)
{
  {
    ObTenantCtxAllocatorGuard guard;
    ASSERT_EQ(NULL, guard.ref_allocator());
    ASSERT_EQ(NULL, guard.operator->());
    ASSERT_EQ(true, guard.lock_);
  }
  {
    ObTenantCtxAllocator *tmp_ta = new ObTenantCtxAllocator(0,0);
    ASSERT_EQ(0, tmp_ta->get_ref_cnt());
    ObTenantCtxAllocatorGuard guard(tmp_ta);
    ASSERT_EQ(1, tmp_ta->get_ref_cnt());
    ASSERT_EQ(tmp_ta, guard.ref_allocator());
    ASSERT_EQ(tmp_ta, guard.operator->());
    ASSERT_EQ(true, guard.lock_);

    // move
    ObTenantCtxAllocatorGuard new_guard(std::move(guard));
    ASSERT_EQ(NULL, guard.ref_allocator());
    ASSERT_NE(NULL, (long)new_guard.ref_allocator());
    new_guard.~ObTenantCtxAllocatorGuard();
    ASSERT_EQ(0, tmp_ta->get_ref_cnt());
    ASSERT_EQ(NULL, new_guard.ref_allocator());
    ASSERT_EQ(NULL, new_guard.operator->());
  }
}

TEST(TestMallocAllocator, bug_47543065)
{
  std::pair<int, int64_t> flow[] = {
    std::make_pair(1, 1002),
    std::make_pair(1, 1001),
    std::make_pair(1, 1006),
    std::make_pair(1, 1005),
    std::make_pair(1, 1004),
    std::make_pair(1, 1003),
    std::make_pair(0, 1001),
    std::make_pair(1, 1008),
    std::make_pair(1, 1007),
    std::make_pair(0, 1005),
    std::make_pair(0, 1007),
    std::make_pair(0, 1008),
    std::make_pair(1, 1008),
    std::make_pair(1, 1007),
    std::make_pair(0, 1007),
    std::make_pair(0, 1008),
    std::make_pair(1, 1008)
  };
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  for (int i = 0; i < sizeof(flow)/sizeof(flow[0]);i++) {
    int create = flow[i].first;
    int64_t tenant_id = flow[i].second;
    if (create) {
      ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
      // make tenant memleak
      auto ta = malloc_allocator->get_tenant_ctx_allocator(tenant_id, 0);
      ASSERT_NE(nullptr, ta);
      ASSERT_NE(nullptr, ta->alloc(8, ObMemAttr(tenant_id)));
    } else {
      malloc_allocator->recycle_tenant_allocator(tenant_id);
    }
  }

  // check status
  for (int i = 0; i < ObMallocAllocator::PRESERVED_TENANT_COUNT; i++) {
    auto ta = malloc_allocator->allocators_[i];
    uint64_t prev_tenant_id = 0;
    while (ta != NULL) {
      ASSERT_EQ(ta->get_tenant_id() % ObMallocAllocator::PRESERVED_TENANT_COUNT, i);
      ASSERT_GT(ta->get_tenant_id(), prev_tenant_id);
      auto ta_other = malloc_allocator->unrecycled_allocators_;
      while (ta_other) {
        ASSERT_NE(ta, ta_other);
        ta_other = ta_other->get_next();
      }
      prev_tenant_id = ta->get_tenant_id();
      ta = ta->get_next();
    }
  }
}

int main(int argc, char *argv[])
{
  signal(49, SIG_IGN);
  OB_LOGGER.set_file_name("t.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

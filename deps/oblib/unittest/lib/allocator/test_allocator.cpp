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
#include "lib/allocator/ob_allocator_v2.h"
#undef private

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

const uint64_t tenant_id = 100;
const uint64_t ctx_id = 2;
const int64_t limit = 1 << 30;
const lib::ObLabel &label = "1";

const uint64_t new_tenant_id = 101;

static bool has_unfree = false;
void has_unfree_callback(char *)
{
  has_unfree = true;
}
class TestAllocator : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    ObMallocAllocator *ma = ObMallocAllocator::get_instance();
    ASSERT_EQ(OB_SUCCESS, ma->create_and_add_tenant_allocator(tenant_id));
    ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(tenant_id, limit));
    auto ta = ma->get_tenant_ctx_allocator(tenant_id, ctx_id);
    ASSERT_TRUE(NULL != ta);

    ASSERT_EQ(OB_SUCCESS, ma->create_and_add_tenant_allocator(new_tenant_id));
    ta = ma->get_tenant_ctx_allocator(new_tenant_id, ctx_id);
    ASSERT_TRUE(NULL != ta);
  }
  //virtual void TearDown();
};

// ObAllocator has no state and no logic, only basic functions are tested here
TEST_F(TestAllocator, basic)
{
  ObMallocAllocator *ma = ObMallocAllocator::get_instance();
  auto ta = ma->get_tenant_ctx_allocator(tenant_id, ctx_id);
  ObMemAttr attr(tenant_id, label, ctx_id);
  ObAllocator a(nullptr, attr);
  int64_t sz = 100;

  void *p[128] = {};
  int64_t cnt = 1L << 18;
  sz = 1L << 4;

  while (cnt--) {
    int i = 0;
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    p[i++] = a.alloc(sz);
    int64_t hold = a.used();
    ASSERT_GT(hold, 0);
    while (i--) {
      a.free(p[i]);
    }
    sz = ((sz | reinterpret_cast<size_t>(p[0])) & ((1<<13) - 1));
  }

  // test alloc_align/free_align
  for (int i = 0; i < 10; ++i) {
    int64_t align = 8<<i;
    void *ptr = a.alloc_align(100, align);
    ASSERT_EQ(0, (int64_t)ptr & (align - 1));
    ASSERT_GT(a.used(), 0);
    a.free_align(ptr);
    ASSERT_EQ(a.used(), 0);
  }
  cout << "done" << endl;
}

TEST_F(TestAllocator, reveal_unfree)
{
  ObMallocAllocator *ma = ObMallocAllocator::get_instance();
  auto ta = ma->get_tenant_ctx_allocator(tenant_id, ctx_id);
  ObMemAttr attr(tenant_id, label, ctx_id);
  has_unfree = false;
  // no unfree
  {
    ObAllocator a(nullptr, attr);
    const int64_t hold = a.used();
    void *ptr = a.alloc(100);
    ASSERT_NE(ptr, nullptr);
    ASSERT_GT(a.used(), hold);
    a.free(ptr);
    a.~ObAllocator();
    ASSERT_FALSE(has_unfree);
    ASSERT_EQ(a.used(), hold);
  }
  // has unfree
  {
    ObAllocator a(nullptr, attr);
    const int64_t hold = a.used();
    void *ptr = a.alloc(100);
    ASSERT_NE(ptr, nullptr);
    ASSERT_GT(a.used(), hold);
    //a.free(ptr);
    a.~ObAllocator();
    ASSERT_TRUE(has_unfree);
    ASSERT_EQ(a.used(), hold);
  }
}

TEST_F(TestAllocator, reset)
{
  ObMallocAllocator *ma = ObMallocAllocator::get_instance();
  auto ta = ma->get_tenant_ctx_allocator(tenant_id, ctx_id);
  ObMemAttr attr(tenant_id, label, ctx_id);
  const int64_t hold = 0;
  ObAllocator a(nullptr, attr);
  void *ptr = a.alloc(100);
  ASSERT_NE(ptr, nullptr);
  ASSERT_GT(a.used(), hold);
  // reset
  a.reset();
  ASSERT_EQ(a.used(), hold);
  // alloc after reset
  ptr = a.alloc(100);
  ASSERT_NE(ptr, nullptr);
  ASSERT_GT(a.used(), hold);
  a.~ObAllocator();
  ASSERT_EQ(a.used(), hold);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

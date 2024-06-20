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
#define private public
#include "lib/allocator/ob_page_manager.h"
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

#if 0
TEST_F(TestAllocator, pm_basic)
{
  ObPageManager pm;
  // use default
  void *page = pm.alloc_page(100);
  ASSERT_NE(nullptr, page);
  ASSERT_EQ(pm.get_hold(), pm.set_.get_total_hold());
  pm.free_page(page);
  ASSERT_EQ(OB_SUCCESS, pm.set_tenant_ctx(tenant_id, ctx_id));
  int64_t ps = 1024;
  void *ptr = nullptr;

  void *p[128] = {};
  ps = 8L << 10;

  int i = 0;
  // For cut tenants, the release of chunks is lazy and triggered by the next application (that is, the first application of a new tenant)
  // So here hold> 0
  int64_t hold = pm.get_hold();
  while (i < 30) {
    p[i] = pm.alloc_page(ps);
    ASSERT_NE(nullptr, p[i++]);
    ps = (8L << 10) * i;
  }
  ASSERT_GT(pm.get_hold(), hold);
  while (i--) {
    pm.free_page(p[i]);
  }
  ASSERT_EQ(pm.get_hold(), 0);

  // freelist
  int large_size = INTACT_ACHUNK_SIZE - 200;
  ptr = pm.alloc_page(large_size);
  hold = pm.get_hold();
  ASSERT_GT(hold, 0);
  pm.free_page(ptr);
  ASSERT_EQ(pm.get_hold(), hold);
  ptr = pm.alloc_page(large_size);
  ASSERT_EQ(pm.get_hold(), hold);
  pm.free_page(ptr);
  ASSERT_LT(pm.get_hold(), hold);
  hold = pm.get_hold();
  ptr = pm.alloc_page(large_size);
  ASSERT_GT(pm.get_hold(), hold);
  pm.free_page(ptr);
  ASSERT_EQ(pm.get_hold(), hold);

  pm.alloc_page(large_size);
  pm.alloc_page(large_size);
  pm.alloc_page(large_size);

  // switch tenant
  hold = pm.get_hold();
  ASSERT_EQ(OB_SUCCESS, pm.set_tenant_ctx(new_tenant_id, ctx_id));
  ptr = pm.alloc_page(100);
  ASSERT_LT(pm.get_hold(), hold);
  ASSERT_GT(pm.get_hold(), 0);

  cout << "done" << endl;
}

TEST_F(TestAllocator, pm_reveal_unfree)
{
  ObMallocAllocator *ma = ObMallocAllocator::get_instance();
  auto ta = ma->get_tenant_ctx_allocator(tenant_id, ctx_id);
  has_unfree = false;
  int64_t ps = 8L << 10;
  // no unfree
  {
    const int64_t hold = ta->get_hold();
    ObPageManager pm;
    ASSERT_EQ(OB_SUCCESS, pm.set_tenant_ctx(tenant_id, ctx_id));
    void *ptr = pm.alloc_page(ps);
    ASSERT_NE(ptr, nullptr);
    ASSERT_GT(ta->get_hold(), hold);
    pm.free_page(ptr);
    ASSERT_EQ(ta->get_hold(), hold);
    pm.~ObPageManager();
    ASSERT_FALSE(has_unfree);
  }
  // has unfree
  {
    const int64_t hold = ta->get_hold();
    ObPageManager pm;
    ASSERT_EQ(OB_SUCCESS, pm.set_tenant_ctx(tenant_id, ctx_id));
    void *ptr = pm.alloc_page(100);
    ASSERT_NE(ptr, nullptr);
    ASSERT_GT(ta->get_hold(), hold);
    pm.~ObPageManager();
    ASSERT_TRUE(has_unfree);
    ASSERT_EQ(ta->get_hold(), hold);
  }
}
#endif

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

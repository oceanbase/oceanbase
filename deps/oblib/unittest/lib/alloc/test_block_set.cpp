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

#include "lib/alloc/block_set.h"
#include "lib/allocator/ob_malloc.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

ObMemAttr attr;

static const uint32_t INTACT_BIG_ABLOCK_SIZE = ACHUNK_SIZE;
static const uint32_t BIG_ABLOCK_SIZE = INTACT_BIG_ABLOCK_SIZE - ABLOCK_HEADER_SIZE;
static const uint32_t INTACT_BIG_AOBJECT_SIZE = BIG_ABLOCK_SIZE;
static const uint32_t BIG_AOBJECT_SIZE = INTACT_BIG_AOBJECT_SIZE - AOBJECT_META_SIZE;

class TestBlockSet
    : public ::testing::Test
{
public:
  TestBlockSet()
      : tallocator_(500)
  {}
  virtual void SetUp()
  {
    tallocator_.set_tenant_memory_mgr();
    tallocator_.set_limit(1000L << 20);
    cs_.set_tenant_ctx_allocator(tallocator_);
  }

  virtual void TearDown()
  {
  }

  ABlock *Malloc(uint64_t size)
  {
    ABlock *block = cs_.alloc_block(size, attr);
    return block;
  }

  void Free(ABlock *block)
  {
    cs_.free_block(block);
  }

  void check_ptr(void *p)
  {
    UNUSED(p);
    ASSERT_TRUE(p != NULL);
    // ASSERT_EQ(56, (uint64_t)p & 0xFF) << ((uint64_t)p & 0xFF);
  }

protected:
  ObTenantCtxAllocator tallocator_;
  BlockSet cs_;
};

TEST_F(TestBlockSet, ManyMalloc)
{
  ABlock *p = NULL;
  int64_t cnt = 1L << 10;
  uint64_t sz = 32;

  while (cnt--) {
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    sz = ((sz | reinterpret_cast<size_t>(p)) & ((1<<18) - 1));
  }
}

TEST_F(TestBlockSet, AllocLarge)
{
  uint64_t sz = 1L << 18;
  int64_t cnt = 1L << 10;
  ABlock *p = NULL;

  while (cnt--) {
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
    sz = ((sz | reinterpret_cast<size_t>(p)) & ((1<<25) - 1));
  }
}

TEST_F(TestBlockSet, NormalBlock)
{
  const uint64_t sz = INTACT_BIG_AOBJECT_SIZE;
  int64_t cnt = 1L << 10;
  ABlock *p = NULL;

  while (cnt--) {
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
  }
}

TEST_F(TestBlockSet, BigBlock)
{
  const uint64_t sz = 1L << 20;
  int64_t cnt = 1L << 20;
  ABlock *p = NULL;

  while (cnt--) {
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
  }
}

TEST_F(TestBlockSet, BigBlockOrigin)
{
  const uint64_t sz = 1L << 20;
  int64_t cnt = 1L << 10;
  void *p = NULL;

  while (cnt--) {
    p = ob_malloc(sz, ObNewModIds::TEST);
    check_ptr(p);
    ob_free(p);
  }
}

TEST_F(TestBlockSet, Single)
{
  uint64_t sz = INTACT_NORMAL_AOBJECT_SIZE;
  ABlock *pa[1024] = {};
  int cnt = 10;
  while (cnt--) {
    int i = 0;
    for (i = 0; i < 255; ++i) {
      pa[i] = Malloc(sz);
      check_ptr(pa[i]);
    }
    cout << "free" << cnt << endl;
    while (i--) {
      Free(pa[i]);
    }
    cout << cnt << endl;
  }
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

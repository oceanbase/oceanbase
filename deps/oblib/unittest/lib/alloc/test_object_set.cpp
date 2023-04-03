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

#include "lib/alloc/object_set.h"
#include "lib/alloc/block_set.h"
#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/ob_errno.h"

using namespace oceanbase::lib;
using namespace std;
using namespace oceanbase::common;
ObMemAttr attr;

#if 0
class TestObjectSet
    : public ::testing::Test
{
  class ObjecSetLocker: public ISetLocker
  {
  public:
    ObjecSetLocker() {}
    void lock() override
    {
    }
    void unlock() override
    {
    }
    bool trylock() override
    {
      return true;
    }
  };
public:
  TestObjectSet()
    : tallocator_(500),
      os_locker_(), bs_(), os_()
  {}

  virtual void SetUp()
  {
    cout << BLOCKS_PER_CHUNK << endl;
    cout << AOBJECT_META_SIZE << " " << ABLOCK_HEADER_SIZE << endl;
    tallocator_.set_tenant_memory_mgr();
    tallocator_.set_limit(1000L << 20);
    bs_.reset();
    bs_.set_tenant_ctx_allocator(tallocator_);
    os_.set_block_mgr(&bs_);
    os_.set_locker(&os_locker_);
    os_.reset();
  }

  virtual void TearDown()
  {
  }

  void *Malloc(uint64_t size)
  {
    void *p = NULL;
    AObject *obj = os_.alloc_object(size, attr);
    if (obj != NULL) {
      p = obj->data_;
      // memset(p, 0, size);
    }
    return p;
  }

  void Free(void *ptr)
  {
    AObject *obj = reinterpret_cast<AObject*>(
        (char*)ptr - AOBJECT_HEADER_SIZE);
    os_.free_object(obj);
  }

  void *Realloc(void *ptr, uint64_t size)
  {
    AObject *obj = NULL;
    if (ptr != NULL) {
      obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    }
    obj = os_.realloc_object(obj, size, attr);
    return obj->data_;
  }

  void Reset()
  {
    os_.reset();
  }

  void check_ptr(void *ptr)
  {
    EXPECT_TRUE(ptr != NULL);
    UNUSED(ptr);
  }

protected:
  ObTenantCtxAllocator tallocator_;
  ObjecSetLocker os_locker_;
  BlockSet bs_;
  ObjectSet os_;
};

TEST_F(TestObjectSet, Basic)
{
  void *p = NULL;
  int64_t cnt = 1L << 20;
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
    sz = ((sz | reinterpret_cast<size_t>(p)) & ((1<<13) - 1));
  }
}

TEST_F(TestObjectSet, Basic2)
{
  void *p[128] = {};
  int64_t cnt = 1L << (28 - 10);
  uint64_t sz = 1024;

  while (cnt--) {
    int i = 0;
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    p[i++] = Malloc(sz);
    while (i--) {
      Free(p[i]);
    }
    sz = ((sz | reinterpret_cast<size_t>(p[0])) & ((1<<13) - 1));
  }
}

TEST_F(TestObjectSet, SplitBug)
{
  void *p[128] = {};

  {
    // build free list
    for (int i = 0; i < 8; i++) {
      p[i] = Malloc(8192);
    }
    for (int i = 0; i < 8; i++) {
      Free(p[i]);
    }
  }

  for (int i = 0; i < 8; i++) {
    p[i] = Malloc(1008);
  }
  for (int i = 0; i < 7; i++) {
    Free(p[i]);
  }
  for (int i = 0; i < 6; i++) {
    p[i] = Malloc(1008);
    memset(p[i], 0, 1008);
  }
  Free(p[0]);
  Free(p[7]);

  // The last remainder will indicate a invalid address if splitting
  // obj doesn't work well.
  Malloc(1);
}

TEST_F(TestObjectSet, Reset)
{
  void *p = NULL;
  int64_t cnt = 1L << (28 - 10);
  uint64_t sz = 32;

  while (cnt--) {
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    p = Malloc(sz);
    check_ptr(p);
    Reset();
    sz = ((sz | reinterpret_cast<size_t>(p)) & ((1<<13) - 1));
  }
}

TEST_F(TestObjectSet, NormalObject)
{
  void *p = NULL;
  int64_t cnt = 1L << (28 - 6);
  uint64_t sz = 1 < 13;

  while (cnt--) {
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
  }
}

TEST_F(TestObjectSet, NormalObject_plus_1)
{
  void *p = NULL;
  int64_t cnt = 1L << (28 - 6);
  uint64_t sz = (1 << 13) + 1;

  p = Malloc(sz);
  while (cnt--) {
    p = Malloc(sz);
    check_ptr(p);
    Free(p);
  }
}

TEST_F(TestObjectSet, MallocReset)
{
  int64_t cnt = 1L << 22;
  const int sz = 1 << 13;

  while (cnt--) {
    Malloc(sz);
    Reset();
  }
}

TEST_F(TestObjectSet, PageArena)
{
  int64_t cnt = 1L << 22;
  const int sz = 32;

  while (cnt--) {
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Malloc(sz);
    Reset();
  }
}

TEST_F(TestObjectSet, PageArenaOrigin)
{
  //int64_t cnt = 1L << 22;
  int64_t cnt = 1L << 20L;
  const int sz = 32;

  oceanbase::common::PageArena<> pa;

  while (cnt--) {
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.alloc(sz);
    pa.free();
  }
}

TEST_F(TestObjectSet, ReallocInc1M)
{
  void *p = NULL;
  int64_t cnt = 1L << 10;
  uint64_t sz = 1;

  p = Realloc(p, 0);
  EXPECT_TRUE(p != NULL);

  while (cnt-- && sz < (1 << 20)) {
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    sz += (reinterpret_cast<int64_t>(p) & ((1 << 6) - 1));
  }
  Free(p);
}

TEST_F(TestObjectSet, ReallocDec1M)
{
  void *p = NULL;
  int64_t cnt = 1L << 10;
  int64_t sz = 1 << 20;

  while (cnt-- && sz > 0) {
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    p = Realloc(p, sz);
    memset(p, 0, sz);
    sz -= (reinterpret_cast<int64_t>(p) & ((1 << 6) - 1));
  }
  Free(p);
}

TEST_F(TestObjectSet, BlockCacheSize)
{
  static const int64_t SZ = 8192;
  static const int64_t COUNT = 10000;
  void *objs[COUNT] = {};
  for (int i = 0; i < COUNT; ++i) {
    objs[i] = Malloc(SZ);
  }
  for (int i = 0; i < COUNT/10; ++i) {
    Free(objs[i]);
  }
  EXPECT_EQ(os_.get_normal_hold() - os_.get_normal_used(), SZ + AOBJECT_META_SIZE);
}
#endif

int main(int argc, char *argv[])
{
  signal(49, SIG_IGN);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

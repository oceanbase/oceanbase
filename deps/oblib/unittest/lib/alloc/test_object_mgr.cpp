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

#define private public
#include "lib/resource/achunk_mgr.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/alloc/object_mgr.h"
#undef private
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_test_util.h"
#include "lib/coro/testing.h"
#include <gtest/gtest.h>

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TestObjectMgr
    : public ::testing::Test
{
public:
  void *Malloc(uint64_t size)
  {
    // void *p = NULL;
    // AObject *obj = os_.alloc_object(size);
    // if (obj != NULL) {
    //   p = obj->data_;
    // }
    // return p;
    return oceanbase::common::ob_malloc(size, ObNewModIds::TEST);
  }

  void Free(void *ptr)
  {
    // AObject *obj = reinterpret_cast<AObject*>(
    //     (char*)ptr - AOBJECT_HEADER_SIZE);
    // os_.free_object(obj);
    return oceanbase::common::ob_free(ptr);
  }

// protected:
//   ObjectMgr<1> om_;
};

TEST_F(TestObjectMgr, Basic2)
{
  cotesting::FlexPool([] {
    void *p[128] = {};
    int64_t cnt = 1L << 18;
    uint64_t sz = 1L << 4;

    while (cnt--) {
      int i = 0;
      for (int j = 0; j < 16; ++j) {
        p[i++] = ob_malloc(sz, ObNewModIds::TEST);
      }
      while (i--) {
        ob_free(p[i]);
      }
      // sz = ((sz | reinterpret_cast<size_t>(p[0])) & ((1<<13) - 1));
    }

    cout << "done" << endl;
  }, 4).start();
}

TEST_F(TestObjectMgr, TestName)
{
  void *p[128];

  Malloc(327800);
  Malloc(65536);
  Malloc(49408);
  Malloc(12376);
  Malloc(65344);

  p[1] = Malloc(2097152);
  Free(p[1]);

  Malloc(49248);
  Malloc(3424);
  Malloc(8);
  Malloc(3072);
  Malloc(176);
  p[11] = Malloc(96);
  p[10] = Malloc(65536);
  p[7] = Malloc(65568);
  p[6] = Malloc(96);
  p[5] = Malloc(65536);
  Malloc(2096160);
  Malloc(16);
  Malloc(65536);
  p[9] = Malloc(96);
  p[8] = Malloc(65536);
  p[4] = Malloc(65568);
  p[3] = Malloc(96);
  p[2] = Malloc(65536);
  Malloc(16);
  Free(p[2]);
  Free(p[3]);
  Free(p[4]);
  Free(p[5]);
  Free(p[6]);
  Free(p[7]);
  Free(p[8]);
  Free(p[9]);
  Free(p[10]);
  Free(p[11]);
  Malloc(12384);
  Malloc(12384);
  Malloc(12384);
  Malloc(96);
  Malloc(65536);
  p[13] = Malloc(96);
  p[12] = Malloc(65536);
  Free(p[12]);
  Free(p[13]);
  p[15] = Malloc(96);
  p[14] = Malloc(65536);
  Free(p[14]);
  Free(p[15]);
  p[17] = Malloc(96);
  p[16] = Malloc(65536);
  Malloc(65536);
  Free(p[16]);
  Free(p[17]);
  p[19] = Malloc(96);
  p[18] = Malloc(65536);
  Free(p[18]);
  Free(p[19]);
  Malloc(96);
}

struct Record
{
  int32_t size_;
  int64_t addr_;
};

AChunk *chunk(void *ptr)
{
  auto *obj = (AObject*)((char*)ptr - AOBJECT_HEADER_SIZE);
  auto *chunk = obj->block()->chunk();
  return chunk;
}

TEST_F(TestObjectMgr, TestFragmentWash)
{
  ObTenantResourceMgrHandle resource_handle;
  ObResourceMgr::get_instance().get_tenant_resource_mgr(
      OB_SERVER_TENANT_ID, resource_handle);
  auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
		  OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID);
  ta->sync_wash(INT64_MAX);
  int washed_size = ta->sync_wash(INT64_MAX);
  ASSERT_EQ(washed_size, 0);

  vector<pair<AChunk*, vector<void*> > > chunk_ptrs;
  int chunk_cnt = 1000;
  int alloc_size = ABLOCK_SIZE - 256;
  do {
    void *ptr = ob_malloc(alloc_size, "mod");
    abort_unless(ptr != nullptr);
    AChunk *cur_chunk = chunk(ptr);
    vector<void*> ptrs{ptr};
    do {
      void *ptr = ob_malloc(alloc_size, "mod");
      abort_unless(ptr != nullptr);
      if (chunk(ptr) != cur_chunk) {
        ob_free(ptr);
        break;
      }
      ptrs.push_back(ptr);
    } while (true);
    chunk_ptrs.push_back(make_pair(cur_chunk, ptrs));
  } while (chunk_cnt--);

  washed_size = ta->sync_wash(INT64_MAX);
  ASSERT_EQ(washed_size, 0);

  int64_t chunk_mgr_hold_before = AChunkMgr::instance().get_hold();
  int64_t resource_mgr_hold_before = resource_handle.get_memory_mgr()->get_sum_hold();
  int free_size = 0;
  for (int i =0; i< chunk_ptrs.size(); i++) {
    // keep first ptr
    for (int j =1; j< chunk_ptrs[i].second.size(); j++) {
      ob_free(chunk_ptrs[i].second[j]);
      free_size += alloc_size;
    }
  }
  int64_t rss_size_orig = 0;
  int64_t rss_size = 0;
  get_virtual_memory_used(&rss_size_orig);
  washed_size = ta->sync_wash(INT64_MAX);
  get_virtual_memory_used(&rss_size);
  ASSERT_GT(washed_size, free_size);
  ASSERT_TRUE(rss_size < rss_size_orig &&
              std::abs(std::abs(rss_size - rss_size_orig) - washed_size) * 1.0 / washed_size < 0.1);

  int64_t chunk_mgr_hold = AChunkMgr::instance().get_hold();
  int64_t resource_mgr_hold = resource_handle.get_memory_mgr()->get_sum_hold();
  ASSERT_EQ(resource_mgr_hold_before - washed_size, resource_mgr_hold);
  ASSERT_EQ(chunk_mgr_hold_before - washed_size, chunk_mgr_hold);

  for (int i =0; i< chunk_ptrs.size(); i++) {
    ob_free(chunk_ptrs[i].second[0]);
  }
}


TEST_F(TestObjectMgr, TestSubObjectMgr)
{
  AChunkMgr::instance().set_max_chunk_cache_size(0);
  oceanbase::lib::set_memory_limit(20LL<<30);
  int fd = open("alloc_flow_records", O_RDONLY, S_IRWXU | S_IRGRP);
  abort_unless(fd > 0);
  struct stat fileInfo;
  bzero(&fileInfo, sizeof(fileInfo));
  int rc = fstat(fd, &fileInfo);
  abort_unless(rc != -1);
  int64_t total_size = fileInfo.st_size;
  void *ptr = ::mmap(0, total_size, PROT_READ, MAP_SHARED, fd, 0);
  abort_unless(ptr != MAP_FAILED);
  int64_t tenant_id = OB_SERVER_TENANT_ID;
  int64_t ctx_id = ObCtxIds::DEFAULT_CTX_ID;
  auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
    tenant_id, ctx_id);
  ObjectMgr som(*ta.ref_allocator(), false, INTACT_NORMAL_AOBJECT_SIZE, 1, false, NULL);
  ObMemAttr attr;
  ObTenantResourceMgrHandle resource_handle;
  ObResourceMgr::get_instance().get_tenant_resource_mgr(
		  tenant_id, resource_handle);
  map<int64_t, AObject*> allocs;
  int i = total_size/sizeof(Record);
  auto *rec = (Record*)ptr;
  while (i--) {
    int32_t size = rec->size_;
    int64_t addr = rec->addr_;
    if (size != 0) {
      auto *object = som.alloc_object(size, attr);
      abort_unless(object != nullptr);
      allocs.insert(pair<int64_t, AObject*>(addr, object));
      memset(object->data_, 0xAA, size);
    } else {
      auto it = allocs.find(addr);
      abort_unless(it != allocs.end());
      AObject *obj = it->second;
      ABlock *block = obj->block();
      abort_unless(block->is_valid());
      ObjectSet *set = block->obj_set_;
      set->free_object(obj);
      allocs.erase(it->first);
    }
    rec++;
  }
}

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
#define protected public
#include "lib/alloc/object_mgr.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/ob_malloc_sample_struct.h"
#include "lib/utility/ob_backtrace.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

class TestObjectSet : public ::testing::Test, public ObjectSet
{
public:
  TestObjectSet()
  {
    ObTenantCtxAllocatorGuard ta = ObMallocAllocator::get_instance()->
        get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, ObCtxIds::GLIBC);
    ObjectSet::set_block_mgr(&ta->get_block_mgr());
  }
};

TEST_F(TestObjectSet, basic)
{
  uint64_t size = 2048;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "test", ObCtxIds::GLIBC);
  // check the status of block is FULL->PARTITIAL->FULL->PARTITIAL->EMPTY
  // check the order of alloc_object is local->avail->new_block
  const int max_cnt = 16;
  AObject *objs[max_cnt + 1];
  memset(objs, 0, sizeof(objs));
  objs[0] = alloc_object(size, attr);
  ABlock *block = objs[0]->block();
  const int sc_idx = block->sc_idx_;
  ABlock *&avail_blist = scs[sc_idx].avail_blist_;
  ASSERT_EQ(max_cnt, block->max_cnt_);
  ASSERT_TRUE(ABlock::FULL == block->status_);
  ASSERT_EQ(NULL, avail_blist);
  free_object(objs[0], block);
  ASSERT_TRUE(ABlock::PARTITIAL == block->status_);
  ASSERT_EQ(block, avail_blist);
  for (int i = 0; i < max_cnt; ++i) {
    AObject *local = scs[sc_idx].local_free_;
    ABlock *avail = avail_blist;
    objs[i] = alloc_object(size, attr);
    if (NULL != local) {
      ASSERT_EQ(local, objs[i]);
    } else if (NULL != avail) {
      ASSERT_EQ(NULL, avail_blist);
      ASSERT_EQ(avail, objs[i]->block());
    }
    ASSERT_EQ(block, objs[i]->block());
  }
  objs[max_cnt] = alloc_object(size, attr);
  ASSERT_NE(block, objs[max_cnt]->block());
  free_object( objs[max_cnt], objs[max_cnt]->block());
  ASSERT_TRUE(ABlock::FULL == block->status_);
  free_object(objs[0], block);
  for (int i = 1; i < max_cnt; ++i) {
    ASSERT_TRUE(ABlock::PARTITIAL == block->status_);
    free_object(objs[i], block);
  }
  ASSERT_TRUE(ABlock::EMPTY == block->status_);
}

TEST(TestMallocAllocator, recycle_tenant_allocator)
{
  int64_t tenant_id = 1001;
  int64_t ctx_id = ObCtxIds::DEFAULT_CTX_ID;
  const char label[] = "Test";
  ObMemAttr attr(tenant_id, label, ctx_id);
  auto ma = ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_SUCCESS, ma->create_and_add_tenant_allocator(tenant_id));
  void *ptr = ma->alloc(1024, attr);
  AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
  char bt[MAX_BACKTRACE_LENGTH] = {'\0'};
  parray(bt, MAX_BACKTRACE_LENGTH, (int64_t*)(obj->bt()), AOBJECT_BACKTRACE_COUNT);
  auto ta = ma->get_tenant_ctx_allocator(tenant_id, ctx_id);
  char first_label[AOBJECT_LABEL_SIZE + 1] = {'\0'};
  char first_bt[MAX_BACKTRACE_LENGTH] = {'\0'};
  ta->check_has_unfree(first_label, first_bt);
  ASSERT_TRUE(0 == STRNCMP(first_label, label, STRLEN(label)));
  ASSERT_TRUE(0 == STRNCMP(first_bt, bt, STRLEN(bt)));
}

int main(int argc, char *argv[])
{
  signal(49, SIG_IGN);
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_object_set.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  enable_memleak_light_backtrace(true);
  return RUN_ALL_TESTS();
}
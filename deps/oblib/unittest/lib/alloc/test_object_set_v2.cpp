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

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

ObjectMgrV2 object_mgr(OB_SERVER_TENANT_ID, ObCtxIds::GLIBC);
class TestObjectSet : public ::testing::Test, public ObjectSetV2
{
public:
  TestObjectSet()
  {
    ObjectSetV2::set_block_mgr(&object_mgr);
  }
};

TEST_F(TestObjectSet, basic)
{
  uint64_t size = 2048;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "test", ObCtxIds::GLIBC);
  // check the status of block is FULL->PARTITIAL->FULL->PARTITIAL->EMPTY
  // check the order of alloc_object is local->avail->new_block
  const int max_cnt = 4;
  AObject *objs[max_cnt + 1];
  memset(objs, 0, sizeof(objs));
  objs[0] = alloc_object(size, attr);
  ABlock *block = objs[0]->block();
  const int sc_idx = block->sc_idx_;
  ASSERT_EQ(max_cnt, block->max_cnt_);
  ASSERT_TRUE(ABlock::FULL == block->status_);
  ASSERT_EQ(NULL, scs[sc_idx].avail_);
  free_object(objs[0], block);
  ASSERT_TRUE(ABlock::PARTITIAL == block->status_);
  ASSERT_EQ(block, scs[sc_idx].avail_);
  for (int i = 0; i < max_cnt; ++i) {
    AObject *local = scs[sc_idx].local_free_;
    ABlock *avail = scs[sc_idx].avail_;
    objs[i] = alloc_object(size, attr);
    if (NULL != local) {
      ASSERT_EQ(local, objs[i]);
    } else if (NULL != avail) {
      ASSERT_EQ(NULL, scs[sc_idx].avail_);
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

int main(int argc, char *argv[])
{
  signal(49, SIG_IGN);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "close_modules/shared_storage/storage/incremental/garbage_collector/ob_ss_garbage_collector.h"
#include "storage/ls/ob_ls.h"
namespace oceanbase
{
using namespace common;

namespace unittest
{

MacroBlockId global_block_id(0, blocksstable::MacroBlockId::AUTONOMIC_BLOCK_INDEX, 0);

class TestBlockDetectAndGC : public ::testing::Test,
                             public storage::BlockDetectAndGC
{
public:
  TestBlockDetectAndGC()
  : storage::BlockDetectAndGC(global_block_id, 4000)
  {
  }
  TestBlockDetectAndGC(
      const MacroBlockId &start_block_id,
      const int64_t max_size)
  : storage::BlockDetectAndGC(start_block_id, max_size)
  {
  }
  virtual ~TestBlockDetectAndGC() {}

  TO_STRING_KV(K(macro_block_ids_[0]),
      K(macro_block_ids_[500]),
      K(macro_block_ids_[1000]),
      K(macro_block_ids_[1500]),
      K(macro_block_ids_[2000]),
      K(macro_block_ids_[2500]),
      K(macro_block_ids_[3000]),
      K(macro_block_ids_[3500]),
      K(macro_block_ids_[4000]),
      K(macro_block_ids_[4500]));

  virtual int is_exist_block_(
      const blocksstable::MacroBlockId &block_id,
      bool &is_exist)
  {
    is_exist = false;
    if (1 == macro_block_ids_[block_id.third_id_]) {
      is_exist = true;
    }
    return OB_SUCCESS;
  }

  virtual int delete_blocks_(
      MacroBlockId block_id,
      const int64_t size)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < size; i++) {
      macro_block_ids_[block_id.third_id_ + i] = 2;
    }
    return ret;
  }
  int64_t macro_block_ids_[5000];
};

TEST_F(TestBlockDetectAndGC, test_detect_and_gc1)
{
  LOG_INFO("test detect_and_gc1 start", KPC(this));

  for (int i = 0; i < 1000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 0; i < 2000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 2000; i < 3000; i++) {
    if (0 == i % 1000) {
      macro_block_ids_[i] = 0;
    } else {
      macro_block_ids_[i] = 1;
    }
  }
  for (int i = 3000; i < 4000; i++) {
    macro_block_ids_[i] = 0;
  }
  for (int i = 4000; i < 5000; i++) {
    macro_block_ids_[i] = 0;
  }

  ASSERT_EQ(OB_SUCCESS, detect_and_gc_block_());
  LOG_INFO("test detect_and_gc1 finish", KPC(this));
  for (int i = 0; i < 3000; i++) {
    ASSERT_EQ(2, macro_block_ids_[i]);
  }
  for (int i = 3000; i < 5000; i++) {
    ASSERT_EQ(0, macro_block_ids_[i]);
  }
}

TEST_F(TestBlockDetectAndGC, test_detect_and_gc2)
{
  LOG_INFO("test detect_and_gc2 start", KPC(this));

  for (int i = 0; i < 1000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 0; i < 2000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 2000; i < 3000; i++) {
    if (0 == i % 1000) {
      macro_block_ids_[i] = 1;
    } else {
      macro_block_ids_[i] = 0;
    }
  }
  for (int i = 3000; i < 4000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 4000; i < 5000; i++) {
    macro_block_ids_[i] = 0;
  }

  ASSERT_EQ(OB_SUCCESS, detect_and_gc_block_());
  LOG_INFO("test detect_and_gc2 finish", KPC(this));
  for (int i = 0; i < 4000; i++) {
    ASSERT_EQ(2, macro_block_ids_[i]);
  }
  for (int i = 4000; i < 5000; i++) {
    ASSERT_EQ(0, macro_block_ids_[i]);
  }
}

class TestBlockDetectAndGC2 : public TestBlockDetectAndGC
{
public:
  TestBlockDetectAndGC2()
  : TestBlockDetectAndGC(global_block_id, 4500)
  {
  }
  virtual ~TestBlockDetectAndGC2() {}

};

TEST_F(TestBlockDetectAndGC2, test_detect_and_gc3)
{
  LOG_INFO("test detect_and_gc3 start", KPC(this));

  for (int i = 0; i < 1000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 0; i < 2000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 2000; i < 3000; i++) {
    if (0 == i % 1000) {
      macro_block_ids_[i] = 1;
    } else {
      macro_block_ids_[i] = 0;
    }
  }
  for (int i = 3000; i < 4000; i++) {
    macro_block_ids_[i] = 1;
  }
  for (int i = 4000; i < 5000; i++) {
    macro_block_ids_[i] = 0;
  }

  ASSERT_EQ(OB_SUCCESS, detect_and_gc_block_());
  LOG_INFO("test detect_and_gc3 finish", KPC(this));
  for (int i = 0; i < 4500; i++) {
    ASSERT_EQ(2, macro_block_ids_[i]);
  }
  for (int i = 4500; i < 5000; i++) {
    ASSERT_EQ(0, macro_block_ids_[i]);
  }
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_detect_and_gc_block.log*");
  OB_LOGGER.set_file_name("test_detect_and_gc_block.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

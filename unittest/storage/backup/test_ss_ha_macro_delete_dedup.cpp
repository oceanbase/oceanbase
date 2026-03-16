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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public

#include "close_modules/shared_storage/storage/high_availability/ob_ss_ha_macro_delete_dag_net.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/hash/ob_hashset.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace backup
{

class TestSSHAMacroDeleteDedup : public ::testing::Test
{
public:
  TestSSHAMacroDeleteDedup() = default;
  virtual ~TestSSHAMacroDeleteDedup() = default;

  virtual void SetUp() override {}
  virtual void TearDown() override {}

  static MacroBlockId make_share_macro_id(int64_t first_id, int64_t second_id, int64_t third_id, int64_t fourth_id)
  {
    MacroBlockId id;
    id.set_first_id(first_id);
    id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    id.set_version_v2();
    id.set_second_id(second_id);
    id.set_third_id(third_id);
    id.set_fourth_id(fourth_id);
    return id;
  }
};

// Isolated test: verify ObHashSet dedup behavior for MacroBlockId (flag=0 means no overwrite, returns OB_HASH_EXIST on duplicate)
TEST_F(TestSSHAMacroDeleteDedup, hash_set_direct_dedup)
{
  common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> macro_id_set;
  ASSERT_EQ(OB_SUCCESS, macro_id_set.create(16));
  MacroBlockId id = make_share_macro_id(5, 5, 5, 5);
  EXPECT_EQ(OB_SUCCESS, macro_id_set.set_refactored(id, 0));
  EXPECT_EQ(OB_HASH_EXIST, macro_id_set.set_refactored(id, 0));
  EXPECT_EQ(OB_HASH_EXIST, macro_id_set.set_refactored(id, 0));
  EXPECT_EQ(1, macro_id_set.size());
}

// Empty array should return OB_ERR_UNEXPECTED
TEST_F(TestSSHAMacroDeleteDedup, empty_array)
{
  ObSSHAMacroDeleteTask task;
  ObArray<MacroBlockId> macro_block_ids;
  int ret = task.deduplicate_before_batch_delete_(macro_block_ids);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0, macro_block_ids.count());
}

// Array remains unchanged when there are no duplicates
TEST_F(TestSSHAMacroDeleteDedup, no_duplicates)
{
  ObSSHAMacroDeleteTask task;
  ObArray<MacroBlockId> macro_block_ids;
  MacroBlockId id1 = make_share_macro_id(1, 1, 1, 1);
  MacroBlockId id2 = make_share_macro_id(2, 2, 2, 2);
  MacroBlockId id3 = make_share_macro_id(3, 3, 3, 3);
  macro_block_ids.push_back(id1);
  macro_block_ids.push_back(id2);
  macro_block_ids.push_back(id3);

  int ret = task.deduplicate_before_batch_delete_(macro_block_ids);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(3, macro_block_ids.count());
  EXPECT_EQ(id1, macro_block_ids.at(0));
  EXPECT_EQ(id2, macro_block_ids.at(1));
  EXPECT_EQ(id3, macro_block_ids.at(2));
}

// Dedup correctly when duplicates exist, preserving first-occurrence order
TEST_F(TestSSHAMacroDeleteDedup, with_duplicates)
{
  ObSSHAMacroDeleteTask task;
  ObArray<MacroBlockId> macro_block_ids;
  MacroBlockId id1 = make_share_macro_id(1, 1, 1, 1);
  MacroBlockId id2 = make_share_macro_id(2, 2, 2, 2);
  MacroBlockId id3 = make_share_macro_id(3, 3, 3, 3);
  macro_block_ids.push_back(id1);
  macro_block_ids.push_back(id2);
  macro_block_ids.push_back(id1);  // duplicate
  macro_block_ids.push_back(id3);
  macro_block_ids.push_back(id2);   // duplicate

  int ret = task.deduplicate_before_batch_delete_(macro_block_ids);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(3, macro_block_ids.count());
  EXPECT_EQ(id1, macro_block_ids.at(0));
  EXPECT_EQ(id2, macro_block_ids.at(1));
  EXPECT_EQ(id3, macro_block_ids.at(2));
}

// All elements are duplicates
TEST_F(TestSSHAMacroDeleteDedup, all_duplicates)
{
  ObSSHAMacroDeleteTask task;
  ObArray<MacroBlockId> macro_block_ids;
  MacroBlockId id = make_share_macro_id(5, 5, 5, 5);
  macro_block_ids.push_back(id);
  macro_block_ids.push_back(id);
  macro_block_ids.push_back(id);

  int ret = task.deduplicate_before_batch_delete_(macro_block_ids);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, macro_block_ids.count());
  EXPECT_EQ(id, macro_block_ids.at(0));
}

// Single element
TEST_F(TestSSHAMacroDeleteDedup, single_element)
{
  ObSSHAMacroDeleteTask task;
  ObArray<MacroBlockId> macro_block_ids;
  MacroBlockId id = make_share_macro_id(10, 20, 30, 40);
  macro_block_ids.push_back(id);

  int ret = task.deduplicate_before_batch_delete_(macro_block_ids);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, macro_block_ids.count());
  EXPECT_EQ(id, macro_block_ids.at(0));
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_ha_macro_delete_dedup.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ss_ha_macro_delete_dedup.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

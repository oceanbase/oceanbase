/**
 * Copyright (c) 2024 OceanBase
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
#include <gmock/gmock.h>
#define private public
#define protected public
#include "lib/ob_errno.h"
#include "lib/hash/ob_hashset.h"
#include "storage/high_availability/ob_storage_ha_struct.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace hash;

namespace unittest
{

class ObMacroBlockReuseMgrTest : public ::testing::Test
{
public:
  ObMacroBlockReuseMgrTest();
  virtual ~ObMacroBlockReuseMgrTest();
  virtual void SetUp();
  virtual void TearDown();
public:
  ObMacroBlockReuseMgr reuse_mgr_;
};

struct MacroBlockInfo final
{
  ObLogicMacroBlockId logic_id;
  MacroBlockId macro_id;
  int64_t data_checksum;
};

ObMacroBlockReuseMgrTest::ObMacroBlockReuseMgrTest()
{
}

ObMacroBlockReuseMgrTest::~ObMacroBlockReuseMgrTest()
{
}

void ObMacroBlockReuseMgrTest::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, reuse_mgr_.init());
}

void ObMacroBlockReuseMgrTest::TearDown()
{
  reuse_mgr_.reset();
}

TEST_F(ObMacroBlockReuseMgrTest, test_init)
{
  // init twice
  ASSERT_EQ(OB_INIT_TWICE, reuse_mgr_.init());

  // init after reset
  reuse_mgr_.reset();
  ASSERT_EQ(OB_INIT_TWICE, reuse_mgr_.init());
}

TEST_F(ObMacroBlockReuseMgrTest, test_add_and_get)
{
  const int64_t TEST_MACRO_BLOCK_COUNT = 10;
  MacroBlockInfo macro_block_info[TEST_MACRO_BLOCK_COUNT] = {};
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    int64_t val = i + 1;
    macro_block_info[i].logic_id = ObLogicMacroBlockId(val, val, val);
    macro_block_info[i].macro_id = MacroBlockId(0, val, 0);
    macro_block_info[i].data_checksum = val;
  }

  // add macro block reuse info
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, reuse_mgr_.add_macro_block_reuse_info(
      macro_block_info[i].logic_id, macro_block_info[i].macro_id, macro_block_info[i].data_checksum));
  }

  // get macro block reuse info
  MacroBlockId macro_id;
  int64_t data_checksum = -1;
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    macro_id.reset();
    data_checksum = -1;
    ASSERT_EQ(OB_SUCCESS, reuse_mgr_.get_macro_block_reuse_info(
      macro_block_info[i].logic_id, macro_id, data_checksum));
    ASSERT_EQ(macro_block_info[i].macro_id, macro_id);
    ASSERT_EQ(macro_block_info[i].data_checksum, data_checksum);
  }

  // get macro block reuse info with non-exist logic id
  ObLogicMacroBlockId non_exist_logic_id(100, 100, 100);
  macro_id.reset();
  data_checksum = -1;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, reuse_mgr_.get_macro_block_reuse_info(
    non_exist_logic_id, macro_id, data_checksum));
}

TEST_F(ObMacroBlockReuseMgrTest, test_duplicated_input)
{
  const int64_t TEST_MACRO_BLOCK_COUNT = 10;
  MacroBlockInfo macro_block_info[TEST_MACRO_BLOCK_COUNT] = {};
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    int64_t val = i + 1;
    macro_block_info[i].logic_id = ObLogicMacroBlockId(val, val, val);
    macro_block_info[i].macro_id = MacroBlockId(0, val, 0);
    macro_block_info[i].data_checksum = val;
  }

  MacroBlockInfo macro_block_info_2[TEST_MACRO_BLOCK_COUNT] = {};
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    int64_t val = i + 1;
    macro_block_info_2[i].logic_id = ObLogicMacroBlockId(val, val, val);
    macro_block_info_2[i].macro_id = MacroBlockId(0, val + 1000, 0);
    macro_block_info_2[i].data_checksum = val + 1000;
  }

  // add macro block reuse info
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, reuse_mgr_.add_macro_block_reuse_info(
      macro_block_info[i].logic_id, macro_block_info[i].macro_id, macro_block_info[i].data_checksum));
  }
  int cur_size = reuse_mgr_.get_size();

  // get macro block reuse info
  MacroBlockId macro_id;
  int64_t data_checksum = -1;
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    macro_id.reset();
    data_checksum = -1;
    ASSERT_EQ(OB_SUCCESS, reuse_mgr_.get_macro_block_reuse_info(
      macro_block_info[i].logic_id, macro_id, data_checksum));
    ASSERT_EQ(macro_block_info[i].macro_id, macro_id);
    ASSERT_EQ(macro_block_info[i].data_checksum, data_checksum);
  }

  // try add duplicated macro block reuse info (will skip and return OB_SUCCESS)
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, reuse_mgr_.add_macro_block_reuse_info(
      macro_block_info_2[i].logic_id, macro_block_info_2[i].macro_id, macro_block_info_2[i].data_checksum));
  }
  // size won't change
  ASSERT_EQ(cur_size, reuse_mgr_.get_size());

  // get macro block reuse info, will get the old one
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    macro_id.reset();
    data_checksum = -1;
    ASSERT_EQ(OB_SUCCESS, reuse_mgr_.get_macro_block_reuse_info(
      macro_block_info_2[i].logic_id, macro_id, data_checksum));
    ASSERT_EQ(macro_block_info[i].macro_id, macro_id);
    ASSERT_EQ(macro_block_info[i].data_checksum, data_checksum);
  }
}

TEST_F(ObMacroBlockReuseMgrTest, test_invalid_input)
{
  const int64_t TEST_MACRO_BLOCK_COUNT = 3;
  const ObLogicMacroBlockId invalid_logic_id;
  const MacroBlockId invalid_macro_id;
  const int64_t invalid_data_checksum = -1;

  MacroBlockInfo macro_block_infos[TEST_MACRO_BLOCK_COUNT] = {
    {invalid_logic_id, MacroBlockId(1, 1, 1, 1), 1},
    {ObLogicMacroBlockId(2, 2, 2), invalid_macro_id, 2},
    {ObLogicMacroBlockId(3, 3, 3), MacroBlockId(3, 3, 3, 3), invalid_data_checksum}
  };

  // test invalid add
  for (int64_t i = 0; i < TEST_MACRO_BLOCK_COUNT; ++i) {
    ASSERT_EQ(OB_INVALID_ARGUMENT, reuse_mgr_.add_macro_block_reuse_info(macro_block_infos[i].logic_id, macro_block_infos[i].macro_id, macro_block_infos[i].data_checksum));
  }

  // test invalid get
  MacroBlockId macro_id;
  int64_t data_checksum = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, reuse_mgr_.get_macro_block_reuse_info(invalid_logic_id, macro_id, data_checksum));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_macro_block_reuse_mgr.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_macro_block_reuse_mgr.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
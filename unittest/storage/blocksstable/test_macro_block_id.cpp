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
#define protected public
#define private public
#include "storage/blocksstable/ob_object_manager.h"


namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace unittest
{
class TestMacroBlockId : public ::testing::Test
{
public:
  TestMacroBlockId() = default;
  void SetUp() {}
  void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

TEST_F(TestMacroBlockId, local_mode)
{
  int ret = OB_SUCCESS;
  MacroBlockId m_local(77, (1L << 33), 0);
  OB_LOG(INFO, "local", K(m_local));
  OB_LOG(INFO, "raw", K(m_local.write_seq_), K(m_local.second_id_));
  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION_V2 == m_local.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL, m_local.id_mode_);
  ASSERT_EQ(77, m_local.write_seq_);
  ASSERT_EQ((1L << 33), m_local.block_index_);
  ASSERT_EQ(0, m_local.third_id_);

  const int64_t buf_len = 32;
  char buf[buf_len] = {0};

  int64_t pos = 0;
  ret = m_local.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(m_local.get_serialize_size() == pos);

  MacroBlockId m_local_des;
  pos = 0;
  ret = m_local_des.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "local", K(m_local_des));
  OB_LOG(INFO, "raw", K(m_local_des.write_seq_), K(m_local_des.second_id_));
  ASSERT_TRUE(m_local.get_serialize_size() == pos);

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION_V2 == m_local_des.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL, m_local_des.id_mode_);
  ASSERT_EQ(m_local.write_seq_, m_local_des.write_seq_);
  ASSERT_EQ((1L << 33), m_local_des.block_index_);
  ASSERT_EQ(0, m_local_des.third_id_);
}

TEST_F(TestMacroBlockId, verification)
{
  int ret = OB_SUCCESS;
  MacroBlockId test_id(0, -3, 0);
  ASSERT_FALSE(test_id.is_valid());
  test_id.block_index_ = -2;
  ASSERT_FALSE(test_id.is_valid());
  test_id.block_index_ = MacroBlockId::AUTONOMIC_BLOCK_INDEX;
  ASSERT_TRUE(test_id.is_valid());

  test_id.third_id_ = 1;
  ASSERT_TRUE(test_id.is_valid());
  test_id.third_id_ = -1;
  ASSERT_FALSE(test_id.is_valid());

  test_id.third_id_ = 0;
  const int64_t buf_len = 24;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ret = test_id.serialize(buf, 23, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);

  ret = test_id.deserialize(buf, 23, pos);
  ASSERT_EQ(OB_DESERIALIZE_ERROR, ret);
  ASSERT_EQ(0, pos);
}

TEST_F(TestMacroBlockId, test_transfer_seq)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectOpt curr_opt;
  MacroBlockId test_block_id;
  uint64_t test_ls_id = 1001;
  uint64_t test_tablet_id = 200001;
  uint64_t test_tablet_version = ObStorageObjectOpt::INVALID_TABLET_VERSION;
  int64_t test_transfer_seq = ObStorageObjectOpt::INVALID_TABLET_TRANSFER_SEQ;
  curr_opt.set_ss_private_tablet_meta_object_opt(test_ls_id, test_tablet_id, test_tablet_version, test_transfer_seq);
  OB_LOG(INFO, "before set");
  hex_dump(&test_block_id.fourth_id_,
           sizeof(int64_t),
           true,
           OB_LOG_LEVEL_WARN);

  // in ss mode
  test_block_id.set_version_v2();
  test_block_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  test_block_id.set_storage_object_type(static_cast<int64_t>(curr_opt.object_type_));
  test_block_id.set_incarnation_id(0);
  test_block_id.set_column_group_id(0);
  test_block_id.set_second_id(curr_opt.ss_private_tablet_opt_.ls_id_);
  test_block_id.set_third_id(curr_opt.ss_private_tablet_opt_.tablet_id_);
  test_block_id.set_meta_version_id(curr_opt.ss_private_tablet_opt_.version_);
  test_block_id.set_meta_transfer_seq(curr_opt.ss_private_tablet_opt_.tablet_transfer_seq_);

  OB_LOG(INFO, "after set");
  hex_dump(&test_block_id.fourth_id_,
           sizeof(int64_t),
           true,
           OB_LOG_LEVEL_WARN);

  OB_LOG(INFO, "show test_block_id", K(test_block_id), K(test_block_id.meta_transfer_seq()), K(test_block_id.meta_version_id()));
  int64_t transfer_seq1 = test_block_id.meta_transfer_seq();
  OB_LOG(INFO, "transfer_seq1");
  hex_dump(&transfer_seq1,
           sizeof(int64_t),
           true,
           OB_LOG_LEVEL_WARN);
  int64_t tablet_version1 = test_block_id.meta_version_id();
  OB_LOG(INFO, "tablet_version1");
  hex_dump(&tablet_version1,
           sizeof(int64_t),
           true,
           OB_LOG_LEVEL_WARN);

  ASSERT_EQ(-1, transfer_seq1);
  ASSERT_EQ(17592186044415, tablet_version1); // compile failed when using ObStorageObjectOpt::INVALID_TABLET_VERSION
  ASSERT_FALSE(test_block_id.is_valid());

  const int64_t buf_len = 50;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ret = test_block_id.serialize(buf, 50, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);

  test_block_id.set_meta_transfer_seq(0);
  test_block_id.set_meta_version_id(100002);

  ret = test_block_id.serialize(buf, 50, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(32, pos);

  pos = 0;
  ret = test_block_id.deserialize(buf, 50, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(32, pos);
}
}
}

int main(int argc, char **argv)
{
  system("rm -f test_macro_block_id.log*");
  //OB_LOGGER.set_file_name("test_macro_block_id.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_macro_block_id");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

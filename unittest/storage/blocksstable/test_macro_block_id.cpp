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

#include <errno.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;

namespace unittest {
class TestMacroBlockId : public ::testing::Test {
public:
  TestMacroBlockId() = default;
  void SetUp()
  {}
  void TearDown()
  {}
  static void SetUpTestCase()
  {}
  static void TearDownTestCase()
  {}
};

TEST_F(TestMacroBlockId, local_mode)
{
  int ret = OB_SUCCESS;

  MacroBlockId::DEFAULT_MODE = ObMacroBlockIdMode::ID_MODE_LOCAL;
  MacroBlockId m_local;
  m_local.disk_no_ = 2;
  m_local.disk_install_count_ = 16;
  m_local.write_seq_ = 77;
  m_local.block_index_ = (1L << 33);
  OB_LOG(INFO, "local", K(m_local));
  OB_LOG(INFO, "raw", K(m_local.first_id_), K(m_local.second_id_));
  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_local.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL, m_local.id_mode_);
  ASSERT_EQ(2, m_local.disk_no_);
  ASSERT_EQ(16, m_local.disk_install_count_);
  ASSERT_EQ(77, m_local.write_seq_);
  ASSERT_EQ((1L << 33), m_local.block_index_);
  ASSERT_EQ(0, m_local.third_id_);
  ASSERT_EQ(0, m_local.fourth_id_);

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
  OB_LOG(INFO, "local", K(m_local_des), LITERAL_K(MacroBlockId::SF_FILTER_OCCUPY_LOCAL));
  OB_LOG(INFO, "raw", K(m_local_des.first_id_), K(m_local_des.second_id_));
  ASSERT_TRUE(m_local.get_serialize_size() == pos);

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_local_des.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL, m_local_des.id_mode_);
  ASSERT_EQ(0, m_local_des.disk_no_);
  ASSERT_EQ(0, m_local_des.disk_install_count_);
  ASSERT_EQ(0, m_local_des.write_seq_);
  ASSERT_EQ((1L << 33), m_local_des.block_index_);
  ASSERT_EQ(0, m_local_des.third_id_);
  ASSERT_EQ(0, m_local_des.fourth_id_);
}

TEST_F(TestMacroBlockId, local_compatibility)
{
  int ret = OB_SUCCESS;
  MacroBlockId::DEFAULT_MODE = ObMacroBlockIdMode::ID_MODE_LOCAL;
  MacroBlockId m_local;
  m_local.disk_no_ = 2;
  m_local.disk_install_count_ = 16;
  m_local.write_seq_ = 77;
  m_local.block_index_ = 2020;
  OB_LOG(INFO, "local", K(m_local));

  const int64_t buf_len = 32;
  char buf[buf_len] = {0};

  int64_t pos = 0;
  ret = m_local.serialize_old(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  MacroBlockId m_local_des;
  pos = 0;
  ret = m_local_des.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "local", K(m_local_des));

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_local_des.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL, m_local_des.id_mode_);
  ASSERT_EQ(0, m_local_des.disk_no_);
  ASSERT_EQ(0, m_local_des.disk_install_count_);
  ASSERT_EQ(0, m_local_des.write_seq_);
  ASSERT_EQ(2020, m_local_des.block_index_);
  ASSERT_EQ(0, m_local_des.third_id_);
  ASSERT_EQ(0, m_local_des.fourth_id_);
}

TEST_F(TestMacroBlockId, deserialize_local_old_invalid)
{
  int ret = OB_SUCCESS;

  const int64_t local_old_invalid = MacroBlockId::INVALID_BLOCK_INDEX_OLD_FORMAT;
  MacroBlockId::DEFAULT_MODE = ObMacroBlockIdMode::ID_MODE_LOCAL;
  MacroBlockId m_local;
  m_local.set_local_block_id(local_old_invalid);
  OB_LOG(INFO, "local_old_invalid", K(m_local), K(local_old_invalid));
  ASSERT_TRUE(m_local.is_valid());

  const int64_t buf_len = 32;
  char buf[buf_len] = {0};

  int64_t pos = 0;
  ret = m_local.serialize_old(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  MacroBlockId m_local_des;
  pos = 0;
  ret = m_local_des.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "local_old_invalid", K(m_local_des));
  ASSERT_FALSE(m_local_des.is_valid());

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_local_des.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL, m_local_des.id_mode_);
  ASSERT_EQ(0, m_local_des.disk_no_);
  ASSERT_EQ(0, m_local_des.disk_install_count_);
  ASSERT_EQ(0, m_local_des.write_seq_);
  ASSERT_EQ(INT64_MAX, m_local_des.block_index_);
  ASSERT_EQ(0, m_local_des.third_id_);
  ASSERT_EQ(0, m_local_des.fourth_id_);
}

TEST_F(TestMacroBlockId, append_compatibility)
{
  int ret = OB_SUCCESS;
  const int64_t FILE_ID = 345;
  const int64_t FILE_OFFSET = 16 << 20;

  MacroBlockId::DEFAULT_MODE = ObMacroBlockIdMode::ID_MODE_APPEND;
  MacroBlockId m_append;
  m_append.set_append_block_id(FILE_ID, FILE_OFFSET);
  OB_LOG(INFO, "append", K(m_append));

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_append.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_APPEND, m_append.id_mode_);
  ASSERT_EQ(FILE_ID, m_append.file_id_);
  ASSERT_EQ(FILE_OFFSET, m_append.file_offset_);
  ASSERT_EQ(0, m_append.third_id_);
  ASSERT_EQ(0, m_append.fourth_id_);

  const int64_t buf_len = 32;
  char buf[buf_len] = {0};

  int64_t pos = 0;
  ret = m_append.serialize_old(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  MacroBlockId m_append_des;
  pos = 0;
  ret = m_append_des.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "append", K(m_append_des));

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_append_des.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_APPEND, m_append_des.id_mode_);
  ASSERT_EQ(0, m_append_des.file_id_);
  ASSERT_EQ(FILE_OFFSET, m_append_des.file_offset_);
  ASSERT_EQ(0, m_append_des.third_id_);
  ASSERT_EQ(0, m_append.fourth_id_);
}

TEST_F(TestMacroBlockId, append_mode)
{
  int ret = OB_SUCCESS;
  const int64_t FILE_ID = 345;
  const int64_t FILE_OFFSET = 16 << 20;

  MacroBlockId::DEFAULT_MODE = ObMacroBlockIdMode::ID_MODE_APPEND;
  MacroBlockId m_append;
  m_append.set_append_block_id(FILE_ID, FILE_OFFSET);
  OB_LOG(INFO, "append", K(m_append));

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_append.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_APPEND, m_append.id_mode_);
  ASSERT_EQ(FILE_ID, m_append.file_id_);
  ASSERT_EQ(FILE_OFFSET, m_append.file_offset_);
  ASSERT_EQ(0, m_append.third_id_);
  ASSERT_EQ(0, m_append.fourth_id_);

  const int64_t buf_len = 32;
  char buf[buf_len] = {0};

  int64_t pos = 0;
  ret = m_append.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(m_append.get_serialize_size() == pos);

  MacroBlockId m_append_des;
  pos = 0;
  ret = m_append_des.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "append", K(m_append_des));
  ASSERT_TRUE(m_append_des.get_serialize_size() == pos);

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_append_des.version_);
  ASSERT_EQ((uint64_t)ObMacroBlockIdMode::ID_MODE_APPEND, m_append_des.id_mode_);
  ASSERT_TRUE(m_append == m_append_des);
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_macro_block_id.log*");
  // OB_LOGGER.set_file_name("test_hash_performance.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_macro_block_id");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

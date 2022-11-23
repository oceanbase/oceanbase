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
  MacroBlockId m_local;
  m_local.write_seq_ = 77;
  m_local.block_index_ = (1L << 33);
  OB_LOG(INFO, "local", K(m_local));
  OB_LOG(INFO, "raw", K(m_local.write_seq_), K(m_local.second_id_));
  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_local.version_);
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

  ASSERT_TRUE(MacroBlockId::MACRO_BLOCK_ID_VERSION == m_local_des.version_);
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
  ASSERT_FALSE(test_id.is_valid());
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

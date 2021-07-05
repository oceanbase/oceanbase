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

#include "clog/ob_log_block.h"

#include <gtest/gtest.h>

using namespace oceanbase::common;
using namespace oceanbase::clog;

namespace oceanbase {
namespace unittest {
class TestObLogBlockMetaV2 : public ::testing::Test {
public:
  TestObLogBlockMetaV2()
  {}
  ~TestObLogBlockMetaV2()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

TEST_F(TestObLogBlockMetaV2, normal)
{
  const int64_t ALIGN_SIZE = 4096;
  const int64_t DATA_SIZE = 1000;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  bool check_ret = false;

  char buf[ALIGN_SIZE];
  char data[DATA_SIZE];
  memset(buf, 0, ALIGN_SIZE);
  memset(data, 'A', DATA_SIZE);

  ObLogBlockMetaV2 old_meta;
  const int64_t meta_size = old_meta.get_serialize_size();
  ret = old_meta.build_serialized_block(buf, ALIGN_SIZE, data, DATA_SIZE, ObBlockType::OB_DATA_BLOCK, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(old_meta.get_serialize_size(), pos);
  ASSERT_EQ(ALIGN_SIZE, old_meta.get_total_len());
  ASSERT_EQ(ALIGN_SIZE - meta_size - DATA_SIZE, old_meta.get_padding_len());

  memcpy(buf + pos, data, DATA_SIZE);

  int16_t my_magic = 0;
  int64_t my_pos = 0;
  ret = serialization::decode_i16(buf, sizeof(int16_t), my_pos, &my_magic);
  ASSERT_EQ(OB_SUCCESS, ret);

  check_ret = old_meta.check_integrity(buf + old_meta.get_serialize_size(), old_meta.get_data_len());
  ASSERT_TRUE(check_ret);

  ObLogBlockMetaV2 new_meta;
  pos = 0;
  ret = new_meta.deserialize(buf, DATA_SIZE, pos);
  ASSERT_EQ(old_meta.get_serialize_size(), pos);
  ASSERT_EQ(new_meta.get_serialize_size(), pos);

  check_ret = new_meta.check_integrity(buf + new_meta.get_serialize_size(), new_meta.get_data_len());
  ASSERT_TRUE(check_ret);
}

TEST_F(TestObLogBlockMetaV2, deserialize_old_format)
{
  const int64_t ALIGN_SIZE = 4096;
  const int64_t DATA_SIZE = 1000;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  bool check_ret = false;

  char buf[ALIGN_SIZE];
  char data[DATA_SIZE];
  memset(buf, 0, ALIGN_SIZE);
  memset(data, 'A', DATA_SIZE);

  ObLogBlockMetaV2 old_meta;
  ret = old_meta.build_serialized_block(buf, ALIGN_SIZE, data, DATA_SIZE, ObBlockType::OB_DATA_BLOCK, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(old_meta.get_serialize_size(), pos);

  memcpy(buf + pos, data, DATA_SIZE);
  check_ret = old_meta.check_integrity(data, old_meta.get_data_len());
  ASSERT_TRUE(check_ret);

  ObLogBlockMetaV2 new_meta;
  pos = 0;
  ret = new_meta.deserialize(buf, ALIGN_SIZE, pos);
  ASSERT_EQ(new_meta.get_serialize_size(), pos);
  ASSERT_EQ(old_meta.get_serialize_size(), new_meta.get_serialize_size());
  ASSERT_EQ(old_meta.get_data_checksum(), new_meta.get_data_checksum());

  check_ret = new_meta.check_integrity(buf + new_meta.get_serialize_size(), DATA_SIZE);
  ASSERT_TRUE(check_ret);
}
}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_block_meta_v2.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_block_meta_v2");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

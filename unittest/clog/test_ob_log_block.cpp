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
class TestObLogBlock : public ::testing::Test {
public:
  virtual void SetUp()
  {
    type = OB_DATA_BLOCK;
    memset(data, 'A', BUFSIZE);
    memset(buf, 0, BUFSIZE);
  }
  virtual void TearDown()
  {}

  static const int64_t ALIGN_SIZE = 1 << 12;
  static const int64_t BUFSIZE = 1 << 13;
  ObBlockType type;
  char data[BUFSIZE];  // Store data block
  char buf[BUFSIZE];   // Used to serialize and deserialize
};

TEST_F(TestObLogBlock, test_just_one_block)
{
  ObLogBlockMetaV2::MetaContent one_blk;
  ObLogBlockMetaV2::MetaContent one_blk2;
  int64_t meta_size = one_blk.get_serialize_size();
  int64_t one_data_size = ALIGN_SIZE - meta_size;
  int64_t one_block_size = ALIGN_SIZE;
  int64_t checksum = 0;
  int16_t magic = 0;

  int64_t pos = 0;

  EXPECT_EQ(OB_SUCCESS, one_blk.generate_block(data, one_data_size, type));
  EXPECT_EQ(OB_SUCCESS, one_blk2.generate_block(NULL, 0, type));

  EXPECT_EQ(one_block_size, one_blk.get_total_len());
  EXPECT_EQ(one_data_size, one_blk.get_data_len());

  EXPECT_TRUE(one_blk.check_integrity(data, one_data_size));
  EXPECT_FALSE(one_blk2.check_integrity(NULL, 0));

  data[1] = 'B';
  EXPECT_FALSE(one_blk.check_integrity(data, one_data_size));

  EXPECT_FALSE(one_blk.check_integrity(data, -1));
  EXPECT_FALSE(one_blk.check_integrity(data, one_data_size - 1));
  EXPECT_FALSE(one_blk.check_integrity(NULL, one_data_size));

  EXPECT_TRUE(one_blk2.check_integrity(data, 0));
  EXPECT_FALSE(one_blk2.check_integrity(NULL, one_data_size));

  one_blk2.reset();
  EXPECT_FALSE(one_blk2.check_integrity(NULL, 0));

  data[1] = 'A';
  checksum = one_blk.get_data_checksum();
  magic = one_blk.get_magic();

  CLOG_LOG(INFO, "before serialize:", "meta", to_cstring(one_blk));
  EXPECT_EQ(OB_INVALID_ARGUMENT, one_blk.serialize(buf, 0, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, one_blk.serialize(NULL, 0, pos));
  EXPECT_EQ(OB_SERIALIZE_ERROR, one_blk.serialize(buf, 1, pos));
  EXPECT_EQ(OB_SUCCESS, one_blk.serialize(buf, BUFSIZE, pos));

  pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, one_blk.deserialize(NULL, one_data_size, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, one_blk.deserialize(buf, -1, pos));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, one_blk.deserialize(buf, 1, pos));
  EXPECT_EQ(OB_SUCCESS, one_blk.deserialize(buf, one_data_size, pos));
  CLOG_LOG(INFO, "after serialize:", "meta", to_cstring(one_blk));

  EXPECT_EQ(checksum, one_blk.get_data_checksum());
  EXPECT_EQ(magic, one_blk.get_magic());

  data[1] = 'A';
  EXPECT_TRUE(one_blk.check_integrity(data, one_data_size));

  buf[meta_size - 1] = 'B';
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, one_blk.deserialize(buf, one_data_size, pos));
  EXPECT_FALSE(one_blk.check_integrity(data, one_data_size));
}

TEST_F(TestObLogBlock, test_small_block)
{
  ObLogBlockMetaV2::MetaContent small_blk;

  int64_t small_data_size = (ALIGN_SIZE >> 1) + 3;
  int64_t small_block_size = ALIGN_SIZE;

  int64_t pos = 0;

  EXPECT_EQ(OB_SUCCESS, small_blk.generate_block(data, small_data_size, type));

  EXPECT_EQ(small_data_size, small_blk.get_data_len());
  EXPECT_EQ(small_block_size, small_blk.get_total_len());

  EXPECT_TRUE(small_blk.check_integrity(data, small_data_size));
  data[1] = 'B';
  EXPECT_FALSE(small_blk.check_integrity(data, small_data_size));
  data[1] = 'A';

  EXPECT_EQ(OB_SUCCESS, small_blk.serialize(buf, BUFSIZE, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, small_blk.deserialize(buf, BUFSIZE, pos));

  EXPECT_TRUE(small_blk.check_integrity(data, small_data_size));
}

TEST_F(TestObLogBlock, test_large_block)
{
  ObLogBlockMetaV2::MetaContent large_blk;

  int64_t large_data_size = ALIGN_SIZE << 1;
  int64_t large_block_size = ALIGN_SIZE * 3;

  int64_t pos = 0;

  EXPECT_EQ(OB_SUCCESS, large_blk.generate_block(data, large_data_size, type));

  EXPECT_EQ(large_data_size, large_blk.get_data_len());
  EXPECT_EQ(large_block_size, large_blk.get_total_len());

  EXPECT_TRUE(large_blk.check_integrity(data, large_data_size));
  data[1] = 'B';
  EXPECT_FALSE(large_blk.check_integrity(data, large_data_size));
  data[1] = 'A';

  EXPECT_EQ(OB_SUCCESS, large_blk.serialize(buf, BUFSIZE, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, large_blk.deserialize(buf, BUFSIZE, pos));

  EXPECT_TRUE(large_blk.check_integrity(data, large_data_size));
}
}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_block.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_block");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

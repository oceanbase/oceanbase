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

#include "storage/blocksstable/ob_sstable_macro_block_header.h"
#include "storage/blocksstable/ob_macro_block.h"

using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace unittest
{

class TestSSTableMacroBlockHeader : public ::testing::Test
{
public:
  TestSSTableMacroBlockHeader();
  virtual ~TestSSTableMacroBlockHeader() = default;
  virtual void SetUp();
  virtual void TearDown();
private:
  static const int64_t ROWKEY_COLUMN_COUNT = 2;
  static const int64_t COLUMN_COUNT = 3;
  common::ObObjMeta column_types_[COLUMN_COUNT];
  common::ObOrderType column_orders_[COLUMN_COUNT];
  int64_t column_checksum_[COLUMN_COUNT];
  ObSSTableMacroBlockHeader macro_header_;
};

TestSSTableMacroBlockHeader::TestSSTableMacroBlockHeader()
  : macro_header_()
{
}

void TestSSTableMacroBlockHeader::SetUp()
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc desc;
  ObTabletID tablet_id(1);
  desc.ls_id_.id_ = 1;
  desc.tablet_id_ = tablet_id;
  desc.micro_block_size_ = 8 * 1024;
  desc.micro_block_size_limit_ = 8 * 1024;
  desc.row_column_count_ = COLUMN_COUNT;
  desc.rowkey_column_count_ = ROWKEY_COLUMN_COUNT;
  desc.schema_version_ = 0;
  desc.schema_rowkey_col_cnt_ = ROWKEY_COLUMN_COUNT;
  desc.snapshot_version_ = 1;
  desc.end_scn_.set_base();
  desc.merge_type_ = MAJOR_MERGE;
  desc.col_desc_array_.init(desc.row_column_count_);
  desc.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  for (int64_t i = 0; i < COLUMN_COUNT; i++) {
    share::schema::ObColDesc col_desc;
    col_desc.col_type_.set_int32();
    EXPECT_EQ(OB_SUCCESS, desc.col_desc_array_.push_back(col_desc));
  }
  desc.rowkey_column_count_ = ROWKEY_COLUMN_COUNT;
  ret = macro_header_.init(desc, column_types_, column_orders_, column_checksum_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestSSTableMacroBlockHeader::TearDown()
{
  macro_header_.reset();
}

TEST_F(TestSSTableMacroBlockHeader, serialize_and_deserialize)
{
  int ret = OB_SUCCESS;
  char buf[1024];
  const int64_t buf_len = 1024;
  int64_t pos = 0;
  ret = macro_header_.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  macro_header_.fixed_header_.row_count_ = 100;
  macro_header_.fixed_header_.occupy_size_ = 400;
  macro_header_.fixed_header_.micro_block_count_ = 1;
  macro_header_.fixed_header_.micro_block_data_offset_ = 400;
  macro_header_.fixed_header_.micro_block_data_size_ = 400;
  ret = macro_header_.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  macro_header_.reset();

  pos = 0;
  ret = macro_header_.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_macro_block_header.log*");
  OB_LOGGER.set_file_name("test_sstable_macro_block_header.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

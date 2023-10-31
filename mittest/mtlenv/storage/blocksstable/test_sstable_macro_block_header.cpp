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
#include "storage/blocksstable/ob_data_store_desc.h"

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
  ObWholeDataStoreDesc desc;
  ObDataStoreDesc &data_desc = desc.get_desc();
  ObStaticDataStoreDesc &static_desc = desc.get_static_desc();
  ObColDataStoreDesc &col_desc = desc.get_col_desc();
  data_desc.static_desc_ = &static_desc;
  data_desc.col_desc_ = &col_desc;

  ObTabletID tablet_id(1);
  col_desc.allocator_.set_tenant_id(500);
  static_desc.ls_id_.id_ = 1;
  static_desc.tablet_id_ = tablet_id;
  data_desc.micro_block_size_ = 8 * 1024;
  static_desc.micro_block_size_limit_ = 8 * 1024;
  col_desc.row_column_count_ = COLUMN_COUNT;
  col_desc.rowkey_column_count_ = ROWKEY_COLUMN_COUNT;
  static_desc.schema_version_ = 0;
  col_desc.schema_rowkey_col_cnt_ = ROWKEY_COLUMN_COUNT;
  static_desc.snapshot_version_ = 1;
  static_desc.end_scn_.set_base();
  static_desc.merge_type_ = compaction::MAJOR_MERGE;
  col_desc.col_desc_array_.init(col_desc.row_column_count_);
  static_desc.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  for (int64_t i = 0; i < COLUMN_COUNT; i++) {
    share::schema::ObColDesc col;
    col.col_type_.set_int32();
    EXPECT_EQ(OB_SUCCESS, col_desc.col_desc_array_.push_back(col));
  }
  col_desc.rowkey_column_count_ = ROWKEY_COLUMN_COUNT;
  ret = macro_header_.init(data_desc, column_types_, column_orders_, column_checksum_);
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

TEST_F(TestSSTableMacroBlockHeader, serialize_and_deserialize_compat)
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


  const int64_t old_size = macro_header_.get_serialize_size() - sizeof(bool);
  const int64_t new_size = macro_header_.get_serialize_size();
  // fake old version header
  pos = 0;
  macro_header_.is_normal_cg_ = true;
  ret = macro_header_.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_size, pos);
  ObSSTableMacroBlockHeader::FixedHeader *fixed_header
      = reinterpret_cast<ObSSTableMacroBlockHeader::FixedHeader *>(buf);
  fixed_header->header_size_ = old_size;

  pos = 0;
  macro_header_.reset();
  ret = macro_header_.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(old_size, pos);
  ASSERT_FALSE(macro_header_.is_normal_cg_);
  ASSERT_EQ(macro_header_.fixed_header_.header_size_, new_size);

  pos = 0;
  macro_header_.is_normal_cg_ = true;
  ret = macro_header_.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_size, pos);

  pos = 0;
  macro_header_.reset();
  ret = macro_header_.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_size, pos);
  ASSERT_TRUE(macro_header_.is_normal_cg_);

  // fake invalid header
  pos = 0;
  macro_header_.fixed_header_.header_size_ = old_size;
  ret = macro_header_.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
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

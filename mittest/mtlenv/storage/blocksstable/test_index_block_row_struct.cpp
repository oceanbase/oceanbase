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
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_macro_block.h"
#include "mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;

namespace unittest
{

class TestIndexBlockRowStruct : public ::testing::Test
{
public:
  static const int64_t rowkey_column_count = 2;
public:
  TestIndexBlockRowStruct() : allocator_(), index_data_allocator_(), desc_(), data_desc_()  {}
  virtual ~TestIndexBlockRowStruct() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
public:
  ObArenaAllocator allocator_;
  ObArenaAllocator index_data_allocator_;
  ObWholeDataStoreDesc desc_;
  ObWholeDataStoreDesc data_desc_;
};

void TestIndexBlockRowStruct::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestIndexBlockRowStruct::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestIndexBlockRowStruct::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  ObDataStoreDesc &data_desc = desc_.get_desc();
  ObStaticDataStoreDesc &static_desc = desc_.get_static_desc();
  ObColDataStoreDesc &col_desc = desc_.get_col_desc();
  data_desc.static_desc_ = &static_desc;
  data_desc.col_desc_ = &col_desc;

  static_desc.ls_id_.id_ = 1;
  static_desc.tablet_id_.id_ = 1;
  static_desc.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  data_desc.micro_block_size_ = 8 * 1024;
  static_desc.micro_block_size_limit_ = 8 * 1024;
  col_desc.row_column_count_ = rowkey_column_count + 1;
  col_desc.rowkey_column_count_ = rowkey_column_count;
  static_desc.schema_version_ = 0;
  col_desc.schema_rowkey_col_cnt_ = rowkey_column_count;
  static_desc.snapshot_version_ = 1;
  static_desc.end_scn_.set_base();
  static_desc.merge_type_ = compaction::MAJOR_MERGE;
  col_desc.col_desc_array_.init(col_desc.row_column_count_);
  col_desc.agg_meta_array_.init(col_desc.row_column_count_ * 3);
  for (int64_t i = 0; i < 3; i++) {
    share::schema::ObColDesc col;
    col.col_type_.set_int32();
    EXPECT_EQ(OB_SUCCESS, col_desc.col_desc_array_.push_back(col));

    ObSkipIndexColMeta agg_meta;
    agg_meta.col_idx_ = i;
    agg_meta.col_type_ = ObSkipIndexColType::SK_IDX_MIN;
    EXPECT_EQ(OB_SUCCESS, col_desc.agg_meta_array_.push_back(agg_meta));
    agg_meta.col_type_ = ObSkipIndexColType::SK_IDX_MAX;
    EXPECT_EQ(OB_SUCCESS, col_desc.agg_meta_array_.push_back(agg_meta));
    agg_meta.col_type_ = ObSkipIndexColType::SK_IDX_NULL_COUNT;
    EXPECT_EQ(OB_SUCCESS, col_desc.agg_meta_array_.push_back(agg_meta));
  }
  col_desc.rowkey_column_count_ = rowkey_column_count;
  EXPECT_EQ(OB_SUCCESS, col_desc.datum_utils_.init(col_desc.col_desc_array_, col_desc.schema_rowkey_col_cnt_, lib::is_oracle_mode(), allocator_)); // data desc same with index desc
  EXPECT_EQ(OB_SUCCESS, data_desc_.assign(desc_.get_desc())); // data desc same with index desc
}

void TestIndexBlockRowStruct::TearDown()
{
  allocator_.reset();
  index_data_allocator_.reset();
}

TEST_F(TestIndexBlockRowStruct, test_invalid)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_column_count = 2;
  ObStorageDatum obj[2];
  obj[0].set_int(1);
  obj[1].set_int(1);
  ObDatumRowkey row_key;
  EXPECT_EQ(OB_SUCCESS, row_key.assign(obj, 2));

  ObIndexBlockRowBuilder row_builder;
  ret = row_builder.init(allocator_, index_data_allocator_, data_desc_.get_desc(), data_desc_.get_desc());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = row_builder.init(allocator_, index_data_allocator_, data_desc_.get_desc(), data_desc_.get_desc());
  EXPECT_NE(OB_SUCCESS, ret);

  const ObDatumRow *row;
  ObIndexBlockRowDesc desc;
  desc.row_key_ = row_key;
  ret = row_builder.build_row(desc, row);
  EXPECT_NE(OB_SUCCESS, ret);
}

TEST_F(TestIndexBlockRowStruct, test_normal)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_column_count = 2;
  ObStorageDatum obj[2];
  obj[0].set_int(1);
  obj[1].set_int(1);
  ObDatumRowkey row_key;
  EXPECT_EQ(OB_SUCCESS, row_key.assign(obj, 2));

  ObIndexBlockRowBuilder row_builder;
  ret = row_builder.init(allocator_, index_data_allocator_, data_desc_.get_desc(), data_desc_.get_desc());
  ASSERT_EQ(OB_SUCCESS, ret);

  ObIndexBlockRowDesc row_desc;
  row_desc.data_store_desc_ = &desc_.get_desc();
  row_desc.row_key_ = row_key;
  ASSERT_TRUE(row_desc.is_valid());

  const ObDatumRow *row;
  ret = row_builder.build_row(row_desc, row);
  EXPECT_EQ(OB_SUCCESS, ret);
  row_builder.reset();
}

TEST_F(TestIndexBlockRowStruct, test_parser_normal)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_column_count = 2;
  ObStorageDatum obj[2];
  obj[0].set_int(1);
  obj[1].set_int(2);
  ObDatumRowkey row_key;
  EXPECT_EQ(OB_SUCCESS, row_key.assign(obj, 2));

  ObDatumRow agg_row;
  ASSERT_EQ(OB_SUCCESS, agg_row.init(allocator_, desc_.get_col_desc().agg_meta_array_.count()));
  for (int64_t i = 0; i < agg_row.get_column_count(); ++i) {
    agg_row.storage_datums_[i].set_int(0);
  }

  ObIndexBlockRowBuilder row_builder;
  ASSERT_EQ(OB_SUCCESS, row_builder.init(allocator_, index_data_allocator_, data_desc_.get_desc(), data_desc_.get_desc()));

  ObIndexBlockRowDesc row_desc;
  row_desc.block_size_ = 1024;
  row_desc.is_deleted_ = false;
  row_desc.block_offset_ = 128;
  row_desc.is_data_block_ = true;
  row_desc.is_macro_node_ = false;
  row_desc.micro_block_count_ = 1;
  row_desc.aggregated_row_ = &agg_row;
  row_desc.is_serialized_agg_row_ = false;

  row_desc.data_store_desc_ = &desc_.get_desc();
  row_desc.row_key_ = row_key;
  const ObDatumRow *row;
  ret = row_builder.build_row(row_desc, row);
  EXPECT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "intermediate row info,", K(*row));

  //paser copy row
  ObIndexBlockRowParser row_parser;
  const ObIndexBlockRowHeader *parsed_header = nullptr;
  ret = row_parser.init(rowkey_column_count, *row);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = row_parser.get_header(parsed_header);
  EXPECT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "header info,", K(*parsed_header));

  EXPECT_TRUE(parsed_header->is_valid());
  EXPECT_TRUE(parsed_header->is_data_block());
  EXPECT_TRUE(parsed_header->is_data_index());
  EXPECT_FALSE(parsed_header->is_macro_node());
  EXPECT_EQ(parsed_header->get_compressor_type(), ObCompressorType::NONE_COMPRESSOR);

  const ObIndexBlockRowMinorMetaInfo *minor_meta = nullptr;

  ret = row_parser.get_minor_meta(minor_meta);
  EXPECT_NE(OB_SUCCESS, ret);

  const char *pre_agg_row = nullptr;
  int64_t row_size = 0;
  EXPECT_EQ(OB_SUCCESS, row_parser.get_agg_row(pre_agg_row, row_size));

  ObAggRowReader agg_row_reader;
  EXPECT_EQ(OB_SUCCESS, agg_row_reader.init(pre_agg_row, row_size));
  for (int64_t i = 0; i < desc_.get_col_desc().agg_meta_array_.count(); ++i) {
    ObStorageDatum tmp_datum;
    EXPECT_EQ(OB_SUCCESS, agg_row_reader.read(desc_.get_col_desc().agg_meta_array_.at(i), tmp_datum));
    ASSERT_EQ(false, tmp_datum.is_null());
    ASSERT_EQ(0, tmp_datum.get_int());
  }

  row_builder.reset();
}

TEST_F(TestIndexBlockRowStruct, test_set_rowkey)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_column_count = 2;

  char buf[64]="setrowkey test";
  ObString rowkey2(buf);
  ObStorageDatum obj[2];
  obj[0].set_int(2);
  obj[1].set_string(rowkey2);
  ObDatumRowkey row_key;
  EXPECT_EQ(OB_SUCCESS, row_key.assign(obj, 2));

  ObIndexBlockRowBuilder row_builder;
  ret = row_builder.init(allocator_, index_data_allocator_, data_desc_.get_desc(), data_desc_.get_desc());
  EXPECT_EQ(OB_SUCCESS, ret);

  ObIndexBlockRowDesc row_desc;
  row_desc.data_store_desc_ = &desc_.get_desc();
  ASSERT_TRUE(row_desc.is_valid());

  const ObDatumRow *row;
  row_desc.row_key_ = row_key;
  ret = row_builder.build_row(row_desc, row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(row->storage_datums_[0] == row_key.datums_[0]);
  EXPECT_TRUE(row->storage_datums_[1] == row_key.datums_[1]);
  row_builder.reset();
}

}
}

int main(int argc, char** argv)
{
  system("rm -f test_index_block_row_struct.log*");
  OB_LOGGER.set_file_name("test_index_block_row_struct.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

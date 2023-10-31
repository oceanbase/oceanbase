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
#define protected public
#define private public
#include "storage/blocksstable/encoding/ob_micro_block_encoder.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/ob_i_store.h"
#include "lib/string/ob_sql_string.h"
#include "../ob_row_generate.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/ob_storage_format.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class TestMicroBlockDecoder : public ::testing::Test
{
public:
  TestMicroBlockDecoder(): tenant_ctx_(500)
  {
    decode_res_pool_ = new(allocator_.alloc(sizeof(ObDecodeResourcePool))) ObDecodeResourcePool();
    tenant_ctx_.set(decode_res_pool_);
    share::ObTenantEnv::set_tenant(&tenant_ctx_);
    encoder_.data_buffer_.allocator_.set_tenant_id(500);
    encoder_.row_buf_holder_.allocator_.set_tenant_id(500);
    decode_res_pool_->init();
  }
  static const int64_t ROWKEY_CNT = 1;
  static const int64_t COLUMN_CNT = ObExtendType - 1 + 7;
  static const int64_t ROW_CNT = 64;

  virtual void SetUp();
  virtual void TearDown() {}

protected:
  ObRowGenerate row_generate_;
  ObMicroBlockEncodingCtx ctx_;
  common::ObArray<share::schema::ObColDesc> col_descs_;
  ObMicroBlockEncoder encoder_;
  ObArenaAllocator allocator_;
  ObObjType *col_obj_types_;
  share::ObTenantBase tenant_ctx_;
  ObDecodeResourcePool *decode_res_pool_;
  int64_t extra_rowkey_cnt_;
  int64_t column_cnt_;
  int64_t full_column_cnt_;
  int64_t rowkey_cnt_;
};

void TestMicroBlockDecoder::SetUp()
{

  if (OB_NOT_NULL(col_obj_types_)) {
    allocator_.free(col_obj_types_);
  }
  column_cnt_ = ObExtendType - 1 + 7;
  rowkey_cnt_ = 2;
  col_obj_types_ = reinterpret_cast<ObObjType *>(allocator_.alloc(sizeof(ObObjType) * column_cnt_));
  for (int64_t i = 0; i < column_cnt_; ++i) {
    ObObjType type = static_cast<ObObjType>(i + 1);
    if (column_cnt_ - 1 == i) {
      type = ObURowIDType;
    } else if (column_cnt_ - 2 == i) {
      type = ObIntervalYMType;
    } else if (column_cnt_ - 3 == i) {
      type = ObIntervalDSType;
    } else if (column_cnt_ - 4 == i) {
      type = ObTimestampTZType;
    } else if (column_cnt_ - 5 == i) {
      type = ObTimestampLTZType;
    } else if (column_cnt_ - 6 == i) {
      type = ObTimestampNanoType;
    } else if (column_cnt_ - 7 == i) {
      type = ObRawType;
    } else if (type == ObExtendType || type == ObUnknownType) {
      type = ObVarcharType;
    }
    col_obj_types_[i] = type;
  }

  extra_rowkey_cnt_ = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  full_column_cnt_ = column_cnt_ + extra_rowkey_cnt_;
  const int64_t tid = 200001;
  ObTableSchema table;
  ObColumnSchemaV2 col;
  table.reset();
  table.set_tenant_id(1);
  table.set_tablegroup_id(1);
  table.set_database_id(1);
  table.set_table_id(tid);
  table.set_table_name("test_micro_block_decoder_schema");
  table.set_rowkey_column_num(rowkey_cnt_);
  table.set_max_column_id(column_cnt_ * 2);
  table.set_block_size(2 * 1024);
  table.set_compress_func_name("none");
  table.set_row_store_type(ENCODING_ROW_STORE);
  table.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);

  ObSqlString str;
  for (int64_t i = 0; i < COLUMN_CNT; ++i) {
    col.reset();
    col.set_table_id(tid);
    col.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    str.assign_fmt("test%ld", i);
    col.set_column_name(str.ptr());
    ObObjType type = col_obj_types_[i]; // 0 is ObNullType
    col.set_data_type(type);
    if (ObVarcharType == type || ObCharType == type || ObHexStringType == type
        || ObNVarchar2Type == type || ObNCharType == type || ObTextType == type){
      col.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      if (ObCharType == type) {
        const int64_t max_char_length = lib::is_oracle_mode()
                                        ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                        : OB_MAX_CHAR_LENGTH;
        col.set_data_length(max_char_length);
      }
    } else {
      col.set_collation_type(CS_TYPE_BINARY);
    }
    if (type == ObIntType) {
      col.set_rowkey_position(1);
    } else if (type == ObUInt64Type) {
      col.set_rowkey_position(2);
    } else{
      col.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table.add_column(col));
  }
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table, true/*multi_version*/));
  ASSERT_EQ(OB_SUCCESS, table.get_multi_version_column_descs(col_descs_));

  ctx_.micro_block_size_ = 64L << 11;
  ctx_.macro_block_size_ = 2L << 20;
  ctx_.rowkey_column_cnt_ = rowkey_cnt_ + extra_rowkey_cnt_;
  ctx_.column_cnt_ = column_cnt_ + extra_rowkey_cnt_;
  ctx_.col_descs_ = &col_descs_;
  ctx_.row_store_type_ = common::ENCODING_ROW_STORE;
  ctx_.compressor_type_ = common::ObCompressorType::NONE_COMPRESSOR;

  ASSERT_EQ(OB_SUCCESS, encoder_.init(ctx_));
}

TEST_F(TestMicroBlockDecoder, decode_test)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seeds[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    seeds[i] = 10000 + i;
  }
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seeds[i], row));
    LOG_INFO("generate row", K(i),  K(row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  LOG_INFO("col_descs before init decoder : ",K(col_descs_));
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, nullptr)) << "buffer size: " << data.get_buf_size() << std::endl;
  ObDatumRow single_row;
  ASSERT_EQ(OB_SUCCESS, single_row.init(allocator_, full_column_cnt_));
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, single_row));
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seeds[i], row));
    LOG_INFO("Current row: ", K(i));
    for (int64_t j = 0; j < full_column_cnt_; ++j) {
      LOG_INFO("current col: ", K(j), K(single_row.storage_datums_[j]), K(row.storage_datums_[j]));
      ASSERT_TRUE(ObDatum::binary_equal(single_row.storage_datums_[j], row.storage_datums_[j]));
    }
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_micro_block_decoder.log*");
  OB_LOGGER.set_file_name("test_micro_block_decoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

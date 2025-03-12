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
#include "ob_cs_encoding_test_base.h"
#include "ob_row_vector_converter.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_util.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_decoder.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

using ::testing::Bool;
using ::testing::Combine;

class TestAppendBatch : public ObCSEncodingTestBase, public ::testing::Test
{
public:
  TestAppendBatch() {}
  virtual ~TestAppendBatch() {}

  virtual void SetUp() {}
  virtual void TearDown() { reuse(); }
};

TEST_F(TestAppendBatch, test_integer_append)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 6;
  ObObjType col_types[col_cnt] = {ObIntType, ObUInt64Type, ObIntType, ObTinyIntType, ObSmallIntType, ObUSmallIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[4] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[5] = ObCSColumnHeader::Type::INTEGER;


  // integer not support VEC_DISCRETE and VEC_CONTINUOUS
  ObArray<VectorFormat> vec_formats;
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_FIXED));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_FIXED));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_FIXED));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_FIXED));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_UNIFORM));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_UNIFORM_CONST));


  const int64_t row_cnt = 400;
  const int64_t batch_size = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));

  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    const int64_t batch_idx = i / batch_size;
    if (batch_idx == 0) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
      row_arr[i].storage_datums_[3].set_null();
      row_arr[i].storage_datums_[4].set_null();
      row_arr[i].storage_datums_[5].set_null();
    } else if (batch_idx == 1) {
      row_arr[i].storage_datums_[1].set_int(i - 100);
      row_arr[i].storage_datums_[2].set_int(200);
      row_arr[i].storage_datums_[3].set_int(i % 64);
      row_arr[i].storage_datums_[4].set_int(i - 400);
      row_arr[i].storage_datums_[5].set_int(1000);
    } else if (batch_idx == 2) {
      if (i%2 == 0) {
        row_arr[i].storage_datums_[1].set_null();
        row_arr[i].storage_datums_[2].set_null();
        row_arr[i].storage_datums_[3].set_null();
        row_arr[i].storage_datums_[4].set_null();
      } else {
        row_arr[i].storage_datums_[1].set_int(i - 100);
        row_arr[i].storage_datums_[2].set_int(200);
        row_arr[i].storage_datums_[3].set_int(i % 65);
        row_arr[i].storage_datums_[4].set_int(i - 400);
      }
      row_arr[i].storage_datums_[5].set_int(2000);
    } else if (batch_idx == 3) {
      if (i%10 == 0) {
        row_arr[i].storage_datums_[1].set_null();
        row_arr[i].storage_datums_[2].set_null();
        row_arr[i].storage_datums_[3].set_null();
        row_arr[i].storage_datums_[4].set_null();
      } else {
        row_arr[i].storage_datums_[1].set_int(100);
        row_arr[i].storage_datums_[2].set_int(200);
        row_arr[i].storage_datums_[3].set_int(127);
        row_arr[i].storage_datums_[4].set_int(400);
      }
      row_arr[i].storage_datums_[5].set_int(500);
    } else {
      ASSERT_EQ(OB_SUCCESS, OB_ERR_UNEXPECTED);
    }
  }

  int64_t batch_idx = 0;
  ObRowVectorConverter vec_convertor;
  ASSERT_EQ(OB_SUCCESS, vec_convertor.init(col_descs_, vec_formats, batch_size));

  bool is_full = false;
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, vec_convertor.append_row(row_arr[i], is_full));
    if (is_full) {
      ObBatchDatumRows batch_rows;
      ASSERT_EQ(OB_SUCCESS, vec_convertor.get_batch_datums(batch_rows));
      if (batch_idx == 0) {
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, batch_size));
        vec_formats.at(1) = VectorFormat::VEC_UNIFORM;
        vec_formats.at(2) = VectorFormat::VEC_UNIFORM_CONST;
      } else if (batch_idx == 1) {
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, 1));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 1, 1));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 2, 2));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 4, batch_size - 4));
        vec_formats.at(1) =  VectorFormat::VEC_FIXED;
        vec_formats.at(2) = VectorFormat::VEC_UNIFORM;
      } else if (batch_idx == 2) {
        vec_formats.at(1) =  VectorFormat::VEC_FIXED;
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, 10));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 10, 20));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 30, 50));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 80, batch_size - 80));
      } else {
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, batch_size));
      }
      batch_idx++;
      vec_convertor.reset();
      ASSERT_EQ(OB_SUCCESS, vec_convertor.init(col_descs_, vec_formats, batch_size));
    }
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_INFO("finish build_micro_block_desc", K(micro_block_desc));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
}

TEST_F(TestAppendBatch, test_string_append)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 6;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObDecimalIntType, ObCharType, ObVarcharType, ObVarcharType, ObVarcharType};
  int64_t precision_arr[col_cnt] = {MAX_PRECISION_DECIMAL_INT_128, MAX_PRECISION_DECIMAL_INT_128};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt, ObCompressorType::ZSTD_1_3_8_COMPRESSOR, precision_arr));

  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STRING;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::STRING;
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::STRING;
  ctx_.column_encodings_[4] = ObCSColumnHeader::Type::STRING;
  ctx_.column_encodings_[5] = ObCSColumnHeader::Type::STRING;

  ObArray<VectorFormat> vec_formats;
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_FIXED));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_FIXED));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_DISCRETE));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_CONTINUOUS));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_UNIFORM));
  ASSERT_EQ(OB_SUCCESS, vec_formats.push_back(VectorFormat::VEC_UNIFORM_CONST));

  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  const int64_t row_cnt = 400;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  const int64_t batch_size = 100;

  char *char_data = static_cast<char *>(allocator_.alloc(1024));
  ASSERT_TRUE(nullptr != char_data);
  MEMSET(char_data, 0xbc, 1024);
  int128_t deci = (int128_t)INT64_MAX << 64;

  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    const int64_t batch_idx = i / batch_size;
    if (batch_idx == 0) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
      row_arr[i].storage_datums_[3].set_null();
      row_arr[i].storage_datums_[4].set_null();
      row_arr[i].storage_datums_[5].set_null();
    } else if (batch_idx == 1) {
      row_arr[i].storage_datums_[1].set_decimal_int(deci);
      row_arr[i].storage_datums_[2].set_string(char_data, 10);
      row_arr[i].storage_datums_[3].set_string(char_data, i % 20);
      row_arr[i].storage_datums_[4].set_string(char_data, i % 30);
      row_arr[i].storage_datums_[5].set_string(char_data, 40);
    } else if (batch_idx == 2) {
      if (i % 2 == 0) {
        row_arr[i].storage_datums_[1].set_null();
        row_arr[i].storage_datums_[2].set_null();
        row_arr[i].storage_datums_[3].set_null();
        row_arr[i].storage_datums_[4].set_null();
      } else {
        row_arr[i].storage_datums_[1].set_decimal_int(deci);
        row_arr[i].storage_datums_[2].set_string(char_data, 0);
        row_arr[i].storage_datums_[3].set_string(char_data, 0);
        row_arr[i].storage_datums_[4].set_string(char_data, 0);
      }
      row_arr[i].storage_datums_[5].set_string(char_data, 0);
    } else if (batch_idx == 3) {
      if (i % 10 == 0) {
        row_arr[i].storage_datums_[2].set_null();
        row_arr[i].storage_datums_[3].set_null();
        row_arr[i].storage_datums_[4].set_null();
      } else {
        row_arr[i].storage_datums_[2].set_string(char_data, 10);
        row_arr[i].storage_datums_[3].set_string(char_data, i % 20);
        row_arr[i].storage_datums_[4].set_string(char_data, i % 30);
      }
      row_arr[i].storage_datums_[1].set_decimal_int(deci); // use VEC_CONST
      row_arr[i].storage_datums_[5].set_string(char_data, 40);
    } else {
      ASSERT_EQ(OB_SUCCESS, OB_ERR_UNEXPECTED);
    }
  }

  int64_t batch_idx = 0;
  ObRowVectorConverter vec_convertor;
  ASSERT_EQ(OB_SUCCESS, vec_convertor.init(col_descs_, vec_formats, batch_size));

  bool is_full = false;
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, vec_convertor.append_row(row_arr[i], is_full));
    if (is_full) {
      ObBatchDatumRows batch_rows;
      ASSERT_EQ(OB_SUCCESS, vec_convertor.get_batch_datums(batch_rows));
      if (batch_idx == 0) {
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, batch_size));
        vec_formats.at(1) =  VectorFormat::VEC_UNIFORM;
        vec_formats.at(2) = VectorFormat::VEC_UNIFORM;
      } else if (batch_idx == 1) {
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, 1));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 1, 1));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 2, 2));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 4, batch_size - 4));
        vec_formats.at(1) =  VectorFormat::VEC_FIXED;
        vec_formats.at(2) = VectorFormat::VEC_DISCRETE;
      } else if (batch_idx == 2) {
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, 10));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 10, 20));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 30, 50));
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 80, batch_size - 80));
        vec_formats.at(1) =  VectorFormat::VEC_UNIFORM_CONST;
        vec_formats.at(2) = VectorFormat::VEC_CONTINUOUS;
      } else {
        ASSERT_EQ(OB_SUCCESS, encoder.append_batch(batch_rows, 0, batch_size));
      }
      batch_idx++;
      vec_convertor.reset();
      ASSERT_EQ(OB_SUCCESS, vec_convertor.init(col_descs_, vec_formats, batch_size));
    }
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_INFO("finish build_micro_block_desc", K(micro_block_desc));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
}

}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_append_batch.log*");
  OB_LOGGER.set_file_name("test_append_batch.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

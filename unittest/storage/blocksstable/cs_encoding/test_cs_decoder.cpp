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

class TestCSDecoder : public ObCSEncodingTestBase, public ::testing::TestWithParam<std::tuple<bool, bool>>
{
public:
  TestCSDecoder() {}
  virtual ~TestCSDecoder() {}

  virtual void SetUp() {}
  virtual void TearDown() { reuse(); }
};

TEST_P(TestCSDecoder, test_integer_decoder)
{
  const bool has_null = std::get<0>(GetParam());
  const bool is_force_raw = std::get<1>(GetParam());
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 3;
  ObObjType col_types[col_cnt] = {ObIntType, ObUInt64Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  const int64_t row_cnt = 120;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  int64_t row_cnt_without_null[col_cnt] = {row_cnt, row_cnt, row_cnt};

  // <1> integer range is -50 to 49 and has null
  for (int64_t i = 0; i < row_cnt - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i - 50, row_arr[i]));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 20; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    if (has_null) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
      row_cnt_without_null[1]--;
      row_cnt_without_null[2]--;
    } else {
      row_arr[i].storage_datums_[1].set_uint(UINT64_MAX - i);
      row_arr[i].storage_datums_[2].set_uint(INT64_MAX - i);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  LOG_INFO("finish build_micro_block_desc", K(micro_block_desc));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, col_cnt, false));

  // <2>  big integer(inc/dec/dec)
  encoder.reuse();
  encoder.is_all_column_force_raw_ = is_force_raw;
  int64_t row_cnt_without_null_1[col_cnt] = {row_cnt, row_cnt, row_cnt};
  for (int64_t i = 0; i < row_cnt - 20 ; ++i) {
    row_arr[i].storage_datums_[0].set_int(INT64_MIN + i);
    row_arr[i].storage_datums_[1].set_uint(UINT64_MAX - i);
    row_arr[i].storage_datums_[2].set_int(INT64_MAX - i);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 20; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(INT32_MAX + i);
    if (has_null) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
      row_cnt_without_null_1[1]--;
      row_cnt_without_null_1[2]--;
    } else {
      row_arr[i].storage_datums_[1].set_uint(UINT64_MAX - i);
      row_arr[i].storage_datums_[2].set_int(INT64_MIN + 100 - i);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null_1, col_cnt, false));

  // <3> all null integer
  encoder.reuse();
  encoder.is_all_column_force_raw_ = is_force_raw;
  int64_t row_cnt_without_null_2[col_cnt] = {row_cnt, 0, 0};
  for (int64_t i = 0; i < row_cnt ; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    row_arr[i].storage_datums_[1].set_null();
    row_arr[i].storage_datums_[2].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null_2, col_cnt, false));
}

TEST_P(TestCSDecoder, test_string_decoder)
{
  const bool has_null = std::get<0>(GetParam());
  const bool is_force_raw = std::get<1>(GetParam());
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 3;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObVarcharType, ObCharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STRING;

  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  const int64_t row_cnt = 120;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  int64_t row_cnt_without_null[col_cnt] = {row_cnt, row_cnt, row_cnt};

  // <1> var string + fixed string
  char *char_data = static_cast<char *>(allocator_.alloc(1024));
  ASSERT_TRUE(nullptr != char_data);
  MEMSET(char_data, 0xbc, 1024);
  for (int64_t i = 0; i < row_cnt - 20; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_string(char_data, i % 100);
    row_arr[i].storage_datums_[2].set_string(char_data, 100);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 20; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (has_null) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
      row_cnt_without_null[1]--;
      row_cnt_without_null[2]--;
    } else {
      row_arr[i].storage_datums_[1].set_string(char_data, i % 100);
      row_arr[i].storage_datums_[2].set_string(char_data, 100);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, col_cnt, false));

  // <2> all zero length +  all null
  int64_t row_cnt_without_null_1[col_cnt] = {row_cnt, row_cnt, row_cnt};
  encoder.reuse();
  encoder.is_all_column_force_raw_ = is_force_raw;
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (has_null) {
      row_arr[i].storage_datums_[1].set_string(char_data, 0);
      row_arr[i].storage_datums_[2].set_null();
      row_cnt_without_null_1[2]--;
    } else {
      // mix zero_length_string and null in one column
      if (i < row_cnt / 2) {
        row_arr[i].storage_datums_[1].set_string(char_data, 0);
        row_arr[i].storage_datums_[2].set_null();
        row_cnt_without_null_1[2]--;
      } else {
        row_arr[i].storage_datums_[1].set_null();
        row_arr[i].storage_datums_[2].set_string(char_data, 0);
        row_cnt_without_null_1[1]--;
      }
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));

  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, col_cnt, false));
}

TEST_P(TestCSDecoder, test_dict_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 4;
  const bool has_null = std::get<0>(GetParam());
  const bool is_force_raw = std::get<1>(GetParam());
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType, ObVarcharType, ObCharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::STR_DICT; // var string
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::STR_DICT; // fixed length string

  const int64_t row_cnt = 120;
  int64_t row_cnt_without_null[col_cnt] = {row_cnt, row_cnt, row_cnt, row_cnt};
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  char *char_data = static_cast<char *>(allocator_.alloc(1024));
  ASSERT_TRUE(nullptr != char_data);
  MEMSET(char_data, 0xbc, 1024);
  const int64_t distint_cnt = 20;
  for (int64_t i = 0; i < row_cnt - 20; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_int(i % distint_cnt + INT32_MAX);
    row_arr[i].storage_datums_[2].set_string(char_data, i % distint_cnt);
    row_arr[i].storage_datums_[3].set_string(char_data, 100);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 20; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[2].set_string(char_data, i % distint_cnt);
    if (has_null) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[3].set_null();
      row_cnt_without_null[1]--;
      row_cnt_without_null[3]--;
    } else {
      row_arr[i].storage_datums_[1].set_int(i % distint_cnt + INT32_MAX);
      row_arr[i].storage_datums_[3].set_string(char_data, 100);
    }

    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));

  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, col_cnt, false));
}

TEST_F(TestCSDecoder, test_null_dict_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 4;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType, ObVarcharType, ObCharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::STR_DICT;
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::STR_DICT;

  const int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  // <1> all column is null
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_null();
    row_arr[i].storage_datums_[1].set_null();
    row_arr[i].storage_datums_[2].set_null();
    row_arr[i].storage_datums_[3].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  int64_t row_cnt_without_null[col_cnt] = {0, 0, 0, 0};
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));

  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, col_cnt, false));

  // <2> middle column is null
  encoder.reuse();
  char *char_data = static_cast<char *>(allocator_.alloc(1024));
  ASSERT_TRUE(nullptr != char_data);
  MEMSET(char_data, 0xbc, 1024);
  // ObInt32Type, ObIntType, ObVarcharType, ObCharType
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_null();
    row_arr[i].storage_datums_[2].set_null();
    row_arr[i].storage_datums_[3].set_string(char_data, 100);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  int64_t row_cnt_without_null_1[col_cnt] = {row_cnt, 0, 0, row_cnt};
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null_1, col_cnt, false));

  // <3> first and last column is null
  encoder.reuse();
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_null();
    row_arr[i].storage_datums_[1].set_int(i - UINT32_MAX);
    row_arr[i].storage_datums_[2].set_string(char_data, i % 30);
    row_arr[i].storage_datums_[3].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt));

  // <4> last column is null
  encoder.reuse();
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(INT32_MAX - 1000 + i);
    row_arr[i].storage_datums_[1].set_int(i - UINT32_MAX);
    row_arr[i].storage_datums_[2].set_string(char_data, i % 30);
    row_arr[i].storage_datums_[3].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt));
}

TEST_P(TestCSDecoder, test_all_object_type_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = ObExtendType - 1 + 7;
  const bool specify_dict = std::get<0>(GetParam());
  const bool is_force_raw = std::get<1>(GetParam());
  ObObjType col_types[ObObjType::ObMaxType];
  for (int64_t i = 0; i < col_cnt; ++i) {
    ObObjType type = static_cast<ObObjType>(i + 1);  // from ObTinyIntType
    if (col_cnt - 1 == i) {
      type = ObURowIDType;
    } else if (col_cnt - 2 == i) {
      type = ObIntervalYMType;
    } else if (col_cnt - 3 == i) {
      type = ObIntervalDSType;
    } else if (col_cnt - 4 == i) {
      type = ObTimestampTZType;
    } else if (col_cnt - 5 == i) {
      type = ObTimestampLTZType;
    } else if (col_cnt - 6 == i) {
      type = ObTimestampNanoType;
    } else if (col_cnt - 7 == i) {
      type = ObRawType;
    } else if (type == ObExtendType || type == ObUnknownType) {
      type = ObVarcharType;
    }
    col_types[i] = type;
  }
  ObCompressorType compressor_type = ObCompressorType::ZSTD_1_3_8_COMPRESSOR;
  if (is_force_raw) {
    compressor_type = ObCompressorType::NONE_COMPRESSOR;
  }
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt, compressor_type));
  ObMicroBlockCSEncoder encoder;
  for (int64_t i = 0; i < col_cnt; ++i) {
    if (i % 2 && specify_dict) {
      const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(col_types[i])];
      if (ObCSEncodingUtil::is_integer_store_class(store_class)) {
        ctx_.column_encodings_[i] = ObCSColumnHeader::Type::INT_DICT; // sepcfiy dict column encoding
      } else {
        ctx_.column_encodings_[i] = ObCSColumnHeader::Type::STR_DICT; // sepcfiy dict column encoding
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;

  const int64_t row_cnt = 128;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  for (int64_t i = 0; i < row_cnt - 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row_arr[i]));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 2; i < row_cnt; ++i) {
    for (int64_t j = 0; j < col_cnt; j++) {
      if (j < rowkey_cnt) {
        row_arr[i].storage_datums_[j].set_int32(i);
      } else {
        row_arr[i].storage_datums_[j].set_null();
      }
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
}

TEST_P(TestCSDecoder, test_decoder_with_all_stream_encoding_types)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 5;
  ObObjType col_types[col_cnt] = {ObIntType, ObVarcharType, ObVarcharType, ObIntType, ObIntType};
  const bool has_null = std::get<0>(GetParam());
  const bool row_cnt_flag = std::get<1>(GetParam());
  int64_t row_cnt = 0;
  if (row_cnt_flag && has_null) {
    row_cnt = 20;
  } else if (row_cnt_flag && !has_null) {
    row_cnt = 100;
  } else if (!row_cnt_flag && has_null) {
    row_cnt = 200;
  } else {
    row_cnt = 1200;
  }

  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STRING;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::STR_DICT;
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[4] = ObCSColumnHeader::Type::INT_DICT;

  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  void *row_arr_buf = allocator_.alloc(sizeof(ObDatumRow) * row_cnt);
  ASSERT_TRUE(nullptr != row_arr_buf);
  ObDatumRow *row_arr = new(row_arr_buf) ObDatumRow[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  char *varchar_data = static_cast<char *>(allocator_.alloc(row_cnt + 1));
  ASSERT_TRUE(varchar_data != nullptr);
  MEMSET(varchar_data, 0xf, row_cnt + 1);
  const int64_t repeat_cnt = 3;
  int64_t row_cnt_without_null[5] = {row_cnt, row_cnt, row_cnt, row_cnt, row_cnt};
  for (int64_t i = 0; i < row_cnt - 10; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    row_arr[i].storage_datums_[1].set_string(varchar_data, i / repeat_cnt);
    row_arr[i].storage_datums_[2].set_string(varchar_data, i / repeat_cnt);
    row_arr[i].storage_datums_[3].set_int(i / repeat_cnt - 100);
    row_arr[i].storage_datums_[4].set_int(i / repeat_cnt - 100);
  }
  for (int64_t i = row_cnt - 10; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    if (has_null) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
      row_arr[i].storage_datums_[3].set_null();
      row_arr[i].storage_datums_[4].set_null();
      row_cnt_without_null[1]--;
      row_cnt_without_null[2]--;
      row_cnt_without_null[3]--;
      row_cnt_without_null[4]--;
    } else {
      row_arr[i].storage_datums_[1].set_string(varchar_data, i / repeat_cnt);
      row_arr[i].storage_datums_[2].set_string(varchar_data, i / repeat_cnt);
      row_arr[i].storage_datums_[3].set_int(i / repeat_cnt - 100);
      row_arr[i].storage_datums_[4].set_int(i / repeat_cnt - 100);
    }
  }
  ObIntegerStream::EncodingType stream_types_[ObCSColumnHeader::MAX_INT_STREAM_COUNT_OF_COLUMN];
  for (int type = 1; type < ObIntegerStream::EncodingType::MAX_TYPE; type++) {
    stream_types_[0] = {ObIntegerStream::EncodingType(type)};
    stream_types_[1] = {ObIntegerStream::EncodingType(type)};
    LOG_INFO("test stream ecoding type", K(type));
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_column_encoding_types(
      0, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::INTEGER, 1, 0), stream_types_, true));
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_column_encoding_types(
      1, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::STRING, 1, 0), stream_types_, true));
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_column_encoding_types(
      2, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::STR_DICT, 2, 0), stream_types_, true)); // string dict
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_column_encoding_types(
      3, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::INTEGER, 1, 0), stream_types_, true));
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_column_encoding_types(
      4, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::INT_DICT, 2, 0), stream_types_, true)); //integer dict

    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));

    ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header,
        micro_block_desc, row_arr, row_cnt, true));
    ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header,
        micro_block_desc, row_arr, row_cnt, true));
    ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, 5, false));

    encoder.reuse();
  }
}

TEST_F(TestCSDecoder, test_dict_const_ref_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 5;
  ObObjType col_types[col_cnt] = {ObIntType, ObVarcharType, ObUInt64Type, ObVarcharType, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  int64_t row_cnt = 1000;
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::STR_DICT;
  ctx_.column_encodings_[4] = ObCSColumnHeader::Type::INT_DICT;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  void *row_arr_buf = allocator_.alloc(sizeof(ObDatumRow) * row_cnt);
  ASSERT_TRUE(nullptr != row_arr_buf);
  ObDatumRow *row_arr = new(row_arr_buf) ObDatumRow[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  char *varchar_data = static_cast<char *>(allocator_.alloc(row_cnt + 1));
  MEMSET(varchar_data, 'a', row_cnt + 1);
  int64_t execption_cnt = 90;
  for (int64_t i = 0; i < row_cnt - execption_cnt; i++) {
    ASSERT_TRUE(nullptr != varchar_data);
    row_arr[i].storage_datums_[0].set_int(i);
    row_arr[i].storage_datums_[1].set_string(varchar_data, 10);
    row_arr[i].storage_datums_[2].set_uint(UINT64_MAX);
    row_arr[i].storage_datums_[3].set_null();
    row_arr[i].storage_datums_[4].set_int(-9999);
  }
  for (int64_t i = row_cnt - execption_cnt; i < row_cnt; i++) {
    row_arr[i].storage_datums_[0].set_int(i);
    row_arr[i].storage_datums_[1].set_string(varchar_data, 20);
    row_arr[i].storage_datums_[2].set_uint(i);
    row_arr[i].storage_datums_[3].set_string(varchar_data, 100 + i % 2);
    row_arr[i].storage_datums_[4].set_null();
  }
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));

  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));

  int64_t row_cnt_without_null[5] = {row_cnt, row_cnt, row_cnt, row_cnt - execption_cnt, execption_cnt};
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, 5, false));
}

TEST_F(TestCSDecoder, test_decimal_int_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 7;
  ObObjType col_types[col_cnt] = {ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType};
  int64_t precision_arr[col_cnt] = {MAX_PRECISION_DECIMAL_INT_64,
                                    MAX_PRECISION_DECIMAL_INT_32,
                                    MAX_PRECISION_DECIMAL_INT_128,
                                    OB_MAX_DECIMAL_PRECISION,
                                    MAX_PRECISION_DECIMAL_INT_128,
                                    MAX_PRECISION_DECIMAL_INT_128,
                                    OB_MAX_DECIMAL_PRECISION};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt, ObCompressorType::ZSTD_1_3_8_COMPRESSOR, precision_arr));
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[5] = ObCSColumnHeader::Type::STRING;
  ctx_.column_encodings_[6] = ObCSColumnHeader::Type::STR_DICT;
  int64_t row_cnt = 1000;
  const int64_t distinct_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));

  void *row_arr_buf = allocator_.alloc(sizeof(ObDatumRow) * row_cnt);
  ASSERT_TRUE(nullptr != row_arr_buf);
  ObDatumRow *row_arr = new(row_arr_buf) ObDatumRow[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  for (int64_t i = 0; i < row_cnt; i++) {
    int64_t p = 0;
    int32_t m = 0;
    int128_t j = 0;
    int256_t k = 0;
    if (i == row_cnt - 3) {
      p = 0;
      row_arr[i].storage_datums_[0].set_decimal_int(p);
      m = INT32_MAX;
      row_arr[i].storage_datums_[1].set_decimal_int(m);
      j = INT64_MAX;
      row_arr[i].storage_datums_[2].set_decimal_int(j);
      k = (i % distinct_cnt) - distinct_cnt / 2;
      row_arr[i].storage_datums_[3].set_decimal_int(k);

      j = (int128_t)INT64_MAX << 64;
      row_arr[i].storage_datums_[4].set_decimal_int(j);
      row_arr[i].storage_datums_[5].set_decimal_int(j);
      k = (int256_t)((i % distinct_cnt) - distinct_cnt / 2) << 128;
      row_arr[i].storage_datums_[6].set_decimal_int(k);
    } else if (i == row_cnt - 2) {
      p  = INT32_MAX;
      row_arr[i].storage_datums_[0].set_decimal_int(p);
      m = INT32_MIN;
      row_arr[i].storage_datums_[1].set_decimal_int(m);
      j = INT64_MIN;
      row_arr[i].storage_datums_[2].set_decimal_int(j);
      k = (i % distinct_cnt) - distinct_cnt / 2;
      row_arr[i].storage_datums_[3].set_decimal_int(k);

      j = (int128_t)INT64_MIN << 64;
      row_arr[i].storage_datums_[4].set_decimal_int(j);
      row_arr[i].storage_datums_[5].set_decimal_int(j);
      k = (int256_t)((i % distinct_cnt) - distinct_cnt / 2) << 128;
      row_arr[i].storage_datums_[6].set_decimal_int(k);
    } else if (i == row_cnt - 1) {
      p = INT64_MAX;
      row_arr[i].storage_datums_[0].set_decimal_int(p);
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
      row_arr[i].storage_datums_[3].set_null();
      row_arr[i].storage_datums_[4].set_null();
      row_arr[i].storage_datums_[5].set_null();
      row_arr[i].storage_datums_[6].set_null();
    } else {
      p = i - INT64_MAX;
      row_arr[i].storage_datums_[0].set_decimal_int(p);
      m = i - row_cnt / 2;
      row_arr[i].storage_datums_[1].set_decimal_int(m); // -500 ~ 498
      j = i + INT32_MAX;
      row_arr[i].storage_datums_[2].set_decimal_int(j);
      k = (i % distinct_cnt) - distinct_cnt / 2;
      row_arr[i].storage_datums_[3].set_decimal_int(k);

      j = i;
      row_arr[i].storage_datums_[4].set_decimal_int(j);
      row_arr[i].storage_datums_[5].set_null();
      k = (int256_t)((i % distinct_cnt) - distinct_cnt / 2) << 128;
      row_arr[i].storage_datums_[6].set_decimal_int(k);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));

  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header,
      micro_block_desc, row_arr, row_cnt, true));

  int64_t row_cnt_without_null[col_cnt] = {row_cnt, row_cnt - 1, row_cnt - 1, row_cnt - 1, row_cnt - 1, 2, row_cnt - 1};
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null, col_cnt, false));
}

INSTANTIATE_TEST_CASE_P(TestDecoder, TestCSDecoder, Combine(Bool(), Bool()));

}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_cs_decoder.log*");
  OB_LOGGER.set_file_name("test_cs_decoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

#include <algorithm>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "ob_cs_encoding_test_base.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

using ::testing::Bool;
using ::testing::Combine;

class TestCSDecoder : public ObCSEncodingTestBase, public ::testing::TestWithParam<std::tuple<bool, bool, bool>>
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
  const bool has_nop = std::get<2>(GetParam());
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

  // <1> integer range is -50 to 49 and has null or nop
  for (int64_t i = 0; i < row_cnt - 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i - 50, row_arr[i]));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 20; i < row_cnt - 10; ++i) {
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
  for (int64_t i = row_cnt - 10; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    if (has_nop) {
      row_arr[i].storage_datums_[1].set_nop();
      row_arr[i].storage_datums_[2].set_nop();
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
  for (int64_t i = row_cnt - 20; i < row_cnt - 10; ++i) {
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
  for (int64_t i = row_cnt - 10; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(INT32_MAX + i);
    if (has_nop) {
      row_arr[i].storage_datums_[1].set_nop();
      row_arr[i].storage_datums_[2].set_nop();
      row_cnt_without_null_1[1]--;
      row_cnt_without_null_1[2]--;
    } else {
      row_arr[i].storage_datums_[1].set_uint(UINT64_MAX - i);
      row_arr[i].storage_datums_[2].set_uint(INT64_MAX - i);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true));
  ASSERT_EQ(OB_SUCCESS, check_get_row_count(header, micro_block_desc, row_cnt_without_null_1, col_cnt, false));

  // <3> all null / nop / normal integer
  encoder.reuse();
  encoder.is_all_column_force_raw_ = is_force_raw;
  int64_t row_cnt_without_null_2[col_cnt] = {row_cnt, 0, 0};
  for (int64_t i = 0; i < row_cnt ; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    if (has_null) {
      row_arr[i].storage_datums_[1].set_null();
      row_arr[i].storage_datums_[2].set_null();
    } else if (has_nop) {
      row_arr[i].storage_datums_[1].set_nop();
      row_arr[i].storage_datums_[2].set_nop();
    } else {
      row_arr[i].storage_datums_[1].set_int(i);
      row_arr[i].storage_datums_[2].set_int(i);
    }
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
  const bool has_nop = std::get<2>(GetParam());
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
  for (int64_t i = row_cnt - 20; i < row_cnt - 10; ++i) {
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
  for (int64_t i = row_cnt - 10; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (has_nop) {
      row_arr[i].storage_datums_[1].set_nop();
      row_arr[i].storage_datums_[2].set_nop();
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
    } else if (has_nop) {
      row_arr[i].storage_datums_[1].set_string(char_data, 0);
      row_arr[i].storage_datums_[2].set_nop();
      row_cnt_without_null_1[2]--;
    } else {
      // mix zero_length_string and null nop in one column
      if (i < row_cnt / 3) {
        row_arr[i].storage_datums_[1].set_string(char_data, 0);
        row_arr[i].storage_datums_[2].set_null();
        row_cnt_without_null_1[2]--;
      } else if (i < row_cnt * 2 / 3) {
        row_arr[i].storage_datums_[1].set_null();
        row_arr[i].storage_datums_[2].set_string(char_data, 0);
        row_cnt_without_null_1[1]--;
      } else {
        row_arr[i].storage_datums_[1].set_nop();
        row_arr[i].storage_datums_[2].set_nop();
        row_cnt_without_null_1[1]--;
        row_cnt_without_null_1[2]--;
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
  const bool has_nop = std::get<2>(GetParam());
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
  for (int64_t i = row_cnt - 20; i < row_cnt - 10; ++i) {
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
  for (int64_t i = row_cnt - 10; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[2].set_string(char_data, i % distint_cnt);
    if (has_nop) {
      row_arr[i].storage_datums_[1].set_nop();
      row_arr[i].storage_datums_[3].set_nop();
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
  const bool has_nop = std::get<2>(GetParam());
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
  {
    int64_t i = row_cnt - 2;
    for (int64_t j = 0; j < col_cnt; j++) {
      if (j < rowkey_cnt) {
        row_arr[i].storage_datums_[j].set_int32(i);
      } else {
        row_arr[i].storage_datums_[j].set_null();
      }
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  {
    int64_t i = row_cnt - 1;
    for (int64_t j = 0; j < col_cnt; j++) {
      if (j < rowkey_cnt) {
        row_arr[i].storage_datums_[j].set_int32(i);
      } else if (has_nop) {
        row_arr[i].storage_datums_[j].set_nop();
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
  const bool has_nop = std::get<2>(GetParam());
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
  for (int64_t i = row_cnt - 10; i < row_cnt - 5; ++i) {
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
  for (int64_t i = row_cnt - 5; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int(i);
    if (has_nop) {
      row_arr[i].storage_datums_[1].set_nop();
      row_arr[i].storage_datums_[2].set_nop();
      row_arr[i].storage_datums_[3].set_nop();
      row_arr[i].storage_datums_[4].set_nop();
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
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_stream_detect_info(
      0, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::INTEGER, 1, 0), stream_types_, DATA_VERSION_4_3_5_0, true));
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_stream_detect_info(
      1, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::STRING, 1, 0), stream_types_, DATA_VERSION_4_3_5_0, true));
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_stream_detect_info(
      2, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::STR_DICT, 2, 0), stream_types_, DATA_VERSION_4_3_5_0, true)); // string dict
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_stream_detect_info(
      3, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::INTEGER, 1, 0), stream_types_, DATA_VERSION_4_3_5_0, true));
    ASSERT_EQ(OB_SUCCESS, encoder.ctx_.previous_cs_encoding_.update_stream_detect_info(
      4, ObColumnEncodingIdentifier(ObCSColumnHeader::Type::INT_DICT, 2, 0), stream_types_, DATA_VERSION_4_3_5_0, true)); //integer dict

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

// Dedicated vector decode tests for ConvertUintToVec paths:
//   - continuous row_ids from 0 and from non-zero start (copy path)
//   - non-continuous row_ids (gather path)
//   - IS_NULL_REPLACED null handling (mark_nulls with value comparison)
//   - dict / dict-const encoding (process_int_dict_column / process_int_dict_column_const)
//   - gather_with_null_ref (covered by int_dict / int_dict_const with null)
TEST_F(TestCSDecoder, test_vector_decode_integer)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  for (int8_t null_flag = 0; null_flag <= 1; ++null_flag) {
    const int64_t null_cnt = null_flag ? 20 : 0;
    const int64_t row_cnt = 100 + null_cnt;
    ObMicroBlockCSEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
    ObDatumRow row_arr[row_cnt];
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
    }
    for (int64_t i = 0; i < row_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      if (i < 100) {
        row_arr[i].storage_datums_[1].set_int(i * 3 - 50);
      } else {
        row_arr[i].storage_datums_[1].set_null();
      }
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

    const int64_t col_idx = 1;
    ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
    sql::ObExecContext exec_ctx(allocator_);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    ObArenaAllocator frame_alloc;

    // 1) continuous row_ids: all rows
    {
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          row_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[row_cnt];
      for (int32_t i = 0; i < row_cnt; ++i) row_ids[i] = i;
      const char *ptr_arr[row_cnt];
      uint32_t len_arr[row_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < row_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), i, row_arr[i].storage_datums_[col_idx])) << "cont i=" << i;
      }
    }

    // 2) continuous row_ids starting from non-zero: {50,51,...,99}
    //    tests copy(dest, src + row_ids[0], ...) with row_ids[0] != 0
    {
      const int64_t start = 50;
      const int64_t decode_cnt = std::min(static_cast<int64_t>(50), row_cnt - start);
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[decode_cnt];
      for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = start + i;
      const char *ptr_arr[decode_cnt];
      uint32_t len_arr[decode_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < decode_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
            << "cont_nonzero i=" << i << " row_id=" << row_ids[i];
      }
    }

    // 3) non-continuous row_ids: every 3rd row
    {
      const int64_t decode_cnt = (row_cnt + 2) / 3;
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[decode_cnt];
      for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = i * 3;
      const char *ptr_arr[decode_cnt];
      uint32_t len_arr[decode_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < decode_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
            << "non-cont i=" << i << " row_id=" << row_ids[i];
      }
    }

    // 4) non-continuous with vec_offset
    {
      const int64_t decode_cnt = 10;
      const int64_t vec_offset = 5;
      const int64_t total_vec_cnt = decode_cnt + vec_offset;
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          total_vec_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[decode_cnt];
      for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = i * 5;
      const char *ptr_arr[decode_cnt];
      uint32_t len_arr[decode_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, vec_offset, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < decode_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), vec_offset + i, row_arr[row_ids[i]].storage_datums_[col_idx]))
            << "offset i=" << i << " row_id=" << row_ids[i];
      }
    }

    encoder.reuse();
  }
}

// Cover IS_NULL_REPLACED path explicitly with SmallInt value range [0,99]+null.
// null_replaced_value = 100 (max+1). Large row count to exercise mark_nulls 64-bit loop.
TEST_F(TestCSDecoder, test_vector_decode_integer_null_replaced)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObSmallIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  const int64_t data_cnt = 200;
  const int64_t null_cnt = 100;
  const int64_t row_cnt = data_cnt + null_cnt;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (i < data_cnt) {
      row_arr[i].storage_datums_[1].set_int(static_cast<int16_t>(i % 100));
    } else {
      row_arr[i].storage_datums_[1].set_null();
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

  const int64_t col_idx = 1;
  ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  ObArenaAllocator frame_alloc;

  // continuous from 0: all rows
  {
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        row_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[row_cnt];
    for (int32_t i = 0; i < row_cnt; ++i) row_ids[i] = i;
    const char *ptr_arr[row_cnt];
    uint32_t len_arr[row_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[i].storage_datums_[col_idx])) << "null_replaced cont i=" << i;
    }
  }

  // continuous from non-zero start: {150,...,299} includes data+null boundary
  {
    const int64_t start = 150;
    const int64_t decode_cnt = row_cnt - start;
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[decode_cnt];
    for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = start + i;
    const char *ptr_arr[decode_cnt];
    uint32_t len_arr[decode_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < decode_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
          << "null_replaced cont_nonzero i=" << i;
    }
  }

  // non-continuous: every 2nd row, decode_cnt=150 > 64, exercises mark_nulls main loop
  {
    const int64_t decode_cnt = row_cnt / 2;
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[decode_cnt];
    for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = i * 2;
    const char *ptr_arr[decode_cnt];
    uint32_t len_arr[decode_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < decode_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
          << "null_replaced non-cont i=" << i;
    }
  }
}

TEST_F(TestCSDecoder, test_vector_decode_int_dict)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT;

  for (int8_t null_flag = 0; null_flag <= 1; ++null_flag) {
    const int64_t distinct_cnt = 20;
    const int64_t null_cnt = null_flag ? 20 : 0;
    const int64_t row_cnt = 100 + null_cnt;
    ObMicroBlockCSEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
    ObDatumRow row_arr[row_cnt];
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
    }
    for (int64_t i = 0; i < row_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      if (i < 100) {
        row_arr[i].storage_datums_[1].set_int(i % distinct_cnt + INT32_MAX);
      } else {
        row_arr[i].storage_datums_[1].set_null();
      }
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

    const int64_t col_idx = 1;
    ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
    sql::ObExecContext exec_ctx(allocator_);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    ObArenaAllocator frame_alloc;

    // continuous row_ids
    {
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          row_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[row_cnt];
      for (int32_t i = 0; i < row_cnt; ++i) row_ids[i] = i;
      const char *ptr_arr[row_cnt];
      uint32_t len_arr[row_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < row_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), i, row_arr[i].storage_datums_[col_idx])) << "dict cont i=" << i;
      }
    }

    // non-continuous row_ids: every 2nd row
    {
      const int64_t decode_cnt = row_cnt / 2;
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[decode_cnt];
      for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = i * 2;
      const char *ptr_arr[decode_cnt];
      uint32_t len_arr[decode_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < decode_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
            << "dict non-cont i=" << i;
      }
    }

    encoder.reuse();
  }
}

TEST_F(TestCSDecoder, test_vector_decode_int_dict_const)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT;

  for (int8_t null_flag = 0; null_flag <= 1; ++null_flag) {
    // 110 rows const value, 4 exception rows, 6 null rows (if null_flag).
    // exception_cnt=10 < row_cnt*10% keeps const encoding; null rows exercise
    // gather_with_null_ref in process_int_dict_column_const (non-continuous decode).
    const int64_t const_cnt = 110;
    const int64_t exception_cnt = 4;
    const int64_t null_cnt = null_flag ? 6 : 0;
    const int64_t row_cnt = const_cnt + exception_cnt + null_cnt;
    ObMicroBlockCSEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
    ObDatumRow row_arr[row_cnt];
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
    }
    for (int64_t i = 0; i < const_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      row_arr[i].storage_datums_[1].set_int(42);
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
    for (int64_t i = const_cnt; i < const_cnt + exception_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      row_arr[i].storage_datums_[1].set_int((i - const_cnt + 1) * 100);
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
    for (int64_t i = const_cnt + exception_cnt; i < row_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      row_arr[i].storage_datums_[1].set_null();
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

    const int64_t col_idx = 1;
    ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
    sql::ObExecContext exec_ctx(allocator_);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    ObArenaAllocator frame_alloc;

    // continuous
    {
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          row_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[row_cnt];
      for (int32_t i = 0; i < row_cnt; ++i) row_ids[i] = i;
      const char *ptr_arr[row_cnt];
      uint32_t len_arr[row_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < row_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), i, row_arr[i].storage_datums_[col_idx])) << "const cont i=" << i;
      }
    }

    // non-continuous: every 7th row
    {
      const int64_t decode_cnt = (row_cnt + 6) / 7;
      sql::ObExpr col_expr;
      ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
          decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
      int32_t row_ids[decode_cnt];
      for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = std::min(static_cast<int64_t>(i * 7), row_cnt - 1);
      const char *ptr_arr[decode_cnt];
      uint32_t len_arr[decode_cnt];
      ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
      ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
      for (int64_t i = 0; i < decode_cnt; ++i) {
        ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
            *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
            << "const non-cont i=" << i << " row_id=" << row_ids[i];
      }
    }

    encoder.reuse();
  }
}

// Cover HAS_NULL_OR_NOP_BITMAP path: TinyInt filling entire [-128,127] range
// so encoder can't find null_replaced_value, falls back to bitmap.
TEST_F(TestCSDecoder, test_vector_decode_integer_null_bitmap)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObTinyIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  const int64_t data_cnt = 256; // -128..127 fills entire TinyInt range
  const int64_t null_cnt = 20;
  const int64_t row_cnt = data_cnt + null_cnt;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = true;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (i < data_cnt) {
      row_arr[i].storage_datums_[1].set_int(static_cast<int8_t>(i - 128));
    } else {
      row_arr[i].storage_datums_[1].set_null();
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

  const int64_t col_idx = 1;
  ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  ObArenaAllocator frame_alloc;

  // continuous all rows
  {
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        row_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[row_cnt];
    for (int32_t i = 0; i < row_cnt; ++i) row_ids[i] = i;
    const char *ptr_arr[row_cnt];
    uint32_t len_arr[row_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[i].storage_datums_[col_idx])) << "bitmap cont i=" << i;
    }
  }

  // non-continuous: every 3rd row
  {
    const int64_t decode_cnt = (row_cnt + 2) / 3;
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[decode_cnt];
    for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = std::min(i * 3, row_cnt - 1);
    const char *ptr_arr[decode_cnt];
    uint32_t len_arr[decode_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < decode_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
          << "bitmap non-cont i=" << i;
    }
  }
}

// Cover is_decimal_V=true path in copy/gather.
TEST_F(TestCSDecoder, test_vector_decode_decimal_int)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObDecimalIntType, ObDecimalIntType};
  int64_t precision_arr[col_cnt] = {MAX_PRECISION_DECIMAL_INT_64, MAX_PRECISION_DECIMAL_INT_32};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt, ObCompressorType::ZSTD_1_3_8_COMPRESSOR, precision_arr));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  for (int8_t null_flag = 0; null_flag <= 1; ++null_flag) {
    const int64_t null_cnt = null_flag ? 20 : 0;
    const int64_t row_cnt = 100 + null_cnt;
    ObMicroBlockCSEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
    ObDatumRow row_arr[row_cnt];
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
    }
    for (int64_t i = 0; i < row_cnt; ++i) {
      if (i < 100) {
        int64_t v0 = i - 50;
        int32_t v1 = static_cast<int32_t>(i * 7 - 200);
        row_arr[i].storage_datums_[0].set_decimal_int(v0);
        row_arr[i].storage_datums_[1].set_decimal_int(v1);
      } else {
        row_arr[i].storage_datums_[0].set_decimal_int(i - 50);
        row_arr[i].storage_datums_[1].set_null();
      }
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
    ObMicroBlockDesc micro_block_desc;
    ObMicroBlockHeader *header = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
    ObMicroBlockData full_transformed_data;
    ObMicroBlockCSDecoder decoder;
    ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

    sql::ObExecContext exec_ctx(allocator_);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    ObArenaAllocator frame_alloc;

    for (int64_t col_idx = 0; col_idx < col_cnt; ++col_idx) {
      ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
      const int16_t precision = col_meta.get_stored_precision();
      VecValueTypeClass vec_tc = common::get_vec_value_tc(col_meta.get_type(), col_meta.get_scale(), precision);
      if (!VectorDecodeTestUtil::need_test_vec_with_type(VEC_FIXED, vec_tc)) continue;

      // continuous
      {
        sql::ObExpr col_expr;
        ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
            row_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
        int32_t row_ids[row_cnt];
        for (int32_t i = 0; i < row_cnt; ++i) row_ids[i] = i;
        const char *ptr_arr[row_cnt];
        uint32_t len_arr[row_cnt];
        ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, row_cnt, 0, col_expr.get_vector_header(eval_ctx));
        ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
        for (int64_t i = 0; i < row_cnt; ++i) {
          ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
              *vec_ctx.get_vector(), i, row_arr[i].storage_datums_[col_idx]))
              << "decimal cont col=" << col_idx << " i=" << i;
        }
      }

      // non-continuous: every 2nd row
      {
        const int64_t decode_cnt = row_cnt / 2;
        sql::ObExpr col_expr;
        ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
            decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
        int32_t row_ids[decode_cnt];
        for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = i * 2;
        const char *ptr_arr[decode_cnt];
        uint32_t len_arr[decode_cnt];
        ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
        ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
        for (int64_t i = 0; i < decode_cnt; ++i) {
          ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
              *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
              << "decimal non-cont col=" << col_idx << " i=" << i;
        }
      }
    }

    encoder.reuse();
  }
}

// Cover mark_nulls 64-bit aligned main loop (rows_num >= 64 with non-continuous row_ids).
TEST_F(TestCSDecoder, test_vector_decode_large_batch_non_continuous)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  const int64_t row_cnt = 500;
  const int64_t null_start = 400;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (i < null_start) {
      row_arr[i].storage_datums_[1].set_int(i * 5 - 1000);
    } else {
      row_arr[i].storage_datums_[1].set_null();
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

  const int64_t col_idx = 1;
  ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  ObArenaAllocator frame_alloc;

  // non-continuous every 2nd row, decode_cnt=250 > 64, exercises mark_nulls 64-bit loop
  {
    const int64_t decode_cnt = row_cnt / 2;
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[decode_cnt];
    for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = i * 2;
    const char *ptr_arr[decode_cnt];
    uint32_t len_arr[decode_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < decode_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
          << "large non-cont i=" << i;
    }
  }

  // non-continuous gather with vec_offset=3, decode_cnt=200 (no null rows in batch)
  {
    const int64_t decode_cnt = 200;
    const int64_t vec_offset = 3;
    const int64_t total = decode_cnt + vec_offset;
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        total, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[decode_cnt];
    for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = i * 2;
    const char *ptr_arr[decode_cnt];
    uint32_t len_arr[decode_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, vec_offset, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < decode_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), vec_offset + i, row_arr[row_ids[i]].storage_datums_[col_idx]))
          << "large offset i=" << i;
    }
  }
}

// Cover is_all_null=true branch in mark_nulls: decode only null rows.
TEST_F(TestCSDecoder, test_vector_decode_all_null_batch)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  const int64_t row_cnt = 200;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  // first 100 rows have data, last 100 rows are null
  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (i < 100) {
      row_arr[i].storage_datums_[1].set_int(i);
    } else {
      row_arr[i].storage_datums_[1].set_null();
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  ObMicroBlockDesc micro_block_desc;
  ObMicroBlockHeader *header = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header));
  ObMicroBlockData full_transformed_data;
  ObMicroBlockCSDecoder decoder;
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));

  const int64_t col_idx = 1;
  ObObjMeta col_meta = col_descs_.at(col_idx).col_type_;
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  ObArenaAllocator frame_alloc;

  // decode only null rows (row 100..199) — triggers is_all_null=true
  {
    const int64_t decode_cnt = 100;
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[decode_cnt];
    for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = 100 + i;
    const char *ptr_arr[decode_cnt];
    uint32_t len_arr[decode_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < decode_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
          << "all_null i=" << i;
    }
  }

  // non-continuous: only null rows every 2nd (row 100,102,...,198)
  {
    const int64_t decode_cnt = 50;
    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        decode_cnt, col_meta, VEC_FIXED, eval_ctx, col_expr, frame_alloc));
    int32_t row_ids[decode_cnt];
    for (int64_t i = 0; i < decode_cnt; ++i) row_ids[i] = 100 + i * 2;
    const char *ptr_arr[decode_cnt];
    uint32_t len_arr[decode_cnt];
    ObVectorDecodeCtx vec_ctx(ptr_arr, len_arr, row_ids, decode_cnt, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.get_col_data(col_idx, vec_ctx));
    for (int64_t i = 0; i < decode_cnt; ++i) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match_nop(
          *vec_ctx.get_vector(), i, row_arr[row_ids[i]].storage_datums_[col_idx]))
          << "all_null non-cont i=" << i;
    }
  }
}

INSTANTIATE_TEST_CASE_P(TestDecoder, TestCSDecoder, Combine(Bool(), Bool(), Bool()));

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

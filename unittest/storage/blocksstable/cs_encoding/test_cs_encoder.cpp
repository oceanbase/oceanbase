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
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "lib/wide_integer/ob_wide_integer.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class TestCSEncoder : public ObCSEncodingTestBase, public ::testing::Test
{
public:
  TestCSEncoder() {}
  virtual ~TestCSEncoder() {}

  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestCSEncoder, test_integer_encoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 1;
  ObObjType col_types[col_cnt] = {ObInt32Type};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;

  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = true;
  // Generate data and encode
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, col_cnt));

  // <1>integer range is -50 to 49
  int64_t seed = 0;
  for (int64_t i = 0; i < row_cnt; ++i) {
    seed = i - 50;  // -50 ~ 49
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  char *buf = nullptr;
  int64_t buf_size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  ObIColumnCSEncoder *e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(-50, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(49, (int64_t)e->ctx_->integer_max_);

  ASSERT_EQ(sizeof(uint64_t) * row_cnt, encoder.ctx_.estimate_block_size_);
  ASSERT_EQ(buf_size, encoder.ctx_.real_block_size_ + encoder.all_headers_size_);
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));

  ObIntegerColumnEncoder *int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_raw_encoding());
  ASSERT_EQ(1, int_col_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(false, int_col_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_base()); // raw encoding also use base
  ASSERT_EQ(-50, int_col_encoder->enc_ctx_.meta_.base_value_);

  // <2> write 3 row: INT32_MIN/-1/NULL
  // row count less than ObCSEncodingUtil::ENCODING_ROW_COUNT_THRESHOLD,
  // so use will force raw encoding in encoder.
  encoder.reuse();
  seed = INT32_MIN;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));

  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));

  seed = -1;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));

  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(INT32_MIN, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(-1, (int64_t)e->ctx_->integer_max_);
  // estimate_block_size_ won't reset to zero when reuse, so total row cnt is 100 + 3
  ASSERT_EQ(sizeof(uint64_t) * 103, encoder.ctx_.estimate_block_size_);

  int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_raw_encoding());
  ASSERT_EQ(4, int_col_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_base()); // raw encoding also use base
  ASSERT_EQ(INT32_MIN, int_col_encoder->enc_ctx_.meta_.base_value());
  ASSERT_EQ(0, int_col_encoder->enc_ctx_.meta_.null_replaced_value_);

  // <3> write 3 row: INT32_MIN/INT32_MAX/NULL + 200 rows
  encoder.reuse();
  seed = INT32_MIN;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  seed = INT32_MAX;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));

  row_cnt = 200;
  for (int64_t i = 0; i < row_cnt; ++i) {
    seed = i - 100;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed, row));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }

  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(INT32_MIN, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(INT32_MAX, (int64_t)e->ctx_->integer_max_);
  // estimate_block_size_ won't reset to zero when reuse, so total row cnt is 100 + 3 + 3 + 200
  ASSERT_EQ(sizeof(uint64_t) * 306, encoder.ctx_.estimate_block_size_);

  int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(4, int_col_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(false, int_col_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_base());
  ASSERT_EQ(INT32_MIN, int_col_encoder->enc_ctx_.meta_.base_value());
  ASSERT_EQ(true, e->get_column_header().has_null_bitmap());

  // <4> write 3 row: 0/INT32_MAX/NULL
  // row count less than ObCSEncodingUtil::ENCODING_ROW_COUNT_THRESHOLD,
  // so use will force raw encoding in encoder.
  encoder.reuse();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(INT32_MAX, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(0, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(INT32_MAX, (int64_t)e->ctx_->integer_max_);
  int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(-1, int_col_encoder->enc_ctx_.meta_.null_replaced_value_);

  // <5> write 3 row: 0/INT32_MAX - 1/NULL
  // row count less than ObCSEncodingUtil::ENCODING_ROW_COUNT_THRESHOLD,
  // so use will force raw encoding in encoder.
  encoder.reuse();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(INT32_MAX - 1, row));
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(0, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(INT32_MAX - 1, (int64_t)e->ctx_->integer_max_);
  int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(INT32_MAX, int_col_encoder->enc_ctx_.meta_.null_replaced_value());

  // <6> write 1000 row, and monotonic increase
  encoder.reuse();
  row_cnt = 1000;
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(0, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(999, (int64_t)e->ctx_->integer_max_);

  int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(2, int_col_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(false, int_col_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  ASSERT_EQ(false, int_col_encoder->enc_ctx_.meta_.is_use_base());

  //<7> write all null
  encoder.reuse();
  row_cnt = 1000;
  row.storage_datums_[0].set_null();
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(1, int_col_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(0, int_col_encoder->enc_ctx_.meta_.null_replaced_value());
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  ASSERT_EQ(false, int_col_encoder->enc_ctx_.meta_.is_use_base());

  reuse();
}

TEST_F(TestCSEncoder, test_big_integer_encoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObUInt64Type};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));

  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, col_cnt));

  int64_t row_cnt = 200;
  for (int64_t i = 0; i < row_cnt; ++i) {
    row.storage_datums_[0].set_int(INT64_MIN + 1000 + i);
    row.storage_datums_[1].set_uint(UINT64_MAX - 1000 + i );
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  row.storage_datums_[0].set_int(INT64_MIN);
  row.storage_datums_[1].set_uint(UINT64_MAX);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  row.storage_datums_[0].set_int(INT64_MAX);
  row.storage_datums_[1].set_uint(UINT64_MAX - UINT32_MAX);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));

  char *buf = nullptr;
  int64_t buf_size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  ObIColumnCSEncoder *e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(INT64_MIN, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(INT64_MAX, (int64_t)e->ctx_->integer_max_);


  ObIntegerColumnEncoder *int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(8, int_col_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(true, int_col_encoder->enc_ctx_.meta_.is_use_base());
  ASSERT_EQ(INT64_MIN, int_col_encoder->enc_ctx_.meta_.base_value());

  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(UINT64_MAX - UINT32_MAX, e->ctx_->integer_min_);
  ASSERT_EQ(UINT64_MAX, e->ctx_->integer_max_);

  int_col_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(8, int_col_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(false, int_col_encoder->enc_ctx_.meta_.is_use_base());

  reuse();
}


TEST_F(TestCSEncoder, test_string_encoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  ObObjType col_types[col_cnt] = {ObIntType, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STRING;

  // <1> 100 fixed len string and has one null
  int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = true;
  // Generate data and encode
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, col_cnt));

  int64_t seed = 0;
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  for (int64_t i = 0; i < col_cnt; i++) {
    row.storage_datums_[i].set_null();
  }
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));

  char *buf = nullptr;
  int64_t buf_size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  ObIColumnCSEncoder *e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(64, e->ctx_->fix_data_size_);
  ASSERT_EQ(64 * row_cnt, e->ctx_->var_data_size_);
  ASSERT_EQ(64 * row_cnt, e->ctx_->dict_var_data_size_);
  ASSERT_EQ(true, e->get_column_header().has_null_bitmap());

  ASSERT_EQ((8 + 64 + 8) * row_cnt + 16, encoder.ctx_.estimate_block_size_);
  ASSERT_EQ(buf_size, encoder.ctx_.real_block_size_ + encoder.all_headers_size_);
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));

  ObStringColumnEncoder *str_col_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(true, str_col_encoder->enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(64, str_col_encoder->enc_ctx_.meta_.get_fixed_string_len());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());
  ASSERT_EQ(true, str_col_encoder->enc_ctx_.info_.raw_encoding_str_offset_);

  // <2> var length string and has null and has no zero length string
  row_cnt = 100;
  encoder.reuse();
  int64_t sum_len = 0;
  for (int64_t i = 0; i < row_cnt; i++) {
    char *varchar_data = static_cast<char *>(allocator_.alloc(i + 1));
    ASSERT_TRUE(nullptr != varchar_data);
    MEMSET(varchar_data, 0xf, i + 1);
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_string(varchar_data, i + 1);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
    sum_len += i + 1;
  }
  row.storage_datums_[0].set_null();
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));

  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(-1, e->ctx_->fix_data_size_);
  ASSERT_EQ(sum_len, e->ctx_->var_data_size_);
  ASSERT_EQ(sum_len, e->ctx_->dict_var_data_size_);
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));

  str_col_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.meta_.get_fixed_string_len());
  ASSERT_EQ(true, str_col_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.info_.raw_encoding_str_offset_);

  // <3> var length string and has null and has zero length string
  encoder.reuse();
  for (int64_t i = 0; i < ObCSEncodingUtil::ENCODING_ROW_COUNT_THRESHOLD - 2; i++) {
    char *varchar_data = static_cast<char *>(allocator_.alloc(i + 1));
    ASSERT_TRUE(nullptr != varchar_data);
    MEMSET(varchar_data, 0xf, i + 1);
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_string(varchar_data, i);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  row.storage_datums_[0].set_null();
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(-1, e->ctx_->fix_data_size_);
  ASSERT_EQ(true, e->get_column_header().has_null_bitmap());
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));

  str_col_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.info_.raw_encoding_str_offset_);

  // <4> write all null
  row_cnt = 100;
  encoder.reuse();
  for (int64_t i = 0; i < row_cnt; i++) {
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(-1, e->ctx_->fix_data_size_);
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));

  str_col_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(true, str_col_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.info_.raw_encoding_str_offset_);

  // <4> write all zero length datum
  row_cnt = 100;
  encoder.reuse();
  for (int64_t i = 0; i < row_cnt; i++) {
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_string(nullptr, 0);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(0, e->ctx_->fix_data_size_);
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));

  str_col_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(true, str_col_encoder->enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.info_.raw_encoding_str_offset_);

  // <5> write all zero length datum and null
  row_cnt = 200;
  encoder.reuse();
  for (int64_t i = 0; i < row_cnt - 100; i++) {
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_string(nullptr, 0);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  for (int64_t i = row_cnt - 100; i < row_cnt; i++) {
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));
  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(0, e->ctx_->fix_data_size_);
  ASSERT_EQ(true, e->get_column_header().has_null_bitmap());
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));

  str_col_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(true, str_col_encoder->enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());
  ASSERT_EQ(false, str_col_encoder->enc_ctx_.info_.raw_encoding_str_offset_);

  reuse();
}

TEST_F(TestCSEncoder, test_dict_encoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 5;
  ObObjType col_types[col_cnt] = {ObIntType, ObVarcharType, ObUInt64Type, ObTextType, ObHexStringType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT;
  // <1> integer dict: has null and has negative value(-50 - 49)
  //     string dict:  has null and has zero length value
  int64_t row_cnt = 1000;
  const int64_t distinct_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, col_cnt));
  char *varchar_data = static_cast<char *>(allocator_.alloc(row_cnt + 1));
  MEMSET(varchar_data, 0xf, row_cnt + 1);
  for (int64_t i = 0; i < row_cnt; i++) {
    ASSERT_TRUE(nullptr != varchar_data);
    row.storage_datums_[0].set_int((i % distinct_cnt) - distinct_cnt / 2);
    row.storage_datums_[1].set_string(varchar_data, i % distinct_cnt);
    row.storage_datums_[2].set_null();  // all null integer
    row.storage_datums_[3].set_null(); // all null string
    row.storage_datums_[4].set_string(nullptr, 0); // all zero length string
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  row.storage_datums_[0].set_null();
  row.storage_datums_[1].set_null();
  row.storage_datums_[2].set_null();
  row.storage_datums_[3].set_null();
  row.storage_datums_[4].set_string(nullptr, 0);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  char *buf = nullptr;
  int64_t buf_size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));

  ObIColumnCSEncoder *e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INT_DICT);
  ASSERT_EQ(-50, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(49, (int64_t)e->ctx_->integer_max_);
  ASSERT_EQ(true, e->ctx_->need_sort_);
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());

  ObIntDictColumnEncoder *dict_encoder = reinterpret_cast<ObIntDictColumnEncoder *>(e);
  ASSERT_EQ(1, dict_encoder->integer_dict_enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(false, dict_encoder->integer_dict_enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(true, dict_encoder->integer_dict_enc_ctx_.info_.is_monotonic_inc_);
  ASSERT_EQ(distinct_cnt, dict_encoder->max_ref_);
  ASSERT_EQ(distinct_cnt, dict_encoder->dict_encoding_meta_.distinct_val_cnt_);

  ASSERT_EQ(1, dict_encoder->ref_enc_ctx_.meta_.get_uint_width_size()); // [0, distinct_cnt]
  ASSERT_EQ(false, dict_encoder->ref_enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(2, dict_encoder->stream_offsets_.count());

  e = encoder.encoders_[1];
  ASSERT_EQ(true, e->ctx_->need_sort_);
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STR_DICT);
  ASSERT_EQ(-1, e->ctx_->fix_data_size_);
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  LOG_INFO("print ObMicroBlockEncodingCtx", K_(ctx));
  ObStrDictColumnEncoder *str_dict_encoder = reinterpret_cast<ObStrDictColumnEncoder *>(e);
  ASSERT_EQ(false, str_dict_encoder->string_dict_enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(false, str_dict_encoder->string_dict_enc_ctx_.meta_.is_use_zero_len_as_null());
  ASSERT_EQ(false, str_dict_encoder->string_dict_enc_ctx_.info_.raw_encoding_str_offset_);
  ASSERT_EQ(distinct_cnt, str_dict_encoder->max_ref_);
  ASSERT_EQ(distinct_cnt, str_dict_encoder->dict_encoding_meta_.distinct_val_cnt_);

  ASSERT_EQ(1, str_dict_encoder->ref_enc_ctx_.meta_.get_uint_width_size()); // [0, distinct_cnt]
  ASSERT_EQ(false, str_dict_encoder->ref_enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(3, str_dict_encoder->stream_offsets_.count());

  e = encoder.encoders_[2];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INT_DICT);
  ASSERT_EQ(true, e->ctx_->need_sort_);
  dict_encoder = reinterpret_cast<ObIntDictColumnEncoder *>(e);
  ASSERT_EQ(0, dict_encoder->dict_encoding_meta_.distinct_val_cnt_);
  ASSERT_EQ(0, dict_encoder->stream_offsets_.count());

  e = encoder.encoders_[3]; // ObTextType
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STR_DICT);
  ASSERT_EQ(true, e->ctx_->need_sort_);
  str_dict_encoder = reinterpret_cast<ObStrDictColumnEncoder *>(e);
  ASSERT_EQ(0, dict_encoder->dict_encoding_meta_.distinct_val_cnt_);
  ASSERT_EQ(0, dict_encoder->stream_offsets_.count());

  e = encoder.encoders_[4]; // all zero length string will choose string encoding
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(0, e->ctx_->fix_data_size_);
  ObStringColumnEncoder *str_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(1, str_encoder->stream_offsets_.count());
  reuse();
}

TEST_F(TestCSEncoder, test_dict_const_ref_encoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 5;
  ObObjType col_types[col_cnt] = {ObIntType, ObVarcharType, ObUInt64Type, ObVarcharType, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  int64_t row_cnt = 1000;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, col_cnt));
  char *varchar_data = static_cast<char *>(allocator_.alloc(row_cnt + 1));
  MEMSET(varchar_data, 'a', row_cnt + 1);
  int64_t execption_cnt = 64;
  for (int64_t i = 0; i < row_cnt - execption_cnt; i++) {
    ASSERT_TRUE(nullptr != varchar_data);
    row.storage_datums_[0].set_int(1000);
    row.storage_datums_[1].set_string(varchar_data, 10);
    row.storage_datums_[2].set_int(i % 2 + 1000); // not const dict ref
    row.storage_datums_[3].set_null();
    row.storage_datums_[4].set_int(1000);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  for (int64_t i = 0; i < execption_cnt; i++) {
    row.storage_datums_[0].set_int(i);
    row.storage_datums_[1].set_string(varchar_data, 20);
    row.storage_datums_[2].set_null();
    row.storage_datums_[3].set_string(varchar_data, 100 + i % 2);
    row.storage_datums_[4].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  char *buf = nullptr;
  int64_t buf_size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));

  ObIColumnCSEncoder *e = encoder.encoders_[0];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INT_DICT);
  ObIntDictColumnEncoder *dict_encoder = reinterpret_cast<ObIntDictColumnEncoder *>(e);
  ASSERT_EQ(true, dict_encoder->dict_encoding_meta_.is_const_encoding_ref());
  ASSERT_EQ(execption_cnt, dict_encoder->const_list_header_->dict_ref_); // const value is the last one in the sorted dict
  ASSERT_EQ(execption_cnt, dict_encoder->ref_exception_cnt_);
  ASSERT_EQ(2 + 2 * execption_cnt, dict_encoder->dict_encoding_meta_.ref_row_cnt_);

  e = encoder.encoders_[1];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STR_DICT);
  ObStrDictColumnEncoder *str_dict_encoder = reinterpret_cast<ObStrDictColumnEncoder *>(e);
  ASSERT_EQ(1, str_dict_encoder->max_ref_);
  ASSERT_EQ(true, str_dict_encoder->dict_encoding_meta_.is_const_encoding_ref());
  ASSERT_EQ(0, str_dict_encoder->const_list_header_->dict_ref_);
  ASSERT_EQ(execption_cnt, str_dict_encoder->ref_exception_cnt_);
  ASSERT_EQ(2 + 2 * execption_cnt, str_dict_encoder->dict_encoding_meta_.ref_row_cnt_);

  e = encoder.encoders_[2];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INT_DICT);
  ASSERT_EQ(true, e->ctx_->need_sort_);
  dict_encoder = reinterpret_cast<ObIntDictColumnEncoder *>(e);
  ASSERT_EQ(false, dict_encoder->dict_encoding_meta_.is_const_encoding_ref());

  e = encoder.encoders_[3];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STR_DICT);
  str_dict_encoder = reinterpret_cast<ObStrDictColumnEncoder *>(e);
  ASSERT_EQ(true, str_dict_encoder->dict_encoding_meta_.is_const_encoding_ref());
  ASSERT_EQ(2, str_dict_encoder->max_ref_);
  ASSERT_EQ(2, str_dict_encoder->const_list_header_->dict_ref_);
  ASSERT_EQ(execption_cnt, str_dict_encoder->ref_exception_cnt_);
  ASSERT_EQ(2 + 2 * execption_cnt, str_dict_encoder->dict_encoding_meta_.ref_row_cnt_);

  e = encoder.encoders_[4];
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INT_DICT);
  dict_encoder = reinterpret_cast<ObIntDictColumnEncoder *>(e);
  ASSERT_EQ(true, dict_encoder->dict_encoding_meta_.is_const_encoding_ref());
  ASSERT_EQ(1, dict_encoder->max_ref_);
  ASSERT_EQ(0, dict_encoder->const_list_header_->dict_ref_);
  ASSERT_EQ(execption_cnt, dict_encoder->ref_exception_cnt_);
  ASSERT_EQ(2 + 2 * execption_cnt, dict_encoder->dict_encoding_meta_.ref_row_cnt_);

  reuse();
}

TEST_F(TestCSEncoder, test_decimal_int_encoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 6;
  ObObjType col_types[col_cnt] = {ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType};
  int64_t precision_arr[col_cnt] = {MAX_PRECISION_DECIMAL_INT_32,  // INTEGER
                                    MAX_PRECISION_DECIMAL_INT_128, // INTEGER
                                    OB_MAX_DECIMAL_PRECISION,      // INT_DICT
                                    MAX_PRECISION_DECIMAL_INT_128, // STRING
                                    MAX_PRECISION_DECIMAL_INT_128, // STRING
                                    OB_MAX_DECIMAL_PRECISION};     // STR_DICT
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt, ObCompressorType::ZSTD_1_3_8_COMPRESSOR, precision_arr));
  ctx_.column_encodings_[4] = ObCSColumnHeader::Type::STRING; // specified, otherwise it will hit STR_DICT
  int64_t row_cnt = 1000;
  const int64_t distinct_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, col_cnt));

  for (int64_t i = 0; i < row_cnt; i++) {
    int32_t m = 0;
    int128_t j = 0;
    int256_t k = 0;
    if (i == row_cnt - 3) {
      m = INT32_MAX;
      row.storage_datums_[0].set_decimal_int(m);
      j = INT64_MAX;
      row.storage_datums_[1].set_decimal_int(j);
      k = 0;
      row.storage_datums_[2].set_decimal_int(k);

      j = (int128_t)INT64_MAX << 64;
      row.storage_datums_[3].set_decimal_int(j);
      row.storage_datums_[4].set_decimal_int(j);
      k = (int256_t)((i % distinct_cnt) - distinct_cnt / 2) << 128;
      row.storage_datums_[5].set_decimal_int(k);
    } else if (i == row_cnt - 2) {
      m = INT32_MIN;
      row.storage_datums_[0].set_decimal_int(m);
      j = INT64_MIN;
      row.storage_datums_[1].set_decimal_int(j);
      k = INT32_MAX;
      row.storage_datums_[2].set_decimal_int(k);

      j = (int128_t)INT64_MIN << 64;
      row.storage_datums_[3].set_decimal_int(j);
      row.storage_datums_[4].set_decimal_int(j);
      k = (int256_t)((i % distinct_cnt) - distinct_cnt / 2) << 128;
      row.storage_datums_[5].set_decimal_int(k);
    } else if (i == row_cnt - 1) {
      row.storage_datums_[0].set_null();
      row.storage_datums_[1].set_null();
      row.storage_datums_[2].set_null();
      row.storage_datums_[3].set_null();
      row.storage_datums_[4].set_null();
      row.storage_datums_[5].set_null();
    } else {
      m = i - row_cnt / 2;
      row.storage_datums_[0].set_decimal_int(m); // -500 ~ 498
      j = i + INT32_MAX;
      row.storage_datums_[1].set_decimal_int(j);
      k = (i % (distinct_cnt - 2)) - INT32_MAX;
      row.storage_datums_[2].set_decimal_int(k);

      j = i;
      row.storage_datums_[3].set_decimal_int(j);
      row.storage_datums_[4].set_null();
      k = (int256_t)((i % distinct_cnt) - distinct_cnt / 2) << 128;
      row.storage_datums_[5].set_decimal_int(k);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row));
  }
  char *buf = nullptr;
  int64_t buf_size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder.build_block(buf, buf_size));

  ObIColumnCSEncoder *e = encoder.encoders_[0]; // int32_t
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(INT32_MIN, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(INT32_MAX, (int64_t)e->ctx_->integer_max_);
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  ObIntegerColumnEncoder *int_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(8, int_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(true, int_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(true, int_encoder->enc_ctx_.meta_.is_use_base());
  ASSERT_EQ((int64_t)(INT32_MIN) - 1, (int64_t)(int_encoder->enc_ctx_.meta_.null_replaced_value()));

  e = encoder.encoders_[1]; // int128_t
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INTEGER);
  ASSERT_EQ(INT64_MIN, (int64_t)e->ctx_->integer_min_);
  ASSERT_EQ(INT64_MAX, (int64_t)e->ctx_->integer_max_);
  ASSERT_EQ(true, e->get_column_header().has_null_bitmap());
  int_encoder = reinterpret_cast<ObIntegerColumnEncoder *>(e);
  ASSERT_EQ(8, int_encoder->enc_ctx_.meta_.get_uint_width_size());
  ASSERT_EQ(false, int_encoder->enc_ctx_.meta_.is_use_null_replace_value());
  ASSERT_EQ(true, int_encoder->enc_ctx_.meta_.is_use_base());
  ASSERT_EQ(INT64_MIN, int_encoder->enc_ctx_.meta_.base_value());

  e = encoder.encoders_[2]; // int256_t
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::INT_DICT);
  ASSERT_EQ(true, e->ctx_->need_sort_);
  ObIntDictColumnEncoder *dict_encoder = reinterpret_cast<ObIntDictColumnEncoder *>(e);
  ASSERT_EQ(true, dict_encoder->integer_dict_enc_ctx_.info_.is_monotonic_inc_);
  ASSERT_EQ(distinct_cnt, dict_encoder->max_ref_);
  ASSERT_EQ(distinct_cnt, dict_encoder->dict_encoding_meta_.distinct_val_cnt_);
  ASSERT_EQ(2, dict_encoder->stream_offsets_.count());

  e = encoder.encoders_[3]; // int128_t, use fix length has less storage cost
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(sizeof(int128_t), e->ctx_->fix_data_size_);
  ASSERT_EQ(true, e->get_column_header().has_null_bitmap());
  ObStringColumnEncoder *str_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(true, str_encoder->enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(false, str_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());

  e = encoder.encoders_[4]; // int128_t, use var length has less storage cost
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STRING);
  ASSERT_EQ(sizeof(int128_t), e->ctx_->fix_data_size_);
  ASSERT_EQ(false, e->get_column_header().has_null_bitmap());
  str_encoder = reinterpret_cast<ObStringColumnEncoder *>(e);
  ASSERT_EQ(false, str_encoder->enc_ctx_.meta_.is_fixed_len_string());
  ASSERT_EQ(true, str_encoder->enc_ctx_.meta_.is_use_zero_len_as_null());

  e = encoder.encoders_[5]; // int256_t
  ASSERT_EQ(e->get_type(), ObCSColumnHeader::Type::STR_DICT);
  ASSERT_EQ(sizeof(int256_t), e->ctx_->fix_data_size_);
  ObStrDictColumnEncoder *str_dict_encoder = reinterpret_cast<ObStrDictColumnEncoder *>(e);
  ASSERT_EQ(distinct_cnt, str_dict_encoder->max_ref_);
  ASSERT_EQ(distinct_cnt, str_dict_encoder->dict_encoding_meta_.distinct_val_cnt_);
  ASSERT_EQ(2, str_dict_encoder->stream_offsets_.count());
  ASSERT_EQ(1, str_dict_encoder->ref_enc_ctx_.meta_.get_uint_width_size()); // [0, distinct_cnt]

  reuse();
}


}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_cs_encoder.log*");
  OB_LOGGER.set_file_name("test_cs_encoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

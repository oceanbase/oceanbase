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
 #include "ob_pd_filter_test_base.h"

namespace oceanbase
{
namespace blocksstable
{
class TestIntegerPdFilter : public ObPdFilterTestBase
{

};

TEST_F(TestIntegerPdFilter, test_integer_decoder_filter)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  for (int8_t flag = 0; flag <= 1; ++flag) {
    bool has_null = flag;
    const int64_t null_cnt = has_null ? 20 : 0;
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
        row_arr[i].storage_datums_[1].set_int(i - 50);
      } else {
        row_arr[i].storage_datums_[1].set_null();
      }
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }

    HANDLE_TRANSFORM();

    const int64_t col_offset = 1;
    bool need_check = true;

    // check NU/NN
    {
      int64_t ref_arr[1];
      const int64_t nu_cnt = has_null ? null_cnt : 0;
      int64_t res_arr_nu[1] = {nu_cnt};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);

      int64_t res_arr_nn[1] = {100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
    }

    // check EQ/NE
    {
      int64_t ref_arr[4] = {-55, -50, 40, 55};
      int64_t res_arr[4] = {0, 1, 1, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr);
    }
    {
      int64_t ref_arr[4] = {-55, -50, 40, 55};
      int64_t res_arr[4] = {100, 99, 99, 100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr);
    }

    // check LE/LT/GE/GT
    {
      int64_t ref_arr[4] = {-55, -50, -40, 55};
      int64_t res_arr_le[4] = {0, 1, 11, 100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
      int64_t res_arr_lt[4] = {0, 0, 10, 100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    }
    {
      int64_t ref_arr[4] = {-55, 40, 49, 55};
      int64_t res_arr_ge[4] = {100, 10, 1, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
      int64_t res_arr_gt[4] = {100, 9, 0, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
    }

    // check IN/BT
    {
      int64_t ref_arr[5] = {-55, -27, 0, 10, 100};
      int64_t res_arr[1] = {3};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
    }
    {
      int64_t ref_arr[10] = {-100, -90, -55, -47, -4, 4, 47, 55, 90, 100};
      int64_t res_arr[5] = {0, 4, 9, 3, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
    }
    LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
  }
}

TEST_F(TestIntegerPdFilter, test_integer_decoder_uint_type)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool has_null = true;
  const bool is_force_raw = true;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObUSmallIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  for (int8_t flag = 0; flag <= 1; ++flag) {
    bool has_null = flag;
    const int64_t null_cnt = has_null ? 20 : 0;
    const int64_t row_cnt = 100 + null_cnt;

    ObMicroBlockCSEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
    encoder.is_all_column_force_raw_ = is_force_raw;
    ObDatumRow row_arr[row_cnt];
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
    }

    for (int64_t i = 0; i < row_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      if (i < 100) {
        row_arr[i].storage_datums_[1].set_uint(100 + i);
      } else {
        row_arr[i].storage_datums_[1].set_null();
      }
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }

    HANDLE_TRANSFORM();

    const int64_t col_offset = 1;
    bool need_check = true;

    // check NU/NN
    {
      int64_t ref_arr[1];
      const int64_t nu_cnt = has_null ? null_cnt : 0;
      int64_t res_arr_nu[1] = {nu_cnt};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
      int64_t res_arr_nn[1] = {100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
    }

    // check EQ/NE
    {
      uint64_t ref_arr[4] = {100, 199, 219, UINT32_MAX};
      int64_t res_arr_eq[4] = {1, 1, 0, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
      int64_t res_arr_ne[4] = {99, 99, 100, 100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
    }

    // check LT/LE/GT/GE
    {
      int64_t ref_arr[4] = {100, 199, 219, UINT32_MAX};
      int64_t res_arr_gt[4] = {99, 0, 0, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
      int64_t res_arr_ge[4] = {100, 1, 0, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
    }
    {
      int64_t ref_arr[4] = {100, 199, 219, UINT32_MAX};
      int64_t res_arr_lt[4] = {0, 99, 100, 100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
      int64_t res_arr_le[4] = {1, 100, 100, 100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
    }
  }
}

TEST_F(TestIntegerPdFilter, test_integer_decoder_nullbitmap_type)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool has_null = true;
  const bool is_force_raw = true;
  abnormal_filter_type_ = AbnormalFilterType::WIDER_WIDTH;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObTinyIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  const int64_t row_cnt = UINT8_MAX + 2;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (i < row_cnt - 1) {
      row_arr[i].storage_datums_[1].set_int(i - 128);
    } else {
      row_arr[i].storage_datums_[1].set_null();
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {1};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {row_cnt - 1};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    int64_t ref_arr[6] = {INT32_MIN, -128, -1, 1, 127, INT32_MAX};
    int64_t res_arr_eq[6] = {0, 1, 1, 1, 1, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 6, 1, res_arr_eq);
    int64_t res_arr_ne[6] = {row_cnt-1, row_cnt-2, row_cnt-2, row_cnt-2, row_cnt-2, row_cnt-1};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 6, 1, res_arr_ne);
  }

  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

TEST_F(TestIntegerPdFilter, test_integer_decoder_float_type)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = false;
  const bool is_force_raw = true;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObFloatType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  for (int8_t flag = 0; flag <= 1; ++flag) {
    bool has_null = flag;
    const int64_t null_cnt = has_null ? 20 : 0;
    const int64_t row_cnt = 100 + null_cnt;
    ObMicroBlockCSEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
    encoder.is_all_column_force_raw_ = is_force_raw;

    ObDatumRow row_arr[row_cnt];
    for (int64_t i = 0; i < row_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
    }

    for (int64_t i = 0; i < row_cnt; ++i) {
      if (i < 100) {
        ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i-49, row_arr[i]));
      } else {
        row_arr[i].storage_datums_[0].set_int32(i);
        row_arr[i].storage_datums_[1].set_null();
      }
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }

    HANDLE_TRANSFORM();

    const int64_t col_offset = 1;
    bool need_check = true;

    // check NU/NN
    {
      int64_t ref_seed_arr[1];
      const int64_t nu_cnt = has_null ? null_cnt : 0;
      int64_t res_arr_nu[1] = {nu_cnt};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
      int64_t res_arr_nn[1] = {100};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
    }

    // check EQ/NE
    {
      int64_t ref_seed_arr[5] = {-100, -49, 1, 50, 100};
      int64_t res_arr_eq[5] = {0, 1, 1, 1, 0};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
      int64_t res_arr_ne[5] = {100, 99, 99, 99, 100};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
    }

    // check LT/LE/GT/GE
    {
      int64_t ref_seed_arr[5] = {-100, -49, 1, 50, 100};
      int64_t res_arr_gt[5] = {100, 99, 49, 0, 0};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
      int64_t res_arr_ge[5] = {100, 100, 50, 1, 0};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
    }
    {
      int64_t ref_seed_arr[5] = {-100, -49, 1, 50, 100};
      int64_t res_arr_lt[5] = {0, 0, 50, 99, 100};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
      int64_t res_arr_le[5] = {0, 1, 51, 100, 100};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
    }

    // check IN/BT
    {
      int64_t ref_seed_arr[5] = {-100, -49, 1, 50, 100};
      int64_t res_arr[1] = {3};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
    }
    {
      int64_t ref_seed_arr[10] = {-100, -50, -49, -10, -1, 10, 20, 70, 100, 200};
      int64_t res_arr[5] = {0, 40, 12, 31, 0};
      raw_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
    }
    LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");

    encoder.reuse();
  }
}

TEST_F(TestIntegerPdFilter, test_integer_abnormal_filter)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  abnormal_filter_type_ = AbnormalFilterType::WIDER_WIDTH;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObSmallIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;

  const int64_t row_cnt = 1000;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (i >= 900) {
      row_arr[i].storage_datums_[1].set_null();
    } else {
      row_arr[i].storage_datums_[1].set_int(i - 500);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {100};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {900};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    int64_t ref_arr[4] = {INT64_MIN, -1, 1, INT64_MAX};
    int64_t res_arr_eq[4] = {0, 1, 1, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
    int64_t res_arr_ne[4] = {900, 899, 899, 900};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int64_t ref_arr[4] = {INT64_MIN, -100, 100, INT64_MAX};
    int64_t res_arr_lt[4] = {0, 400, 600, 900};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    int64_t res_arr_le[4] = {0, 401, 601, 900};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }
  {
    int64_t ref_arr[4] = {INT64_MIN, -100, 100, INT64_MAX};
    int64_t res_arr_gt[4] = {900, 499, 299, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
    int64_t res_arr_ge[4] = {900, 500, 300, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int64_t ref_arr[5] = {INT64_MIN, -500, -1, 100, INT64_MAX};
    int64_t res_arr[1] = {3};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    int64_t ref_arr[10] = {INT64_MIN, -501, -500, -100, -1, 11, 10, 100, 101, INT64_MAX};
    int64_t res_arr[5] = {0, 401, 13, 91, 299};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }

  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

TEST_F(TestIntegerPdFilter, test_all_null_integer_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool is_force_raw = false;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER; // integer
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER; // integer

  const int64_t row_cnt = 120;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {120};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ
  {
    int64_t ref_arr[2] = {-100, 30};
    int64_t res_arr_eq[2] = {0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 2, 1, res_arr_eq);
  }

  // check LT/LE/GT/GE
  {
    int64_t ref_arr[3] = {0, INT32_MIN, INT64_MAX};
    int64_t res_arr_lt[3] = {0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 3, 1, res_arr_lt);
    int64_t res_arr_le[4] = {0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }
  {
    int64_t ref_arr[3] = {0, INT32_MIN, INT32_MIN};
    int64_t res_arr_gt[3] = {0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 3, 1, res_arr_gt);
    int64_t res_arr_ge[3] = {0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 3, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int64_t ref_arr[4] = {0, 1, 2 ,3};
    int64_t res_arr[1] = {0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 4, res_arr);
  }
  {
    int64_t ref_arr[2] = {INT32_MIN, INT32_MAX};
    int64_t res_arr[1] = {0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 1, 2, res_arr);
  }

  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

//
TEST_F(TestIntegerPdFilter, test_exceed_range_compare_filter)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER; // integer
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER; // integer

  const int64_t row_cnt = 2;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  row_arr[0].storage_datums_[0].set_int32(0);
  row_arr[0].storage_datums_[1].set_int(-10000000);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[0]));
  row_arr[1].storage_datums_[0].set_int32(1);
  row_arr[1].storage_datums_[1].set_null();
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[1]));

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check EQ NE
  {
    int64_t ref_arr[4] = {-10000001, -10000000, -1, 10000000};
    int64_t res_arr_eq[4] = {0, 1, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
    int64_t res_arr_ne[4] = {1, 0, 1, 1};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int64_t ref_arr[4] = {-10000001, -10000000, -1, 10000000};
    int64_t res_arr_lt[4] = {0, 0, 1, 1};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    int64_t res_arr_le[4] = {0, 1, 1, 1};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }
  {
    int64_t ref_arr[4] = {-10000001, -10000000, -1, 10000000};
    int64_t res_arr_gt[4] = {1, 0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
    int64_t res_arr_ge[4] = {1, 1, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
  }
  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

//
TEST_F(TestIntegerPdFilter, test_singed_and_unsigned_compare_filter)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 3;
  const bool enable_check = ENABLE_CASE_CHECK;
  abnormal_filter_type_ = AbnormalFilterType::OPPOSITE_SIGN;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObUSmallIntType, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER; // integer
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER; // integer
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::INTEGER; // integer

  const int64_t row_cnt = 2;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  row_arr[0].storage_datums_[0].set_int32(0);
  row_arr[0].storage_datums_[1].set_uint(0);
  row_arr[0].storage_datums_[2].set_int(-1);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[0]));
  row_arr[1].storage_datums_[0].set_int32(1);
  row_arr[1].storage_datums_[1].set_uint(1);
  row_arr[1].storage_datums_[2].set_int(0);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[1]));

  HANDLE_TRANSFORM();

  int64_t col_offset = 1;
  bool need_check = true;

  // check EQ NE
  {
    int64_t ref_arr[4] = {0, 1, 100, 1000};
    int64_t res_arr_eq[4] = {1, 1, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
    int64_t res_arr_ne[4] = {1, 1, 2, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int64_t ref_arr[4] = {-1, 0, 1, 100};
    int64_t res_arr_lt[4] = {0, 0, 1, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    int64_t res_arr_le[4] = {0, 1, 2, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }
  {
    int64_t ref_arr[4] = {-1, 0, 1, 100};
    int64_t res_arr_gt[4] = {2, 1, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
    int64_t res_arr_ge[4] = {2, 2, 1, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
  }
  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");

  col_offset = 2;
  // check LT/LE/GT/GE
  {
    int64_t ref_arr[4] = {0, 1, 10, 100};
    int64_t res_arr_lt[4] = {1, 2, 2, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    int64_t res_arr_le[4] = {2, 2, 2, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }
  {
    int64_t ref_arr[4] = {0, 1, 10, 100};
    int64_t res_arr_gt[4] = {0, 0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
    int64_t res_arr_ge[4] = {1, 0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
  }

  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_integer_pd_filter.log*");
  OB_LOGGER.set_file_name("test_integer_pd_filter.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

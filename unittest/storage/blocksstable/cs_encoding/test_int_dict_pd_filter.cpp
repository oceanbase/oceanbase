
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

class TestIntDictPdFilter : public ObPdFilterTestBase
{

};

TEST_F(TestIntDictPdFilter, test_int_dict_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool has_null = true;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT; // integer dict

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

    const int64_t distinct_cnt = 20;
    for (int64_t i = 0; i < row_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      if (i < 100) {
        row_arr[i].storage_datums_[1].set_int(i % distinct_cnt + INT32_MAX);
      } else {
        row_arr[i].storage_datums_[1].set_null();
      }
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }

    HANDLE_TRANSFORM();

    const int64_t col_offset = 1;
    const int64_t TMP_PARAM = 10000;
    const int64_t round_cnt = 100 / distinct_cnt;
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
      int64_t ref_arr[4] = {-100, 2 % distinct_cnt + INT32_MAX,
        19 % distinct_cnt + INT32_MAX, 500 % TMP_PARAM + INT32_MAX};
      int64_t res_arr_eq[4] = {0, round_cnt, round_cnt, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
      int64_t res_arr_ne[4] = {100, 100 - round_cnt, 100 - round_cnt, 100};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
    }

    // check LT/LE/GT/GE
    {
      int64_t ref_arr[4] = {-100, 1 % distinct_cnt + INT32_MAX,
        6 % distinct_cnt + INT32_MAX, 500 % TMP_PARAM + INT32_MAX};
      int64_t res_arr_lt[4] = {0, round_cnt, 6 * round_cnt, 20 * round_cnt};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
      int64_t res_arr_le[4] = {0, 2 * round_cnt, 7 * round_cnt, 20 * round_cnt};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
    }
    {
      int64_t ref_arr[4] = {-100, 17 % distinct_cnt + INT32_MAX,
        19 % distinct_cnt + INT32_MAX, 500 % TMP_PARAM + INT32_MAX};
      int64_t res_arr_gt[4] = {20 * round_cnt, 2 * round_cnt, 0, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
      int64_t res_arr_ge[4] = {20 * round_cnt, 3 * round_cnt, round_cnt, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
    }

    // check IN/BT
    {
      int64_t ref_arr[4] = {-100, 1 % distinct_cnt + INT32_MAX,
        6 % distinct_cnt + INT32_MAX, 500 % TMP_PARAM + INT32_MAX};
      int64_t res_arr[1] = {round_cnt * 2};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 4, res_arr);
    }
    {
      int64_t ref_arr[10] = {-100, -90, -55, 2 % distinct_cnt + INT32_MAX, 1 % distinct_cnt + INT32_MAX,
        4 % distinct_cnt + INT32_MAX, 18 % distinct_cnt + INT32_MAX, 500 % TMP_PARAM + INT32_MAX,
        1000 % TMP_PARAM + INT32_MAX, 2000 % TMP_PARAM + INT32_MAX};
      int64_t res_arr[5] = {0, 3 * round_cnt, 4 * round_cnt, 2 * round_cnt, 0};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
    }

    LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");

    encoder.reuse();
  }
}

TEST_F(TestIntDictPdFilter, test_positive_int_dict_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool has_null = true;
  const bool is_force_raw = true;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT; // integer dict

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
        row_arr[i].storage_datums_[1].set_int(100 + i);
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

    // check LT/LE/GT/GE
    {
      int64_t ref_arr[3] = {-1, 50, 150};
      int64_t res_arr_gt[3] = {100, 100, 49};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 3, 1, res_arr_gt);
      int64_t res_arr_ge[3] = {100, 100, 50};
      integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 3, 1, res_arr_ge);
    }
  }
}

TEST_F(TestIntDictPdFilter, test_int_dict_const_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT; // integer dict

  const int64_t row_cnt = 120;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  for (int64_t i = 0; i < row_cnt - 5; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_int(30);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 5; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (i == row_cnt - 1) {
      row_arr[i].storage_datums_[1].set_null();
    } else {
      row_arr[i].storage_datums_[1].set_int((i - row_cnt + 6) * 100);
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
    int64_t res_arr_nn[1] = {119};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    int64_t ref_arr[4] = {-100, 30, 100, 101};
    int64_t res_arr_eq[4] = {0, 115, 1, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
    int64_t res_arr_ne[4] = {119, 4, 118, 119};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int64_t ref_arr[4] = {-100, 30, 31, 300};
    int64_t res_arr_lt[4] = {0, 0, 115, 117};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    int64_t res_arr_le[4] = {0, 115, 115, 118};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }
  {
    int64_t ref_arr[4] = {30, 100, 400, 500};
    int64_t res_arr_gt[4] = {4, 3, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
    int64_t res_arr_ge[4] = {119, 4, 1, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int64_t ref_arr[4] = {-100, 30, 105, 300};
    int64_t res_arr[1] = {116};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 4, res_arr);
  }
  {
    int64_t ref_arr[10] = {-100, -50, -10, 40, -1, 100, 31, 105, 50, 500};
    int64_t res_arr[5] = {0, 115, 116, 1, 4};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }
  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

TEST_F(TestIntDictPdFilter, test_int_dict_null_const_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT; // integer dict

  const int64_t row_cnt = 120;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  for (int64_t i = 0; i < row_cnt - 5; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 5; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_int(i);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {115};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {5};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    int64_t ref_arr[4] = {-100, 30, 115, 200};
    int64_t res_arr_eq[4] = {0, 0, 1, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
    int64_t res_arr_ne[4] = {5, 5, 4, 5};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
  }

  // check LT/LE
  {
    int64_t ref_arr[4] = {-100, 30, 115, 119};
    int64_t res_arr_lt[4] = {0, 0, 0, 4};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    int64_t res_arr_le[4] = {0, 0, 1, 5};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }

  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

TEST_F(TestIntDictPdFilter, test_int_dict_const_without_null_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool is_force_raw = false;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObSmallIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT; // integer dict

  const int64_t row_cnt = 1200;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  for (int64_t i = 0; i < row_cnt - 100; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_int(0);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = 0; i < 10; ++i) {
    for (int64_t j = 0; j < 10; ++j) {
      int64_t cur_idx = row_cnt - 100 + (i * 10 + j);
      row_arr[cur_idx].storage_datums_[0].set_int32(cur_idx);
      row_arr[cur_idx].storage_datums_[1].set_int(1 + i * 10);
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[cur_idx]));
    }
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // NOTICE:
  // In this case, we will use 'abnormal' filter value, that means, although the column type is smallint,
  // we will use some value larger than INT16_MAX or less than INT16_MIN to check the correctness of filter.
  abnormal_filter_type_ = AbnormalFilterType::WIDER_WIDTH;

  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {1200};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    int64_t ref_arr[4] = {0, 1, 2, 32768};
    int64_t res_arr_eq[4] = {1100, 10, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 4, 1, res_arr_eq);
    int64_t res_arr_ne[4] = {100, 1190, 1200, 1200};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 4, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int64_t ref_arr[4] = {-32769, -1, 1, 90};
    int64_t res_arr_lt[4] = {0, 0, 1100, 1190};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 4, 1, res_arr_lt);
    int64_t res_arr_le[4] = {0, 0, 1110, 1190};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 4, 1, res_arr_le);
  }
  {
    int64_t ref_arr[4] = {-32769, -1, 10, 32768};
    int64_t res_arr_gt[4] = {1200, 1200, 90, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 4, 1, res_arr_gt);
    int64_t res_arr_ge[4] = {1200, 1200, 90, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 4, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int64_t ref_arr[4] = {-32769, 1, 91, 32768};
    int64_t res_arr[1] = {20};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 4, res_arr);
  }
  {
    int64_t ref_arr[10] = {-32769, -32768, -32769, 0, -1, 10, 0, 21, 1, 32768};
    int64_t res_arr[5] = {0, 1100, 1110, 1130, 100};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }

  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

TEST_F(TestIntDictPdFilter, test_all_null_int_dict_const_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool is_force_raw = false;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT; // integer dict

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

  /*
  // Actually, it won't use const encoding.
  */
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

  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

//test fix of
// but actually int dict encoding does not trigger this problem, because int dict are always sorted
// and take other paths. only integer encoding may trigger this problem.
TEST_F(TestIntDictPdFilter, test_exceed_range_compare_filter)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT;

  const int64_t row_cnt = 2;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.has_lob_out_row_ = true;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  row_arr[0].storage_datums_[0].set_int32(0);
  row_arr[0].storage_datums_[1].set_int(-10000000);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[0]));
  row_arr[1].storage_datums_[0].set_int32(1);
  row_arr[1].storage_datums_[1].set_int(-10000001);
  ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[1]));

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check EQ NE
  {
    int64_t ref_arr[5] = {-10000002, -10000001, -10000000, -1, 10000000};
    int64_t res_arr_eq[5] = {0, 1, 1, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {2, 1, 1, 2, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int64_t ref_arr[5] = {-10000002, -10000001, -10000000, -1, 10000000};
    int64_t res_arr_lt[5] = {0, 0, 1, 2, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {0, 1, 2, 2, 2};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int64_t ref_arr[5] = {-10000002, -10000001, -10000000, -1, 10000000};
    int64_t res_arr_gt[5] = {2, 1, 0, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {2, 2, 1, 0, 0};
    integer_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }
  LOG_INFO(">>>>>>>>>>FINISH PD FILTER<<<<<<<<<<<");
}

//
// but actually int dict encoding does not trigger this problem, because int dict are always sorted
// and take other paths. only integer encoding may trigger this problem.
TEST_F(TestIntDictPdFilter, test_singed_and_unsigned_compare_filter)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 3;
  const bool enable_check = ENABLE_CASE_CHECK;
  abnormal_filter_type_ = AbnormalFilterType::OPPOSITE_SIGN;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObUSmallIntType, ObIntType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::INT_DICT;

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
  system("rm -f test_int_dict_pd_filter.log*");
  OB_LOGGER.set_file_name("test_int_dict_pd_filter.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

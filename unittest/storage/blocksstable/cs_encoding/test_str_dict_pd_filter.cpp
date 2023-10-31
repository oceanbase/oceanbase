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

class TestStrDictPdFilter : public ObPdFilterTestBase
{

};

TEST_F(TestStrDictPdFilter, test_fixed_string_dict_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObCharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT; // fixed length string

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
    const int64_t char_data_arr_cnt = 4;
    const int64_t each_type_cnt = 25;
    char char_type_arr[char_data_arr_cnt] = {'a', 'b', 'c', 'd'};
    char **char_data_arr = static_cast<char **>(allocator_.alloc(sizeof(char *) * char_data_arr_cnt));
    for (int64_t i = 0; i < char_data_arr_cnt; ++i) {
      char_data_arr[i] = static_cast<char *>(allocator_.alloc(1024));
      ASSERT_TRUE(nullptr != char_data_arr[i]);
      MEMSET(char_data_arr[i], char_type_arr[i], 1024);
    }

    for (int64_t idx = 0; idx < char_data_arr_cnt; ++idx) {
      for (int64_t i = each_type_cnt * idx; i < each_type_cnt * (idx + 1); ++i) {
        row_arr[i].storage_datums_[0].set_int32(i);
        row_arr[i].storage_datums_[1].set_string(char_data_arr[idx], 100);
        ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
      }
    }
    for (int64_t i = row_cnt - null_cnt; i < row_cnt; ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      row_arr[i].storage_datums_[1].set_null();
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }

    HANDLE_TRANSFORM();

    const int64_t col_offset = 1;
    bool need_check = true;

    // check NU/NN
    {
      std::pair<int64_t, int64_t> ref_arr[1] = {{0, 5}};
      const int64_t nu_cnt = has_null ? null_cnt : 0;
      int64_t res_arr_nu[1] = {nu_cnt};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
      int64_t res_arr_nn[1] = {100};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
    }

    // check EQ/NE
    {
      std::pair<int64_t, int64_t> ref_arr[3] = {{0, 5}, {1, 30}, {2, 100}};
      int64_t res_arr_eq[3] = {0, 0, each_type_cnt};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 3, 1, res_arr_eq);
      int64_t res_arr_ne[3] = {100, 100, 100 - each_type_cnt};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 3, 1, res_arr_ne);
    }

    // check LT/LE/GT/GE
    {
      std::pair<int64_t, int64_t> ref_arr[3] = {{0, 100}, {1, 100}, {2, 100}};
      int64_t res_arr_lt[3] = {0, each_type_cnt , 2 * each_type_cnt};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 3, 1, res_arr_lt);
      int64_t res_arr_le[3] = {each_type_cnt, 2 * each_type_cnt , 3 * each_type_cnt};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 3, 1, res_arr_le);
    }
    {
      std::pair<int64_t, int64_t> ref_arr[3] = {{3, 100}, {2, 100}, {1, 100}};;
      int64_t res_arr_gt[3] = {0, each_type_cnt, 2 * each_type_cnt};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 3, 1, res_arr_gt);
      int64_t res_arr_ge[3] = {each_type_cnt, each_type_cnt * 2, 3 * each_type_cnt};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 3, 1, res_arr_ge);
    }

    // check IN/BT
    {
      std::pair<int64_t, int64_t> ref_arr[5] = {{0, 5}, {0, 100}, {1, 40}, {2, 100}, {3, 20}};
      int64_t res_arr[1] = {each_type_cnt * 2};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
    }
    {
      std::pair<int64_t, int64_t> ref_arr[6] = {{0, 100}, {1, 100}, {0, 100}, {2, 100}, {0, 100}, {3, 100}};
      int64_t res_arr[3] = {each_type_cnt * 2, each_type_cnt * 3, each_type_cnt * 4};
      string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 3, 2, res_arr);
    }
  }
}

TEST_F(TestStrDictPdFilter, test_var_string_dict_decoder_filter)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 3;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool is_force_raw = true;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObVarcharType, ObCharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT; // var string
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::STR_DICT; // fixed length string

  const int64_t row_cnt = 120;
  const int64_t delta = 20;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  const int64_t char_data_arr_cnt = 4;
  const int64_t each_type_cnt = (row_cnt - delta) / char_data_arr_cnt;
  char char_type_arr[char_data_arr_cnt + 1] = {'a', 'b', 'c', 'd', 'e'};
  char **char_data_arr = static_cast<char **>(allocator_.alloc(sizeof(char *) * (char_data_arr_cnt + 1)));
  for (int64_t i = 0; i < char_data_arr_cnt; ++i) {
    char_data_arr[i] = static_cast<char *>(allocator_.alloc(1024));
    ASSERT_TRUE(nullptr != char_data_arr[i]);
    MEMSET(char_data_arr[i], char_type_arr[i], 1024);
  }

  for (int64_t idx = 0; idx < char_data_arr_cnt; ++idx) {
    for (int64_t i = each_type_cnt * idx; i < each_type_cnt * (idx + 1); ++i) {
      row_arr[i].storage_datums_[0].set_int32(i);
      row_arr[i].storage_datums_[1].set_string(char_data_arr[idx], idx * 10);
      row_arr[i].storage_datums_[2].set_string(char_data_arr[idx], 100);
      ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
    }
  }

  for (int64_t i = row_cnt - delta; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_null();
    row_arr[i].storage_datums_[2].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  int64_t col_offset = 1;
  bool need_check = true;

  // check EQ/NE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 0}, {1, 10}, {2, 10}};
    int64_t res_arr_eq[3] = {25, 25, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 3, 1, res_arr_eq);
    int64_t res_arr_ne[3] = {75, 75, 100};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 3, 1, res_arr_ne);
  }

  // check GE/GT
  {
    col_offset = 2;
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 100}, {1, 100}, {2, 100}};
    int64_t res_arr_ge[3] = {100, 75, 50};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 3, 1, res_arr_ge);
    int64_t res_arr_gt[3] = {75, 50, 25};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 3, 1, res_arr_gt);
  }
}


TEST_F(TestStrDictPdFilter, test_fixed_string_dict_const_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool is_force_raw = true;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObCharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT; // fixed length string

  const int64_t row_cnt = 100;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  const int64_t char_data_arr_cnt = 4;
  const int64_t each_type_cnt = 25;
  char char_type_arr[char_data_arr_cnt] = {'a', 'b', 'c', 'd'};
  char **char_data_arr = static_cast<char **>(allocator_.alloc(sizeof(char *) * char_data_arr_cnt));
  for (int64_t i = 0; i < char_data_arr_cnt; ++i) {
    char_data_arr[i] = static_cast<char *>(allocator_.alloc(1024));
    ASSERT_TRUE(nullptr != char_data_arr[i]);
    MEMSET(char_data_arr[i], char_type_arr[i], 1024);
  }

  for (int64_t i = 0; i < 97; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_string(char_data_arr[0], 100);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  for (int64_t i = 97; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_string(char_data_arr[i-96], 100);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check NU/NN
  {
    std::pair<int64_t, int64_t> ref_arr[1] = {{0, 5}};
    int64_t res_arr_nu[1] = {0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {100};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 5}, {0, 100}, {1, 100}};
    int64_t res_arr_eq[3] = {0, 97, 1};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 3, 1, res_arr_eq);
    int64_t res_arr_ne[3] = {100, 3, 99};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 3, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 100}, {0, 200}, {1, 200}};
    int64_t res_arr_lt[3] = {0, 97, 98};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 3, 1, res_arr_lt);
    int64_t res_arr_le[3] = {97, 97, 98};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 3, 1, res_arr_le);
  }
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 50}, {0, 100}, {1, 100}};;
    int64_t res_arr_gt[3] = {100, 3, 2};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 3, 1, res_arr_gt);
    int64_t res_arr_ge[3] = {100, 100, 3};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 3, 1, res_arr_ge);
  }
}

TEST_F(TestStrDictPdFilter, test_var_string_dict_const_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  const bool has_null = true;
  const bool is_force_raw = false;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT; // var string

  const int64_t row_cnt = 120;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  encoder.is_all_column_force_raw_ = is_force_raw;
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  const int64_t char_data_arr_cnt = 5;
  char char_type_arr[char_data_arr_cnt] = {'a', 'b', 'c', 'd', 'e'};
  char **char_data_arr = static_cast<char **>(allocator_.alloc(sizeof(char *) * char_data_arr_cnt));
  for (int64_t i = 0; i < char_data_arr_cnt; ++i) {
    char_data_arr[i] = static_cast<char *>(allocator_.alloc(512));
    ASSERT_TRUE(nullptr != char_data_arr[i]);
    MEMSET(char_data_arr[i], char_type_arr[i], 512);
  }

  for (int64_t i = 0; i < row_cnt - 5; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_string(char_data_arr[0], 50);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  for (int64_t i = row_cnt - 5; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    if (has_null && (i == row_cnt - 1)) {
      row_arr[i].storage_datums_[1].set_null();
    } else {
      const int64_t cur_char_idx = (i - row_cnt + 6) % 5;
      row_arr[i].storage_datums_[1].set_string(char_data_arr[cur_char_idx], 50);
    }
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check NU/NN
  {
    std::pair<int64_t, int64_t> ref_arr[1] = {{0, 5}};
    int64_t res_arr_nu[1] = {1};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {119};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 50}, {1, 50}, {0, 100}};
    int64_t res_arr_eq[3] = {115, 1, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 3, 1, res_arr_eq);
    int64_t res_arr_ne[3] = {4, 118, 119};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 3, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 50}, {0, 51}, {2, 50}};
    int64_t res_arr_lt[3] = {0, 115 , 116};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 3, 1, res_arr_lt);
    int64_t res_arr_le[3] = {115, 115, 117};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 3, 1, res_arr_le);
  }
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 49}, {0, 50}, {2, 50}};
    int64_t res_arr_gt[3] = {119, 4, 2};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 3, 1, res_arr_gt);
    int64_t res_arr_ge[3] = {119, 119, 3};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 3, 1, res_arr_ge);
  }

  // check IN/BT
  {
    std::pair<int64_t, int64_t> ref_arr[5] = {{0, 50}, {0, 100}, {1, 40}, {2, 100}, {3, 50}};
    int64_t res_arr[1] = {116};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    std::pair<int64_t, int64_t> ref_arr[6] = {{0, 10}, {0, 49}, {0, 10}, {1, 50}, {1, 50}, {4, 50}};
    int64_t res_arr[3] = {0, 116, 4};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 3, 2, res_arr);
  }
}

TEST_F(TestStrDictPdFilter, test_string_dict_all_const_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT; // var string

  const int64_t row_cnt = 120;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  const int64_t char_data_arr_cnt = 5;
  char char_type_arr[char_data_arr_cnt] = {'a', 'b', 'c', 'd', 'e'};
  char **char_data_arr = static_cast<char **>(allocator_.alloc(sizeof(char *) * char_data_arr_cnt));
  for (int64_t i = 0; i < char_data_arr_cnt; ++i) {
    char_data_arr[i] = static_cast<char *>(allocator_.alloc(512));
    ASSERT_TRUE(nullptr != char_data_arr[i]);
    MEMSET(char_data_arr[i], char_type_arr[i], 512);
  }

  for (int64_t i = 0; i < row_cnt; ++i) {
    row_arr[i].storage_datums_[0].set_int32(i);
    row_arr[i].storage_datums_[1].set_string(char_data_arr[0], 50);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  const int64_t col_offset = 1;
  bool need_check = true;

  // check NU/NN
  {
    std::pair<int64_t, int64_t> ref_arr[1] = {{0, 5}};
    int64_t res_arr_nu[1] = {0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {120};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 50}, {1, 50}, {0, 100}};
    int64_t res_arr_eq[3] = {120, 0, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 3, 1, res_arr_eq);
    int64_t res_arr_ne[3] = {0, 120, 120};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 3, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 50}, {0, 51}, {2, 50}};
    int64_t res_arr_lt[3] = {0, 120, 120};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 3, 1, res_arr_lt);
    int64_t res_arr_le[3] = {120, 120, 120};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 3, 1, res_arr_le);
  }
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 49}, {0, 50}, {2, 50}};
    int64_t res_arr_gt[3] = {120, 0, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 3, 1, res_arr_gt);
    int64_t res_arr_ge[3] = {120, 120, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 3, 1, res_arr_ge);
  }

  // check IN/BT
  {
    std::pair<int64_t, int64_t> ref_arr[5] = {{0, 50}, {0, 100}, {1, 40}, {2, 100}, {3, 50}};
    int64_t res_arr[1] = {120};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    std::pair<int64_t, int64_t> ref_arr[6] = {{0, 10}, {0, 49}, {0, 10}, {1, 50}, {1, 50}, {4, 50}};
    int64_t res_arr[3] = {0, 120, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 3, 2, res_arr);
  }
}

TEST_F(TestStrDictPdFilter, test_string_dict_all_null_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 2;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObInt32Type, ObVarcharType};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INT_DICT; // integer dict
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::STR_DICT; // var string

  const int64_t row_cnt = 120;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));
  ObDatumRow row_arr[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }
  const int64_t char_data_arr_cnt = 5;
  char char_type_arr[char_data_arr_cnt] = {'a', 'b', 'c', 'd', 'e'};
  char **char_data_arr = static_cast<char **>(allocator_.alloc(sizeof(char *) * char_data_arr_cnt));
  for (int64_t i = 0; i < char_data_arr_cnt; ++i) {
    char_data_arr[i] = static_cast<char *>(allocator_.alloc(512));
    ASSERT_TRUE(nullptr != char_data_arr[i]);
    MEMSET(char_data_arr[i], char_type_arr[i], 512);
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
    std::pair<int64_t, int64_t> ref_arr[1] = {{0, 5}};
    int64_t res_arr_nu[1] = {120};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }

  // check EQ/NE
  {
    std::pair<int64_t, int64_t> ref_arr[3] = {{0, 50}, {1, 50}, {0, 100}};
    int64_t res_arr_eq[3] = {0, 0, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 3, 1, res_arr_eq);
    int64_t res_arr_ne[3] = {0, 0, 0};
    string_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 3, 1, res_arr_ne);
  }
}

}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_str_dict_pd_filter.log*");
  OB_LOGGER.set_file_name("test_str_dict_pd_filter.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

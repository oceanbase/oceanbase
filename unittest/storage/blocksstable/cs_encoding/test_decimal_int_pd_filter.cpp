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

class TestDecimalIntPdFilter : public ObPdFilterTestBase
{
};

TEST_F(TestDecimalIntPdFilter, test_decimal_int_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 7;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType};
  int64_t precision_arr[col_cnt] = {MAX_PRECISION_DECIMAL_INT_64,
                                    MAX_PRECISION_DECIMAL_INT_32,
                                    MAX_PRECISION_DECIMAL_INT_128,
                                    OB_MAX_DECIMAL_PRECISION,
                                    MAX_PRECISION_DECIMAL_INT_128,
                                    MAX_PRECISION_DECIMAL_INT_128,
                                    OB_MAX_DECIMAL_PRECISION};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt, ObCompressorType::ZSTD_1_3_8_COMPRESSOR, precision_arr));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[4] = ObCSColumnHeader::Type::STRING;
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
    } else { // [0 ~ 996]
      p = i + INT64_MIN;
      row_arr[i].storage_datums_[0].set_decimal_int(p);
      m = i - 500;
      row_arr[i].storage_datums_[1].set_decimal_int(m); // -500 ~ 496
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

  HANDLE_TRANSFORM();
  int64_t col_offset = 0; // INTEGER: [0-996] + INT64_MIN, 0, INT32_MAX, INT64_MAX
  bool need_check = true;
  // check NU/NN
  {
    int32_t ref_arr[1];
    int64_t res_arr_nu[1] = {0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {1000};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int128_t ref_arr[5] = {INT64_MIN, 0, INT32_MIN, INT32_MAX, INT64_MAX};
    int64_t res_arr_eq[5] = {1, 1, 0, 1, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {999, 999, 1000, 999, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }
  // check LT/LE/GT/GE
  {
    int32_t ref_arr[5] = {INT32_MIN, 0, INT32_MAX};
    int64_t res_arr_lt[5] = {997, 997, 998};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 3, 1, res_arr_lt);
    int64_t res_arr_le[5] = {997, 998, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 3, 1, res_arr_le);
  }
  {
    int128_t ref_arr[5] = {int128_t(INT64_MIN) << 64, INT32_MIN, 0, INT32_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_gt[5] = {1000, 3, 2, 1, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {1000, 3, 3, 2, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }
  // check IN/BT
  {
    int32_t ref_arr[3] = {INT32_MIN, 0, INT32_MAX};
    int64_t res_arr[1] = {2};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 3, res_arr);
  }
  {
    int128_t ref_arr[10] = {int128_t(INT64_MIN) << 64, INT64_MIN, INT64_MIN, INT32_MIN,
        INT32_MIN, 0, 0, INT32_MAX, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[5] = {1, 997, 1, 2, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }


  col_offset = 1; // INTEGER: INT32_MIN, INT32_MAX, null, [-500 ~ 496]
  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int128_t ref_arr[5] = {INT64_MIN, 0, INT32_MIN, INT32_MAX, INT64_MAX};
    int64_t res_arr_eq[5] = {0, 1, 1, 1, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {999, 998, 998, 998, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int128_t ref_arr[5] = {int128_t(INT64_MIN) << 64, INT32_MIN, 0, INT32_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_lt[5] = {0, 0, 501, 998, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {0, 1, 502, 999, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int128_t ref_arr[5] = {int128_t(INT64_MIN) << 64, INT32_MIN, 0, INT32_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_gt[5] = {999, 998, 497, 0, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {999, 999, 498, 1, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int128_t ref_arr[5] = {int128_t(INT64_MIN) << 64, INT32_MIN, 0, INT32_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[1] = {3};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    int128_t ref_arr[10] = {int128_t(INT64_MIN) << 64, INT64_MIN, INT64_MIN, INT32_MIN,
        INT32_MIN, 0, 0, INT32_MAX, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[5] = {0, 1, 502, 498, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }


  col_offset = 2; // INTEGER: INT64_MIN, INT64_MAX, null, [0-996] + INT32_MAX
  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int128_t ref_arr[5] = {INT64_MIN, 0, INT32_MIN, INT32_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_eq[5] = {1, 0, 0, 1, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {998, 999, 999, 998, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int256_t ref_arr[5] = {int256_t(INT64_MIN) << 192, INT32_MIN, 0, INT32_MAX, INT64_MAX};
    int64_t res_arr_lt[5] = {0, 1, 1, 1, 998};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {0, 1, 1, 2, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int256_t ref_arr[5] = {INT64_MIN, INT32_MIN, 0, INT32_MAX, int256_t(INT64_MAX) << 64};
    int64_t res_arr_gt[5] = {998, 998, 998, 997, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {999, 998, 998, 998, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int128_t ref_arr[5] = {int128_t(INT64_MIN) << 64, INT32_MIN, 0, INT32_MAX, INT64_MAX};
    int64_t res_arr[1] = {2};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    int256_t ref_arr[10] = {int256_t(INT64_MIN) << 192, INT64_MIN, INT64_MIN, INT32_MIN,
        INT32_MIN, 0, 0, INT64_MAX, INT64_MAX, int256_t(INT64_MAX) << 192};
    int64_t res_arr[5] = {1, 1, 0, 998, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }

  col_offset = 3; // INT_DICT:  null, [-50-46]@10, (47,48,49)@9
  // check NU/NN
  {
    int128_t ref_arr[1];
    int64_t res_arr_nu[1] = {1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int128_t ref_arr[5] = {INT64_MIN, -50, 0, 49, int128_t(INT64_MAX) << 64};
    int64_t res_arr_eq[5] = {0, 10, 10, 9, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {999, 989, 989, 990, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int512_t ref_arr[5] = {int128_t(INT64_MIN) << 64, -50, 0, 49, INT64_MAX};
    int64_t res_arr_lt[5] = {0, 0, 500, 990, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {0, 10, 510, 999, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int512_t ref_arr[5] = {INT64_MIN, -50, 0, 49, int128_t(INT64_MAX) << 64};
    int64_t res_arr_gt[5] = {999, 989, 489, 0, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {999, 999, 499, 9, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int128_t ref_arr[5] = {int128_t(INT64_MIN) << 64, -50, 0, 49, INT64_MAX};
    int64_t res_arr[1] = {29};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    int128_t ref_arr[10] = {int128_t(INT64_MIN) << 64, INT64_MIN, INT64_MIN, -50,
        -50, 0, 0, INT64_MAX, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[5] = {0, 10, 510, 499, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }

  col_offset = 4; // STRING:  (int128_t)INT64_MIN << 64, [0-996], (int128_t)INT64_MAX << 64, null
  // check NU/NN
  {
    int128_t ref_arr[1];
    int64_t res_arr_nu[1] = {1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int128_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_eq[5] = {1, 0, 1, 0, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {998, 999, 998, 999, 998};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int128_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_lt[5] = {0, 1, 1, 998, 998};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {1, 1, 2, 998, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int128_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_gt[5] = {998, 998, 997, 1, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {999, 998, 998, 1, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int128_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[1] = {3};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    int128_t ref_arr[10] = {int128_t(INT64_MIN) << 64, INT64_MIN, INT64_MIN, 0,
        0, INT32_MAX, INT32_MAX, INT64_MAX, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[5] = {1, 1, 997, 0, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }

  col_offset = 5; // STRING:  (int128_t)INT64_MIN << 64, (int128_t)INT64_MAX << 64, null@998
  // check NU/NN
  {
    int128_t ref_arr[1];
    int64_t res_arr_nu[1] = {998};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {2};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int512_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_eq[5] = {1, 0, 0, 0, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {1, 2, 2, 2, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int128_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_lt[5] = {0, 1, 1, 1, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {1, 1, 1, 1, 2};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int512_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr_gt[5] = {1, 1, 1, 1, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {2, 1, 1, 1, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }

  // check IN/BT
  {
    int128_t ref_arr[5] = {(int128_t)INT64_MIN << 64, INT64_MIN, 0, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[1] = {2};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  {
    int128_t ref_arr[10] = {int128_t(INT64_MIN) << 64, INT64_MIN, INT64_MIN, 0,
        0, INT32_MAX, INT32_MAX, INT64_MAX, INT64_MAX, int128_t(INT64_MAX) << 64};
    int64_t res_arr[5] = {1, 0, 0, 0, 1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }

  col_offset = 6; //  k = (int256_t)[-50-48]@10 << 128, 49<<128@9, null
  // check NU/NN
  {
    int128_t ref_arr[1];
    int64_t res_arr_nu[1] = {1};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int512_t ref_arr[5] = {(int512_t)INT64_MIN << 128, (int512_t)-50 << 128 , 0, (int512_t)49 << 128 , int512_t(INT64_MAX) << 128};
    int64_t res_arr_eq[5] = {0, 10, 10, 9, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {999, 989, 989, 990, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int512_t ref_arr[5] = {(int512_t)INT64_MIN << 128, (int512_t)-50 << 128 , 0, (int512_t)49 << 128 , int512_t(INT64_MAX) << 128};
    int64_t res_arr_lt[5] = {0, 0, 500, 990, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {0, 10, 510, 999, 999};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int512_t ref_arr[5] = {(int512_t)INT64_MIN << 128, (int512_t)-50 << 128 , 0, (int512_t)49 << 128 , int512_t(INT64_MAX) << 128};
    int64_t res_arr_gt[5] = {999, 989, 489, 0, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {999, 999, 499, 9, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }
  // check IN/BT
  // if filter precision is different with col precision, the hash is different even if the integer value is equal
  /*
  {
    int512_t ref_arr[5] = {(int512_t)INT64_MIN << 128, (int512_t)-50 << 128 , 0, (int512_t)49 << 128 , int512_t(INT64_MAX) << 128};
    int64_t res_arr[1] = {29};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_IN, 1, 5, res_arr);
  }
  */

  {
    int512_t ref_arr[10] = {int512_t(INT64_MIN) << 256, (int512_t)INT64_MIN << 128, (int512_t)INT64_MIN << 128, 0,
        0, (int512_t)INT64_MAX << 64, (int512_t)INT64_MAX << 64, (int512_t)INT64_MAX << 128,
        int512_t(INT64_MAX) << 128, int512_t(INT64_MAX) << 256};
    int64_t res_arr[5] = {0, 510, 10, 489, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_BT, 5, 2, res_arr);
  }

}

TEST_F(TestDecimalIntPdFilter, test_decimal_int_const_decoder)
{
  const int64_t rowkey_cnt = 1;
  const int64_t col_cnt = 4;
  const bool enable_check = ENABLE_CASE_CHECK;
  ObObjType col_types[col_cnt] = {ObDecimalIntType, ObDecimalIntType, ObDecimalIntType, ObDecimalIntType};
  int64_t precision_arr[col_cnt] = {MAX_PRECISION_DECIMAL_INT_64,
                                    MAX_PRECISION_DECIMAL_INT_32,
                                    MAX_PRECISION_DECIMAL_INT_128,
                                    OB_MAX_DECIMAL_PRECISION};
  ASSERT_EQ(OB_SUCCESS, prepare(col_types, rowkey_cnt, col_cnt, ObCompressorType::ZSTD_1_3_8_COMPRESSOR, precision_arr));
  ctx_.column_encodings_[0] = ObCSColumnHeader::Type::INTEGER;
  ctx_.column_encodings_[1] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[2] = ObCSColumnHeader::Type::INT_DICT;
  ctx_.column_encodings_[3] = ObCSColumnHeader::Type::STR_DICT;
  int64_t row_cnt = 1000;
  ObMicroBlockCSEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(ctx_));

  void *row_arr_buf = allocator_.alloc(sizeof(ObDatumRow) * row_cnt);
  ASSERT_TRUE(nullptr != row_arr_buf);
  ObDatumRow *row_arr = new(row_arr_buf) ObDatumRow[row_cnt];
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_arr[i].init(allocator_, col_cnt));
  }

  int32_t m = 0;
  int128_t j = 0;
  int256_t k = 0;
  for (int64_t i = 0; i < row_cnt - 50; i++) {
    row_arr[i].storage_datums_[0].set_decimal_int(i);
    m = INT32_MAX;
    row_arr[i].storage_datums_[1].set_decimal_int(m);
    row_arr[i].storage_datums_[2].set_null();
    k = (int256_t)INT64_MAX << 128;
    row_arr[i].storage_datums_[3].set_decimal_int(k);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 50; i < row_cnt - 10; i++) {
    m = i;
    row_arr[i].storage_datums_[0].set_decimal_int(i);
    row_arr[i].storage_datums_[1].set_decimal_int(m);
    j = INT64_MIN;
    row_arr[i].storage_datums_[2].set_decimal_int(j);
    k = 0;
    row_arr[i].storage_datums_[3].set_decimal_int(k);
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }
  for (int64_t i = row_cnt - 10; i < row_cnt; i++) {
    row_arr[i].storage_datums_[0].set_decimal_int(i);
    row_arr[i].storage_datums_[1].set_null();
    row_arr[i].storage_datums_[2].set_null();
    row_arr[i].storage_datums_[3].set_null();
    ASSERT_EQ(OB_SUCCESS, encoder.append_row(row_arr[i]));
  }

  HANDLE_TRANSFORM();

  int64_t col_offset = 1; // INT_DICT: INT32_MAX@950, [950-989] null@10
  bool need_check = true;
  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {10};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {990};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int128_t ref_arr[5] = {INT32_MAX, 950, 960, 989, 990};
    int64_t res_arr_eq[5] = {950, 1, 1, 1, 0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 5, 1, res_arr_eq);
    int64_t res_arr_ne[5] = {40, 989, 989, 989, 990};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 5, 1, res_arr_ne);
  }

  // check LT/LE/GT/GE
  {
    int128_t ref_arr[5] = {INT32_MAX, 950, 960, 989, 990};
    int64_t res_arr_lt[5] = {40, 0, 10, 39, 40};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LT, 5, 1, res_arr_lt);
    int64_t res_arr_le[5] = {990, 1, 11, 40, 40};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_LE, 5, 1, res_arr_le);
  }
  {
    int128_t ref_arr[5] = {INT32_MAX, 950, 960, 989, 990};
    int64_t res_arr_gt[5] = {0, 989, 979, 950, 950};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GT, 5, 1, res_arr_gt);
    int64_t res_arr_ge[5] = {950, 990, 980, 951, 950};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_GE, 5, 1, res_arr_ge);
  }

  col_offset = 2; // INT_DICT:  null@960 INT64_MIN@40
  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {960};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {40};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int128_t ref_arr[1] = {INT64_MIN};
    int64_t res_arr_eq[1] = {40};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 1, 1, res_arr_eq);
    int64_t res_arr_ne[1] = {0};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 1, 1, res_arr_ne);
  }

  col_offset = 3; // STIRNG_DICT:  (int256_t)INT64_MAX << 128@950, null@10, 0@40
  // check NU/NN
  {
    int64_t ref_arr[1];
    int64_t res_arr_nu[1] = {10};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NU, 1, 0, res_arr_nu);
    int64_t res_arr_nn[1] = {990};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NN, 1, 0, res_arr_nn);
  }
  // check EQ/NE
  {
    int256_t ref_arr[2] = {(int256_t)INT64_MAX << 128, 0};
    int64_t res_arr_eq[2] = {950, 40};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_EQ, 2, 1, res_arr_eq);
    int64_t res_arr_ne[2] = {40, 950};
    decimal_type_filter_normal_check(true, ObWhiteFilterOperatorType::WHITE_OP_NE, 2, 1, res_arr_ne);
  }
}

}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_decimal_int_pd_filter.log*");
  OB_LOGGER.set_file_name("test_decimal_int_pd_filter.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

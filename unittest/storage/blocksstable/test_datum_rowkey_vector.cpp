/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <chrono>
#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/oblog/ob_log_module.h"
#include "storage/blocksstable/ob_datum_rowkey_vector.h"
namespace oceanbase
{

using namespace std::chrono;
using namespace share;
using namespace common;
using namespace storage;

namespace blocksstable
{
class ObDatumRowkeyVectorTest: public ::testing::Test
{
public:
  ObDatumRowkeyVectorTest();
  virtual ~ObDatumRowkeyVectorTest() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
private:
  void prepare_datum_util(const int64_t rowkey_cnt, ObStorageDatumUtils &datum_util);
  bool is_oracle_mode_;
  ObArenaAllocator allocator_;
};

ObDatumRowkeyVectorTest::ObDatumRowkeyVectorTest()
{
  is_oracle_mode_ = false;
}

void ObDatumRowkeyVectorTest::SetUpTestCase()
{
}

void ObDatumRowkeyVectorTest::TearDownTestCase()
{
}

void ObDatumRowkeyVectorTest::SetUp()
{
}

void ObDatumRowkeyVectorTest::TearDown()
{
}

void ObDatumRowkeyVectorTest::prepare_datum_util(const int64_t rowkey_cnt, ObStorageDatumUtils &datum_util)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(rowkey_cnt < 16);
  ObSEArray<share::schema::ObColDesc, 16> cols_desc;
  for (int64_t i = 0; i < rowkey_cnt; ++i) {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = 16 + i;
    col_desc.col_type_.set_type(ObIntType);
    col_desc.col_type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    col_desc.col_type_.set_collation_level(CS_LEVEL_IMPLICIT);
    ret = cols_desc.push_back(col_desc);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  datum_util.init(cols_desc, rowkey_cnt, false, allocator_);
  ASSERT_EQ(ret, OB_SUCCESS);
}

TEST_F(ObDatumRowkeyVectorTest, int_vector_locate_key)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(1, datum_utils);
  const ObStorageDatumCmpFunc &cmp_func = datum_utils.get_cmp_funcs().at(0);
  int64_t row_count = 10;
  int64_t row_arr[10] = {1,2,3,3,3,4,5,5,6,6};
  ObColumnVector int_vec;
  int_vec.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec.signed_ints_ = row_arr;
  int_vec.row_cnt_ = row_count;
  int_vec.has_null_ = false;

  bool need_upper_bound = false;
  int64_t begin = 0;
  int64_t end = row_count;
  int64_t key = 3;
  ObStorageDatum datum_key;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 2);
  ASSERT_EQ(end, 2);

  need_upper_bound = true;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 2);
  ASSERT_EQ(end, 5);

  key = 6;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 8);
  ASSERT_EQ(end, 10);

  row_count = 1;
  int64_t row_arr1[1] = {0};
  int_vec.signed_ints_ = row_arr1;
  int_vec.row_cnt_ = row_count;
  key = 0;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 0);
  ASSERT_EQ(end, 1);
}

TEST_F(ObDatumRowkeyVectorTest, datum_vector_locate_key)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(1, datum_utils);
  const ObStorageDatumCmpFunc &cmp_func = datum_utils.get_cmp_funcs().at(0);

  int64_t row_count = 10;
  int64_t row_arr[10] = {1,2,3,3,3,4,5,5,6,6};
  ObStorageDatum datum_arr[10];
  for (int64_t i = 0; i < row_count; ++i) {
    datum_arr[i].set_int(row_arr[i]);
  }
  ObColumnVector datum_vec;
  datum_vec.type_ = ObColumnVectorType::DATUM_TYPE;
  datum_vec.datums_ = datum_arr;
  datum_vec.row_cnt_ = row_count;

  ObStorageDatum datum_key;
  bool need_upper_bound = false;
  int64_t key = 3;
  int64_t begin = 0;
  int64_t end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = datum_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 2);
  ASSERT_EQ(end, 2);

  need_upper_bound = true;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = datum_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 2);
  ASSERT_EQ(end, 5);

  key = 6;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = datum_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 8);
  ASSERT_EQ(end, 10);

  int64_t int_val;
  ret = datum_vec.get_column_int(5, int_val);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(int_val, 4);

  row_count = 1;
  ObStorageDatum datum_arr1[1];
  datum_arr1[0].set_int(0);
  datum_vec.datums_ = datum_arr1;
  datum_vec.row_cnt_ = row_count;
  key = 0;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = datum_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 0);
  ASSERT_EQ(end, 1);
}

TEST_F(ObDatumRowkeyVectorTest, int_vector_locate_key_with_null)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(1, datum_utils);
  const ObStorageDatumCmpFunc &cmp_func = datum_utils.get_cmp_funcs().at(0);

  int64_t row_count = 10;
  ObColumnVector int_vec;
  int_vec.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int64_t row_arr[10] = {1,2,3,3,3,4,5,5,6,6};
  bool null_arr[10] = {false,false,false,false,false,false,false,false,false,false};
  int_vec.signed_ints_ = row_arr;
  int_vec.row_cnt_ = row_count;
  int_vec.nulls_ = null_arr;
  int_vec.has_null_ = false;
  ObStorageDatum datum_key;

  bool need_upper_bound = false;
  int64_t begin = 0;
  int64_t end = row_count;
  datum_key.reuse();
  datum_key.set_null();
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 0);
  ASSERT_EQ(end, 0);

  for (int64_t i = 0; i < 5; ++i) {
    null_arr[i] = true;
  }
  int_vec.has_null_ = true;
  begin = 0;
  end = row_count;
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 0);
  ASSERT_EQ(end, 0);

  need_upper_bound = true;
  begin = 0;
  end = row_count;
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 0);
  ASSERT_EQ(end, 5);

  int64_t key = 3;
  need_upper_bound = false;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 5);
  ASSERT_EQ(end, 5);

  key = 6;
  begin = 0;
  end = row_count;
  datum_key.reuse();
  datum_key.set_int(key);
  ret = int_vec.locate_key(need_upper_bound, begin, end, datum_key, cmp_func, is_oracle_mode_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin, 8);
  ASSERT_EQ(end, 8);
}

TEST_F(ObDatumRowkeyVectorTest, rowkey_vector_locate_key)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(1, datum_utils);

  // 0 3 6 9 12 ... 3*(n-1) ... 2997
  int64_t row_count = 1000;
  int64_t interval = 3;
  ObColumnVector int_vec;
  int_vec.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  void *buf = allocator_.alloc(sizeof(int64_t) *row_count);
  ASSERT_TRUE(buf != nullptr);
  int_vec.signed_ints_ = static_cast<int64_t *>(buf);
  int_vec.row_cnt_ = row_count;
  int_vec.has_null_ = false;
  for (int64_t i = 0; i < row_count; ++i) {
    int_vec.signed_ints_[i] = i * interval;
  }

  ObColumnVector col_vectors[1];
  col_vectors[0] = int_vec;
  ObRowkeyVector rowkey_vector;
  rowkey_vector.columns_ = col_vectors;
  rowkey_vector.col_cnt_ = 1;
  rowkey_vector.row_cnt_ = row_count;

  ObDatumRowkey rowkey;
  ObStorageDatum rowkey_datums[1];
  ret = rowkey.assign(rowkey_datums, 1);
  ASSERT_EQ(ret, OB_SUCCESS);

  int64_t key_arr[11] = {-1,0,1,2,3,4,1000,1500,2996,2997,2998};
  for (int64_t i = 0; i < 11; ++i) {
    int64_t key = key_arr[i];
    rowkey_datums[0].set_int(key);
    int64_t rowkey_idx = -1;
    ret = rowkey_vector.locate_key(0, row_count, rowkey, datum_utils, rowkey_idx);
    ASSERT_EQ(ret, OB_SUCCESS);
    int64_t target_idx = key < 0 ?
                         0 : key / interval + (key % interval == 0 ? 0 : 1);
    ASSERT_EQ(rowkey_idx, target_idx) << "i="<< i << " key=" << key;
  }
}

TEST_F(ObDatumRowkeyVectorTest, rowkey_vector_locate_range)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(1, datum_utils);

  // 0 3 6 9 12 ... 3*(n-1) ... 2997
  int64_t row_count = 1000;
  int64_t interval = 3;
  ObColumnVector int_vec;
  int_vec.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  void *buf = allocator_.alloc(sizeof(int64_t) *row_count);
  ASSERT_TRUE(buf != nullptr);
  int_vec.signed_ints_ = static_cast<int64_t *>(buf);
  for (int64_t i = 0; i < row_count; ++i) {
    int_vec.signed_ints_[i] = i * interval;
  }
  bool *nulls = static_cast<bool*>(allocator_.alloc(sizeof(bool) * row_count));
  ASSERT_TRUE(nulls != nullptr);
  for (int64_t i = 0; i < row_count; ++i) {
    nulls[i] = false;
  }
  int_vec.row_cnt_ = row_count;
  int_vec.nulls_ = nulls;
  int_vec.has_null_ = false;

  ObColumnVector col_vectors[1];
  col_vectors[0] = int_vec;
  ObRowkeyVector rowkey_vector;
  rowkey_vector.columns_ = col_vectors;
  rowkey_vector.col_cnt_ = 1;
  rowkey_vector.row_cnt_ = row_count;

  ObDatumRange range;
  ObDatumRowkey start_key, end_key;
  ObStorageDatum start_datums[1];
  ObStorageDatum end_datums[1];
  ret = start_key.assign(start_datums, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = end_key.assign(end_datums, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  range.set_start_key(start_key);
  range.set_end_key(end_key);

  bool is_left_border = true;
  bool is_right_border = true;
  bool is_normal_cg = false;
  int64_t begin_idx, end_idx;
  start_datums[0].set_min();
  end_datums[0].set_max();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, row_count - 1);

  start_datums[0].set_min();
  end_datums[0].set_int(9);
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, 4);

  end_datums[0].set_int(9);
  range.set_right_open();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, 3);

  start_datums[0].set_int(9);
  end_datums[0].set_int(2500);
  range.set_left_open();
  range.set_right_open();

  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 4);
  ASSERT_EQ(end_idx, 834);

  start_datums[0].set_int(2610);
  end_datums[0].set_max();
  range.set_left_open();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 871);
  ASSERT_EQ(end_idx, row_count - 1);

  range.set_left_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 870);
  ASSERT_EQ(end_idx, row_count - 1);

  for (int64_t i = 0; i < row_count; ++i) {
    nulls[i] = true;
  }
  rowkey_vector.columns_[0].has_null_ = true;
  start_datums[0].set_null();
  end_datums[0].set_null();
  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, row_count - 1);
}

TEST_F(ObDatumRowkeyVectorTest, rowkey_vector_locate_range_2int_col)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(2, datum_utils);

  int64_t row_count = 13;
  int64_t int_arr0[13] = {1,1,1,2,2,2,2,2,3,3,4,5,7};
  bool bool_arr0[13] = {false,false,false,false,false,false,false,false,false,false,false,false,false};
  int64_t int_arr1[13] = {2,3,4,1,1,1,2,3,4,5,6,6,8};
  bool bool_arr1[13] = {false,false,false,false,false,false,false,false,false,false,false,false,false};
  ObColumnVector int_vec0;
  int_vec0.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec0.row_cnt_ = row_count;
  int_vec0.has_null_ = false;
  int_vec0.signed_ints_ = int_arr0;
  int_vec0.nulls_ = bool_arr0;

  ObColumnVector int_vec1;
  int_vec1.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec1.row_cnt_ = row_count;
  int_vec1.has_null_ = false;
  int_vec1.signed_ints_ = int_arr1;
  int_vec1.nulls_ = bool_arr1;

  ObColumnVector col_vectors[2];
  col_vectors[0] = int_vec0;
  col_vectors[1] = int_vec1;
  ObRowkeyVector rowkey_vector;
  rowkey_vector.columns_ = col_vectors;
  rowkey_vector.col_cnt_ = 2;
  rowkey_vector.row_cnt_ = row_count;

  ObDatumRange range;
  ObDatumRowkey start_key, end_key;
  ObStorageDatum start_datums[2];
  ObStorageDatum end_datums[2];
  ret = start_key.assign(start_datums, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = end_key.assign(end_datums, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  range.set_start_key(start_key);
  range.set_end_key(end_key);

  bool is_normal_cg = false;
  bool is_left_border = true;
  bool is_right_border = true;
  int64_t begin_idx, end_idx;
  // (1,3 : 2,1) -> 2,3
  start_datums[0].set_int(1);
  start_datums[1].set_int(3);
  end_datums[0].set_int(2);
  end_datums[1].set_int(1);
  range.set_left_open();
  range.set_right_open();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 2);
  ASSERT_EQ(end_idx, 3);

  // (1,3 : 2,1] -> 2,6
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 2);
  ASSERT_EQ(end_idx, 6);

  // [1,3 : 2,1] -> 1,6
  range.set_left_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 1);
  ASSERT_EQ(end_idx, 6);

  // middle block
  is_left_border = false;
  is_right_border = false;
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, 12);

  is_left_border = true;
  is_right_border = true;
  // [2,1 : 2,1] -> 3,6
  start_datums[0].set_int(2);
  start_datums[1].set_int(1);
  end_datums[0].set_int(2);
  end_datums[1].set_int(1);
  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 3);
  ASSERT_EQ(end_idx, 6);

  // [3,6 : 6,6] -> 10,12
  start_datums[0].set_int(3);
  start_datums[1].set_int(6);
  end_datums[0].set_int(6);
  end_datums[1].set_int(6);
  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 10);
  ASSERT_EQ(end_idx, 12);

  // [2,min : 3,max] -> 3,10
  start_datums[0].set_int(2);
  start_datums[1].set_min();
  end_datums[0].set_int(3);
  end_datums[1].set_max();
  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 3);
  ASSERT_EQ(end_idx, 10);

  // [0,1 : 7,8] -> 0,12
  start_datums[0].set_int(0);
  start_datums[1].set_int(1);
  end_datums[0].set_int(7);
  end_datums[1].set_int(8);
  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, 12);

  // [8,1 : 3,max] -> OB_BEYOND_THE_RANGE
  start_datums[0].set_int(8);
  start_datums[1].set_int(1);
  end_datums[0].set_int(3);
  end_datums[1].set_max();
  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_BEYOND_THE_RANGE);

  MEMSET(bool_arr0, true, sizeof(bool_arr0));
  rowkey_vector.columns_[0].has_null_ = true;
  for (int64_t i = 0; i < 13; ++i) {
    int_arr1[i] = i;
  }
  start_datums[0].set_null();
  start_datums[1].set_min();
  end_datums[0].set_null();
  end_datums[1].set_max();
  range.set_left_open();
  range.set_right_open();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, 12);

  start_datums[1].set_int(2);
  end_datums[1].set_int(9);
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 3);
  ASSERT_EQ(end_idx, 9);

  MEMSET(bool_arr0, false, sizeof(bool_arr0));
  MEMSET(bool_arr1, true, sizeof(bool_arr1));
  rowkey_vector.columns_[0].has_null_ = false;
  rowkey_vector.columns_[1].has_null_ = true;
  for (int64_t i = 0; i < 13; ++i) {
    int_arr0[i] = i;
  }
  start_datums[0].set_int(1);
  start_datums[1].set_null();
  end_datums[0].set_int(7);
  end_datums[1].set_null();
  range.set_left_open();
  range.set_right_open();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 2);
  ASSERT_EQ(end_idx, 7);

  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 1);
  ASSERT_EQ(end_idx, 8);
}

TEST_F(ObDatumRowkeyVectorTest, rowkey_vector_locate_range_with_datum)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(2, datum_utils);

  int64_t row_count = 10;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * row_count);
  ASSERT_TRUE(buf != nullptr);
  ObStorageDatum *datums = new (buf) ObStorageDatum[row_count];
  ObColumnVector datum_vec0;
  datum_vec0.type_ = ObColumnVectorType::DATUM_TYPE;
  datum_vec0.row_cnt_ = row_count;
  datum_vec0.datums_ = datums;
  for (int64_t i = 0; i < row_count; ++i) {
    datums[i].set_int(i);
  }

  buf = allocator_.alloc(sizeof(ObStorageDatum) *row_count);
  ASSERT_TRUE(buf != nullptr);
  datums = new (buf) ObStorageDatum[row_count];
  ObColumnVector datum_vec1;
  datum_vec1.type_ = ObColumnVectorType::DATUM_TYPE;
  datum_vec1.row_cnt_ = row_count;
  datum_vec1.datums_ = datums;
  for (int64_t i = 0; i < row_count; ++i) {
    datums[i].set_int(i);
  }

  ObColumnVector col_vectors[2];
  col_vectors[0] = datum_vec0;
  col_vectors[1] = datum_vec1;
  ObRowkeyVector rowkey_vector;
  rowkey_vector.columns_ = col_vectors;
  rowkey_vector.col_cnt_ = 2;
  rowkey_vector.row_cnt_ = row_count;
  rowkey_vector.is_datum_vectors_ = 1;

  buf = allocator_.alloc(sizeof(ObDiscreteDatumRowkey) *row_count);
  ASSERT_TRUE(buf != nullptr);
  rowkey_vector.discrete_rowkey_array_ = new (buf) ObDiscreteDatumRowkey[row_count];
  for (int64_t i = 0; i < row_count; ++i) {
    rowkey_vector.discrete_rowkey_array_[i].row_idx_ = i;
    rowkey_vector.discrete_rowkey_array_ [i].rowkey_vector_ = &rowkey_vector;
  }

  ObDatumRange range;
  ObDatumRowkey start_key, end_key;
  ObStorageDatum start_datums[2];
  ObStorageDatum end_datums[2];
  ret = start_key.assign(start_datums, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = end_key.assign(end_datums, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  range.set_start_key(start_key);
  range.set_end_key(end_key);

  bool is_left_border = true;
  bool is_right_border = true;
  bool is_normal_cg = false;
  int64_t begin_idx, end_idx;
  start_datums[0].set_min();
  start_datums[1].set_min();
  end_datums[0].set_max();
  end_datums[1].set_max();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 0);
  ASSERT_EQ(end_idx, row_count - 1);

  start_datums[0].set_int(1);
  start_datums[1].set_int(1);
  end_datums[0].set_int(2);
  end_datums[1].set_int(2);
  range.set_left_open();
  range.set_right_open();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 2);
  ASSERT_EQ(end_idx, 2);

  range.set_left_closed();
  range.set_right_closed();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 1);
  ASSERT_EQ(end_idx, 3);

  start_datums[1].set_min();
  end_datums[1].set_max();
  range.set_left_open();
  range.set_right_open();
  ret = rowkey_vector.locate_range(range, is_left_border, is_right_border, is_normal_cg, datum_utils, begin_idx, end_idx);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(begin_idx, 1);
  ASSERT_EQ(end_idx, 3);
}

TEST_F(ObDatumRowkeyVectorTest, rowkey_vector_compare_rowkey)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(2, datum_utils);

  int64_t row_count = 13;
  int64_t int_arr0[13] = {1,1,1,2,2,2,2,2,3,3,4,5,7};
  bool bool_arr0[13] = {false,false,false,false,false,false,false,false,false,false,false,false,false};
  int64_t int_arr1[13] = {2,3,4,1,1,1,2,3,4,5,6,6,8};
  bool bool_arr1[13] = {false,false,false,false,false,false,false,false,false,false,false,false,false};
  ObColumnVector int_vec0;
  int_vec0.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec0.row_cnt_ = row_count;
  int_vec0.has_null_ = false;
  int_vec0.signed_ints_ = int_arr0;
  int_vec0.nulls_ = bool_arr0;
  ObColumnVector int_vec1;
  int_vec1.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec1.row_cnt_ = row_count;
  int_vec1.has_null_ = false;
  int_vec1.signed_ints_ = int_arr1;
  int_vec1.nulls_ = bool_arr1;

  ObColumnVector col_vectors[2];
  col_vectors[0] = int_vec0;
  col_vectors[1] = int_vec1;
  ObRowkeyVector rowkey_vector;
  rowkey_vector.columns_ = col_vectors;
  rowkey_vector.col_cnt_ = 2;
  rowkey_vector.row_cnt_ = row_count;

  ObDatumRowkey curr_key;
  ObStorageDatum curr_key_datums[2];
  ret = curr_key.assign(curr_key_datums, 2);
  ASSERT_EQ(ret, OB_SUCCESS);

  int cmp_ret = 0;
  bool compare_datum_cnt = true;
  curr_key_datums[0].set_int(1);
  curr_key_datums[1].set_int(1);
  ret = rowkey_vector.compare_rowkey(curr_key, 0, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);

  curr_key_datums[0].set_int(1);
  curr_key_datums[1].set_int(2);
  ret = rowkey_vector.compare_rowkey(curr_key, 0, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret == 0);

  curr_key_datums[0].set_int(2);
  curr_key_datums[1].set_int(1);
  ret = rowkey_vector.compare_rowkey(curr_key, 2, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret < 0);

  ret = curr_key.assign(curr_key_datums, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  curr_key_datums[0].set_int(2);
  ret = rowkey_vector.compare_rowkey(curr_key, 3, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);
  compare_datum_cnt = false;
  ret = rowkey_vector.compare_rowkey(curr_key, 3, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret == 0);

  curr_key_datums[0].set_min();
  ret = rowkey_vector.compare_rowkey(curr_key, 3, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);
  curr_key_datums[0].set_max();
  ret = rowkey_vector.compare_rowkey(curr_key, 3, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret < 0);

  ObDiscreteDatumRowkey discrete_rowkey;
  discrete_rowkey.row_idx_ = 0;
  discrete_rowkey.rowkey_vector_ = &rowkey_vector;
  ret = rowkey_vector.compare_rowkey(discrete_rowkey, 0, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret == 0);

  discrete_rowkey.row_idx_ = 1;
  ret = rowkey_vector.compare_rowkey(discrete_rowkey, 0, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret < 0);

  discrete_rowkey.row_idx_ = 0;
  ret = rowkey_vector.compare_rowkey(discrete_rowkey, 1, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);

  ObRowkeyVector rowkey_vector1;
  rowkey_vector1.columns_ = col_vectors;
  rowkey_vector1.col_cnt_ = 2;
  rowkey_vector1.row_cnt_ = row_count;
  discrete_rowkey.row_idx_ = 3;
  discrete_rowkey.rowkey_vector_ = &rowkey_vector1;
  ret = rowkey_vector.compare_rowkey(discrete_rowkey, 1, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret < 0);

  ret = rowkey_vector.compare_rowkey(discrete_rowkey, 4, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret == 0);

  ret = rowkey_vector.compare_rowkey(discrete_rowkey, 6, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);

  int64_t int_val;
  ret = rowkey_vector.get_column_int(5, 0, int_val);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(int_val, 2);
}

TEST_F(ObDatumRowkeyVectorTest, rowkey_vector_compare_rowkey_datum)
{
  int ret = 0;
  ObStorageDatumUtils datum_utils;
  prepare_datum_util(2, datum_utils);

  int64_t row_count = 13;
  int64_t int_arr0[13] =   {1,1,1,2,2,2,2,2,3,3,4,5,7};
  bool bool_arr0[13] = {false,false,false,false,false,false,false,false,false,false,false,false,false};
  uint64_t uint_arr1[13] = {2,3,4,1,1,1,2,3,4,5,6,6,8};
  bool bool_arr1[13] = {false,false,false,false,false,false,false,false,false,false,false,false,false};
  ObColumnVector int_vec0;
  int_vec0.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec0.row_cnt_ = row_count;
  int_vec0.has_null_ = false;
  int_vec0.signed_ints_ = int_arr0;
  int_vec0.nulls_ = bool_arr0;
  ObColumnVector uint_vec1;
  uint_vec1.type_ = ObColumnVectorType::UNSIGNED_INTEGER_TYPE;
  uint_vec1.row_cnt_ = row_count;
  uint_vec1.has_null_ = false;
  uint_vec1.unsigned_ints_ = uint_arr1;
  uint_vec1.nulls_ = bool_arr1;

  ObColumnVector col_vectors1[2];
  col_vectors1[0] = int_vec0;
  col_vectors1[1] = uint_vec1;
  ObRowkeyVector rowkey_vector1;
  rowkey_vector1.columns_ = col_vectors1;
  rowkey_vector1.col_cnt_ = 2;
  rowkey_vector1.row_cnt_ = row_count;

  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * row_count);
  ASSERT_TRUE(buf != nullptr);
  ObStorageDatum *datums = new (buf) ObStorageDatum[row_count];
  ObColumnVector datum_vec0;
  datum_vec0.type_ = ObColumnVectorType::DATUM_TYPE;
  datum_vec0.row_cnt_ = row_count;
  datum_vec0.datums_ = datums;
  for (int64_t i = 0; i < row_count; ++i) {
    datums[i].set_int(i);
  }

  buf = allocator_.alloc(sizeof(ObStorageDatum) *row_count);
  ASSERT_TRUE(buf != nullptr);
  datums = new (buf) ObStorageDatum[row_count];
  ObColumnVector datum_vec1;
  datum_vec1.type_ = ObColumnVectorType::DATUM_TYPE;
  datum_vec1.row_cnt_ = row_count;
  datum_vec1.datums_ = datums;
  for (int64_t i = 0; i < row_count; ++i) {
    datums[i].set_uint(i);
  }

  ObColumnVector col_vectors2[2];
  col_vectors2[0] = datum_vec0;
  col_vectors2[1] = datum_vec1;
  ObRowkeyVector rowkey_vector2;
  rowkey_vector2.columns_ = col_vectors2;
  rowkey_vector2.col_cnt_ = 2;
  rowkey_vector2.row_cnt_ = row_count;
  rowkey_vector2.is_datum_vectors_ = 1;

  ObDiscreteDatumRowkey discrete_rowkey;
  discrete_rowkey.row_idx_ = 0;
  discrete_rowkey.rowkey_vector_ = &rowkey_vector2;
  int cmp_ret = 0;
  bool compare_datum_cnt = true;
  ret = rowkey_vector1.compare_rowkey(discrete_rowkey, 0, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);

  discrete_rowkey.row_idx_ = 2;
  ret = rowkey_vector1.compare_rowkey(discrete_rowkey, 6, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret == 0);

  discrete_rowkey.row_idx_ = 3;
  ret = rowkey_vector1.compare_rowkey(discrete_rowkey, 3, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret < 0);

  discrete_rowkey.row_idx_ = 12;
  discrete_rowkey.rowkey_vector_ = &rowkey_vector1;
  ret = rowkey_vector2.compare_rowkey(discrete_rowkey, 9, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);

  discrete_rowkey.row_idx_ = 6;
  ret = rowkey_vector2.compare_rowkey(discrete_rowkey, 2, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret == 0);

  discrete_rowkey.row_idx_ = 8;
  ret = rowkey_vector2.compare_rowkey(discrete_rowkey, 3, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret < 0);

  discrete_rowkey.row_idx_ = 12;
  discrete_rowkey.rowkey_vector_ = &rowkey_vector2;
  for (int64_t i = 0; i < row_count; ++i) {
    datum_vec0.datums_[i].set_null();
  }
  ret = rowkey_vector1.compare_rowkey(discrete_rowkey, 0, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);

  bool_arr0[0] = bool_arr0[1] = true;
  discrete_rowkey.row_idx_ = 1;
  discrete_rowkey.rowkey_vector_ = &rowkey_vector1;
  ret = rowkey_vector2.compare_rowkey(discrete_rowkey, 3, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret == 0);

  discrete_rowkey.row_idx_ = 1;
  ret = rowkey_vector2.compare_rowkey(discrete_rowkey, 4, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret > 0);

  discrete_rowkey.row_idx_ = 1;
  ret = rowkey_vector2.compare_rowkey(discrete_rowkey, 1, datum_utils, cmp_ret, compare_datum_cnt);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(cmp_ret < 0);
}

TEST_F(ObDatumRowkeyVectorTest, rowkey_vector_deep_copy)
{
  int ret = 0;
  ObRowkeyVector rowkey_vector;
  ObColumnVector col_vectors[2];

  int64_t row_count = 6;
  int64_t int_arr0[6] = {1,2,3,4,5,6};
  bool bool_arr0[6] = {false,false,false,false,false,false};
  int64_t int_arr1[6] = {2,3,4,1,1,1};
  bool bool_arr1[6] = {false,true,false,true,false,true};
  ObColumnVector int_vec0;
  int_vec0.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec0.row_cnt_ = row_count;
  int_vec0.has_null_ = false;
  int_vec0.signed_ints_ = int_arr0;
  int_vec0.nulls_ = bool_arr0;
  int_vec0.is_filled_ = true;
  ObColumnVector int_vec1;
  int_vec1.type_ = ObColumnVectorType::SIGNED_INTEGER_TYPE;
  int_vec1.row_cnt_ = row_count;
  int_vec1.has_null_ = false;
  int_vec1.signed_ints_ = int_arr1;
  int_vec1.nulls_ = bool_arr1;
  int_vec1.is_filled_ = true;

  col_vectors[0] = int_vec0;
  col_vectors[1] = int_vec1;
  rowkey_vector.columns_ = col_vectors;
  rowkey_vector.col_cnt_ = 2;
  rowkey_vector.row_cnt_ = row_count;

  int64_t buf_size = 0;
  ObObjMeta obj_meta;
  obj_meta.set_type_simple(ObIntType);
  ObColDesc col_desc;
  col_desc.col_type_ = obj_meta;
  ObSEArray<ObColDesc, 2> col_descs;
  ASSERT_EQ(col_descs.push_back(col_desc), OB_SUCCESS);
  ASSERT_EQ(col_descs.push_back(col_desc), OB_SUCCESS);
  ret = ObRowkeyVector::get_occupied_size(row_count, 2, &col_descs, buf_size);
  ASSERT_EQ(ret, OB_SUCCESS);
  int64_t expected_size = sizeof(ObRowkeyVector) // 1 rowkey vector
                        + 2 * sizeof(ObColumnVector) // 2 column vector
                        + 2 * row_count * (sizeof(int64_t) + sizeof(bool)) // 2 int column vector
                        + sizeof(ObDatumRowkey) + sizeof(ObStorageDatum) * 2 // last rowkey
                        + sizeof(ObDiscreteDatumRowkey) * row_count; // discrete datum rowkey array
  ASSERT_EQ(buf_size, expected_size);

  void *buf = allocator_.alloc(buf_size);
  ASSERT_TRUE(buf != nullptr);
  ObRowkeyVector new_vector;
  int64_t pos = 0;
  ret = new_vector.deep_copy((char*)buf, pos, buf_size, rowkey_vector);
  STORAGE_LOG(INFO, "deep copy", K(ret), K(new_vector));
  ASSERT_EQ(ret, OB_SUCCESS);

  const ObDatumRowkey *last_rowkey = new_vector.get_last_rowkey();
  ASSERT_TRUE(last_rowkey != nullptr);
  ASSERT_TRUE(last_rowkey->is_valid());
  ASSERT_EQ(6, last_rowkey->datums_[0].get_int());
  ASSERT_TRUE(last_rowkey->datums_[1].is_null());
  ASSERT_TRUE(nullptr != new_vector.discrete_rowkey_array_);
  ASSERT_EQ(0, new_vector.discrete_rowkey_array_[0].row_idx_);
  ASSERT_EQ(&new_vector, new_vector.discrete_rowkey_array_[0].rowkey_vector_);
  ASSERT_EQ(5, new_vector.discrete_rowkey_array_[5].row_idx_);
  ASSERT_EQ(&new_vector, new_vector.discrete_rowkey_array_[5].rowkey_vector_);

  ObCommonDatumRowkey endkey;
  ret = new_vector.get_rowkey(3, endkey);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(endkey.is_valid());
  ASSERT_TRUE(endkey.is_discrete_rowkey());
  ASSERT_EQ(3, endkey.get_discrete_rowkey()->row_idx_);
  ASSERT_EQ(&new_vector, endkey.get_discrete_rowkey()->rowkey_vector_);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_datum_rowkey_vector.log*");
  OB_LOGGER.set_file_name("test_datum_rowkey_vector.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

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
#include "test_column_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class TestConstDecoder : public TestColumnDecoder
{
public:
  TestConstDecoder() : TestColumnDecoder(ObColumnHeader::Type::CONST) {}
  virtual ~TestConstDecoder() {}
};

TEST_F(TestConstDecoder, no_exception_nu_nn)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    row.storage_datums_[i].set_null();
  }
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);
  ObMalloc mallocer;
  mallocer.set_label("ConstDecoder");
  ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);

  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, row));
  }

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    white_filter.op_type_ = sql::WHITE_OP_NU;
    int32_t col_idx = i;
    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NN;
    result_bitmap.reuse();
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
  }
}

TEST_F(TestConstDecoder, no_exception_other)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed_1 = 1;
  int64_t seed_2 = 2;
  int64_t seed_3 = 3;
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed_2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  // Dedcode and filter_push_down
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("ConstDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1, ref_obj_2, ref_obj_3;
    setup_obj(ref_obj_1, i, seed_1);
    setup_obj(ref_obj_2, i, seed_2);
    setup_obj(ref_obj_3, i, seed_3);

    objs.push_back(ref_obj_2);
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_EQ;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_NE;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_LT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_GT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_LE;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_GE;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64, result_bitmap.popcnt());

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_IN;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64, result_bitmap.popcnt());

    objs.reuse();
    objs.init(2);
    objs.push_back(ref_obj_1);
    objs.push_back(ref_obj_3);

    result_bitmap.reuse();
    white_filter.op_type_ = sql::WHITE_OP_BT;
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64, result_bitmap.popcnt());
  }
}

TEST_F(TestConstDecoder, filter_push_down_nu_nn_eq_ne)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed_1 = 0x1;
  int64_t seed_2 = 0x2;
  for (int64_t i = 0; i < ROW_CNT - 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed_1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 3; i < ROW_CNT - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed_2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 1; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  int64_t seed1_count = ROW_CNT - 3;
  int64_t seed2_count = 2;
  int64_t null_count = 1;

  char* buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;
  sql::ObPushdownWhiteFilterNode white_filter(allocator_);

  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, decoder.get_row(i, row));
  }

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }

    ObMalloc mallocer;
    mallocer.set_label("ConstDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);
    ObObj ref_obj_1, ref_obj_2;
    setup_obj(ref_obj_1, i, seed_1);
    setup_obj(ref_obj_2, i, seed_2);
    objs.push_back(ref_obj_1);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_EQ;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    objs.pop_back();
    objs.push_back(ref_obj_2);
    white_filter.op_type_ = sql::WHITE_OP_EQ;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NU;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(null_count, result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NN;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
  }
}

TEST_F(TestConstDecoder, filter_push_down_gt_lt_ge_le)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0;
  int64_t seed1 = 2;
  int64_t seed2 = 4;
  for (int64_t i = 0; i < ROW_CNT - 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 5; i < ROW_CNT - 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 2; i < ROW_CNT - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 1; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    LOG_INFO("Null row appended: ", K(row));
  }

  int64_t seed0_count = ROW_CNT - 5;
  int64_t seed1_count = 3;
  int64_t seed2_count = 1;
  int64_t null_count = 1;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    // Test with reference object as non-const value
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("ConstDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    ObObj ref_obj_0, ref_obj_1;
    setup_obj(ref_obj_0, i, seed0);
    setup_obj(ref_obj_1, i, seed1);

    objs.push_back(ref_obj_1);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_GT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    // Greater Than
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());

    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(ROW_CNT - seed0_count - null_count, result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());
  }

  for (int64_t i = 0; i < full_column_cnt_ - 1; ++i) {
    // Test with reference object as const value
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("ConstDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 1);
    objs.init(1);

    ObObj ref_obj_0, ref_obj_1;
    setup_obj(ref_obj_0, i, seed0);
    setup_obj(ref_obj_1, i, seed1);

    objs.push_back(ref_obj_0);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_GT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    // Greater Than
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count + seed1_count, result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());

    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64 - null_count, result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());
  }
}

TEST_F(TestConstDecoder, filter_push_down_bt)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0;
  int64_t seed1 = 1;
  int64_t seed2 = 2;
  int64_t seed3 = 3;
  for (int64_t i = 0; i < ROW_CNT - 63; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 63; i < ROW_CNT - 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 5; i < ROW_CNT - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 1; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed3, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  int64_t seed0_count = ROW_CNT - 63;
  int64_t seed1_count = ROW_CNT - 5 - seed0_count;
  int64_t seed2_count = 4;
  int64_t seed3_count = 1;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    ObMalloc mallocer;
    mallocer.set_label("ConstDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 2);
    objs.init(2);

    ObObj ref_obj_1;
    setup_obj(ref_obj_1, i, seed1);
    ObObj ref_obj_2;
    setup_obj(ref_obj_2, i, seed2);
    ObObj ref_obj_3;
    setup_obj(ref_obj_3, i, seed3);

    // const value in range
    objs.push_back(ref_obj_2);
    objs.push_back(ref_obj_3);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_BT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count + seed3_count, result_bitmap.popcnt());

    // const value not in range
    objs.reuse();
    objs.init(2);
    objs.push_back(ref_obj_1);
    objs.push_back(ref_obj_3);

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count + seed3_count, result_bitmap.popcnt());
  }
}

TEST_F(TestConstDecoder, filter_push_down_in)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));

  int64_t seed0 = 0x0;
  int64_t seed1 = 0x1;
  int64_t seed2 = 0x2;
  int64_t seed3 = 0x3;
  int64_t seed5 = 0x5;

  for (int64_t i = 0; i < ROW_CNT - 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 4; i < ROW_CNT - 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 3; i < ROW_CNT - 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed2, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }
  for (int64_t i = ROW_CNT - 2; i < ROW_CNT - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed3, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }


  for (int64_t j = 0; j < full_column_cnt_; ++j) {
    row.storage_datums_[j].set_null();
  }
  for (int64_t i = ROW_CNT - 1; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    LOG_INFO("Null row appended: ", K(row));
  }

  int64_t seed0_count = ROW_CNT - 4;
  int64_t seed1_count = 1;
  int64_t seed2_count = 1;
  int64_t seed3_count = 1;
  int64_t null_count = 1;
  int64_t seed5_count = 0;

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_)) << "buffer size: " << data.get_buf_size() << std::endl;

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    if (i >= rowkey_cnt_ && i < read_info_.get_rowkey_count()) {
      continue;
    }
    sql::ObPushdownWhiteFilterNode white_filter(allocator_);
    sql::ObPushdownWhiteFilterNode white_filter_2(allocator_);

    ObMalloc mallocer;
    mallocer.set_label("ConstDecoder");
    ObFixedArray<ObObj, ObIAllocator> objs(mallocer, 3);
    objs.init(3);

    ObObj ref_obj0;
    setup_obj(ref_obj0, i, seed0);
    ObObj ref_obj1;
    setup_obj(ref_obj1, i, seed1);
    ObObj ref_obj2;
    setup_obj(ref_obj2, i, seed2);
    ObObj ref_obj5;
    setup_obj(ref_obj5, i, seed5);

    objs.push_back(ref_obj1);
    objs.push_back(ref_obj2);
    objs.push_back(ref_obj5);

    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_IN;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());


    objs.reuse();
    objs.init(3);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    objs.push_back(ref_obj5);
    white_filter_2.op_type_ = sql::WHITE_OP_IN;

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter_2, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
  }
}

TEST_F(TestConstDecoder, batch_decode_to_datum_test_without_expection)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;

  for (int64_t i = 0; i < ROW_CNT ; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.get_data().data(), encoder_.get_data().pos());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  const ObRowHeader *row_header = nullptr;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  const char *cell_datas[ROW_CNT];
  void *datum_buf_1 = allocator_.alloc(sizeof(int8_t) * 128 * ROW_CNT);
  void *datum_buf_2 = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    int32_t col_offset = i;
    STORAGE_LOG(INFO, "Current col: ", K(i),K(col_descs_.at(i)), K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int64_t row_ids[ROW_CNT];
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf_1) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
        .batch_decode(decoder.row_index_, row_ids, cell_datas, ROW_CNT, datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      ObObj obj;
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(row_ids[j], row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(obj, row_ids[j], bs, row_data, row_len));
      STORAGE_LOG(INFO, "row: ", K(j), K(obj));
      ObObj obj_cast_from_datum;
      ASSERT_EQ(OB_SUCCESS, datums[j].to_obj(obj_cast_from_datum, col_descs_.at(i).col_type_));
      ASSERT_EQ(obj, obj_cast_from_datum);
      ObDatum datum_cast_from_obj;
      datum_cast_from_obj.ptr_ = reinterpret_cast<char *>(datum_buf_2);
      ASSERT_EQ(OB_SUCCESS, datum_cast_from_obj.from_obj(obj));
      ASSERT_TRUE(ObDatum::binary_equal(datum_cast_from_obj, datums[j]));
    }
  }
}

TEST_F(TestConstDecoder, batch_decode_to_datum_test_with_expection)
{
  batch_decode_to_datum_test();
}

// TEST_F(TestConstDecoder, batch_get_row_perf_test)
// {
//   batch_get_row_perf_test();
// }

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_const_decoder.log*");
  OB_LOGGER.set_file_name("test_const_decoder.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

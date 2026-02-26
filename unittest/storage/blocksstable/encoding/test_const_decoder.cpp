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
private:
  void batch_decode_to_vector_no_exc_test(const VectorFormat vector_format);
};

void TestConstDecoder::batch_decode_to_vector_no_exc_test(const VectorFormat vector_format)
{
  FLOG_INFO("start one batch decode to vector no exception test", K(vector_format));
  ObArenaAllocator test_allocator;
  encoder_.reuse();
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(test_allocator, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(test_allocator, full_column_cnt_));
  int64_t seed0 = 10000;

  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));

  ObArenaAllocator frame_allocator;
  sql::ObExecContext exec_context(test_allocator);
  sql::ObEvalCtx eval_ctx(exec_context);
  const char *ptr_arr[ROW_CNT];
  uint32_t len_arr[ROW_CNT];
  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    bool need_test_column = true;
    ObObjMeta col_meta = col_descs_.at(i).col_type_;
    const int16_t precision = col_meta.is_decimal_int() ? col_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
    VecValueTypeClass vec_tc = common::get_vec_value_tc(
        col_meta.get_type(),
        col_meta.get_scale(),
        precision);
    if (i >= ROWKEY_CNT && i < read_info_.get_rowkey_count()) {
      need_test_column = false;
    } else {
      need_test_column = VectorDecodeTestUtil::need_test_vec_with_type(vector_format, vec_tc);
    }

    if (!need_test_column) {
      continue;
    }

    sql::ObExpr col_expr;
    ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
        ROW_CNT, col_meta, vector_format, eval_ctx, col_expr, frame_allocator));
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_meta),  K(*decoder.decoders_[col_offset].ctx_), K(precision), K(vec_tc));

    int32_t row_ids[ROW_CNT];
    for (int32_t datum_idx = 0; datum_idx < ROW_CNT; ++datum_idx) {
      row_ids[datum_idx] = datum_idx;
    }

    ObVectorDecodeCtx vector_ctx(ptr_arr, len_arr, row_ids, ROW_CNT, 0, col_expr.get_vector_header(eval_ctx));
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset].decode_vector(decoder.row_index_, vector_ctx));
    for (int64_t vec_idx = 0; vec_idx < ROW_CNT; ++vec_idx) {
      ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match(*(col_expr.get_vector_header(eval_ctx).get_vector()),
          vec_idx, rows[vec_idx].storage_datums_[col_offset]));
    }
  }
}

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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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

    // 0 --- ROW_CNT-3 --- ROW_CNT-1 --- ROW_CNT
    // |    seed1   |   seed2    |     null   |
    //     |            |
    // ROW_CNT-32   ROW_CNT-2
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_EQ;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 32, ROW_CNT - 2, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(29, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 32, ROW_CNT - 2, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(1, pd_result_bitmap.popcnt());

    objs.pop_back();
    objs.push_back(ref_obj_2);
    white_filter.op_type_ = sql::WHITE_OP_EQ;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 32, ROW_CNT - 2, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(1, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 32, ROW_CNT - 2, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(29, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NU;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(null_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 32, ROW_CNT - 2, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    white_filter.op_type_ = sql::WHITE_OP_NN;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 32, ROW_CNT - 2, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(30, pd_result_bitmap.popcnt());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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

    // 0 --- ROW_CNT-5 --- ROW_CNT-2  --- ROW_CNT-1 --- ROW_CNT
    // |    seed0 |    seed1    |   seed2      |     null     |
    //     |               |
    // ROW_CNT-33      ROW_CNT-3
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_GT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    // Greater Than
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(28, pd_result_bitmap.popcnt());

    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(ROW_CNT - seed0_count - null_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(1 + seed2_count, pd_result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count + seed1_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(30, pd_result_bitmap.popcnt());
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
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    // Greater Than
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count + seed1_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(2, pd_result_bitmap.popcnt());

    // Less Than
    white_filter.op_type_ = sql::WHITE_OP_LT;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(0, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    // Greater than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_GE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(64 - null_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(30, pd_result_bitmap.popcnt());

    // Less than or Equal to
    white_filter.op_type_ = sql::WHITE_OP_LE;
    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed0_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(28, pd_result_bitmap.popcnt());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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

    // 0 --- ROW_CNT-63 --- ROW_CNT-5  --- ROW_CNT-1 --- ROW_CNT
    // |  seed0 |    seed1        |   seed2    |   seed3   |
    //                 |                 |
    //             ROW_CNT-33          ROW_CNT-3
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_BT;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed2_count + seed3_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(2, pd_result_bitmap.popcnt());

    // const value not in range
    objs.reuse();
    objs.init(2);
    objs.push_back(ref_obj_1);
    objs.push_back(ref_obj_3);

    result_bitmap.reuse();
    ASSERT_EQ(0, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count + seed3_count, result_bitmap.popcnt());
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(28 + 2, pd_result_bitmap.popcnt());
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
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
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

    // 0 --- ROW_CNT-4 --- ROW_CNT-3  --- ROW_CNT-2 --- ROW_CNT-1 --- ROW_CNT
    // |  seed0 |    seed1        |   seed2    |   seed3   |     null       |
    //     |                     |
    //   ROW_CNT-33          ROW_CNT-3
    int32_t col_idx = i;
    white_filter.op_type_ = sql::WHITE_OP_IN;

    ObBitmap result_bitmap(allocator_);
    result_bitmap.init(ROW_CNT);
    ASSERT_EQ(0, result_bitmap.popcnt());
    ObBitmap pd_result_bitmap(allocator_);
    pd_result_bitmap.init(30);
    ASSERT_EQ(0, pd_result_bitmap.popcnt());

    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown(col_idx, false, decoder, white_filter, result_bitmap, objs));
    ASSERT_EQ(seed1_count + seed2_count, result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(seed1_count, pd_result_bitmap.popcnt());


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
    pd_result_bitmap.reuse();
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
    ASSERT_EQ(OB_SUCCESS, test_filter_pushdown_with_pd_info(ROW_CNT - 33, ROW_CNT - 3, col_idx, false, decoder, white_filter, pd_result_bitmap, objs));
    ASSERT_EQ(0, pd_result_bitmap.popcnt());
  }
}

TEST_F(TestConstDecoder, batch_decode_to_datum_test_without_expection)
{
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;

  for (int64_t i = 0; i < ROW_CNT ; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  const ObRowHeader *row_header = nullptr;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  const char *cell_datas[ROW_CNT];
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128 * ROW_CNT);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    int32_t col_offset = i;
    STORAGE_LOG(INFO, "Current col: ", K(i),K(col_descs_.at(i)), K(*decoder.decoders_[col_offset].ctx_));
    ObDatum datums[ROW_CNT];
    int32_t row_ids[ROW_CNT];
    for (int32_t j = 0; j < ROW_CNT; ++j) {
      datums[j].ptr_ = reinterpret_cast<char *>(datum_buf) + j * 128;
      row_ids[j] = j;
    }
    ASSERT_EQ(OB_SUCCESS, decoder.decoders_[col_offset]
        .batch_decode(decoder.row_index_, row_ids, cell_datas, ROW_CNT, datums));
    for (int64_t j = 0; j < ROW_CNT; ++j) {
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[i]), K(datums[j]));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[i], datums[j]));
    }
  }
}

TEST_F(TestConstDecoder, batch_decode_to_datum_test_with_expection)
{
  batch_decode_to_datum_test();
}

TEST_F(TestConstDecoder, batch_decode_to_vector_test)
{
  #define TEST_ONE_WITH_ALIGN(row_aligned, vec_format) \
  batch_decode_to_vector_test(false, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(false, false, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, true, row_aligned, vec_format); \
  batch_decode_to_vector_test(true, false, row_aligned, vec_format);

  #define TEST_ONE(vec_format) \
  TEST_ONE_WITH_ALIGN(true, vec_format) \
  TEST_ONE_WITH_ALIGN(false, vec_format)

  TEST_ONE(VEC_UNIFORM);
  TEST_ONE(VEC_FIXED);
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestConstDecoder, batch_decode_to_vector_no_exc_test)
{
  batch_decode_to_vector_no_exc_test(VEC_UNIFORM);
  batch_decode_to_vector_no_exc_test(VEC_FIXED);
  batch_decode_to_vector_no_exc_test(VEC_DISCRETE);
  batch_decode_to_vector_no_exc_test(VEC_CONTINUOUS);
}

TEST_F(TestConstDecoder, cell_decode_to_datum_test_without_expection)
{
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  int64_t seed0 = 10000;
  int64_t row_cnt = 30;
  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed0, row));
    row.storage_datums_[0].set_null();
    row.storage_datums_[1].set_nop();
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, allocator_);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  int64_t row_len = 0;
  const char *row_data = nullptr;
  void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128);

  for (int64_t i = 0; i < full_column_cnt_; ++i) {
    int32_t col_offset = i;
    LOG_INFO("Current col: ", K(i), K(col_descs_.at(i)),  K(*decoder.decoders_[col_offset].ctx_));
    for (int64_t j = 0; j < row_cnt; ++j) {
      ObDatum datum;
      datum.ptr_ = reinterpret_cast<char *>(datum_buf);
      ASSERT_EQ(OB_SUCCESS, decoder.row_index_->get(j, row_data, row_len));
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      ASSERT_EQ(OB_SUCCESS,
          decoder.decoders_[col_offset].decode(datum,j, bs, row_data, row_len));
      LOG_INFO("Current row: ", K(j), K(col_offset), K(rows[j].storage_datums_[col_offset]), K(datum));
      ASSERT_TRUE(ObDatum::binary_equal(rows[j].storage_datums_[col_offset], datum));
    }
  }
}

TEST_F(TestConstDecoder, cell_decode_to_datum_test_with_expection)
{
  cell_decode_to_datum_test();
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

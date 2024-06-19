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
#include "test_column_decoder.h"
#define protected public
#define private public


namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;

class TestColumnEqualMicroDecoder : public TestColumnDecoder
{
public:
  TestColumnEqualMicroDecoder() : TestColumnDecoder(ObColumnHeader::Type::COLUMN_EQUAL) {}
  virtual ~TestColumnEqualMicroDecoder() {}
};

TEST_F(TestColumnEqualMicroDecoder, small_uint_with_large_exception)
{
  ObArenaAllocator test_allocator;
  encoder_.reuse();
  void *row_buf = allocator_.alloc(sizeof(ObDatumRow) * ROW_CNT);
  ObDatumRow *rows = new (row_buf) ObDatumRow[ROW_CNT];
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(test_allocator, full_column_cnt_));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(test_allocator, full_column_cnt_));

  int64_t seed0 = UINT32_MAX;
  int64_t seed1 = 100001;

  for (int64_t i = 0; i < ROW_CNT; ++i) {
    int64_t seed = (i == (ROW_CNT - 1)) ? seed0 : seed1;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(seed1, row));
    for (int64_t j = read_info_.get_rowkey_count() + 1; j < full_column_cnt_; j += 2) {
      row.storage_datums_[j] = row.storage_datums_[j - 1];
      if (i == (ROW_CNT - 1) && (ob_obj_type_class(col_descs_.at(j).col_type_.get_type()) == common::ObUIntTC)) {
        row.storage_datums_[j - 1].set_uint(UINT32_MAX);
      }
    }
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row)) << "i: " << i << std::endl;
    rows[i].deep_copy(row, test_allocator);
  }

  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, encoder_.build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data(encoder_.data_buffer_.data(), encoder_.data_buffer_.length());
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));


  const VectorFormat vector_format = VEC_UNIFORM;
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

} // blocksstable
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_column_equal_decoder.log*");
  OB_LOGGER.set_file_name("test_column_equal_decoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

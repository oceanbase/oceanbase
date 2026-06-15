/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "test_column_decoder.h"
#define protected public
#define private public

#define PUSHDOWN_GENERAL_TEST(x) \
            TEST_F(x, basic_filter_pushdown_op_test_eq_ne_nu_nn) { basic_filter_pushdown_eq_ne_nu_nn_test(); } \
            TEST_F(x, basic_filter_pushdown_op_test_comparison) { basic_filter_pushdown_comparison_test(); } \
            TEST_F(x, basic_filter_pushdown_op_test_in) { basic_filter_pushdown_in_op_test(); } \
            TEST_F(x, basic_filter_pushdown_op_test_bt) { basic_filter_pushdown_bt_test(); }

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

class TestDictDecoder : public TestColumnDecoder
{
public:
  TestDictDecoder() : TestColumnDecoder(ObColumnHeader::Type::DICT) {}
  virtual ~TestDictDecoder() {}
};

class TestRLEDecoder : public TestColumnDecoder
{
public:
  TestRLEDecoder() : TestColumnDecoder(ObColumnHeader::Type::RLE) {}
  virtual ~TestRLEDecoder() {}
};

class TestIntBaseDiffDecoder : public TestColumnDecoder
{
public:
  TestIntBaseDiffDecoder() : TestColumnDecoder(ObColumnHeader::Type::INTEGER_BASE_DIFF) {}
  virtual ~TestIntBaseDiffDecoder() {}
};

class TestRetroPDDecoder : public TestColumnDecoder
{
public:
  TestRetroPDDecoder() : TestColumnDecoder(true) {}
  virtual ~TestRetroPDDecoder() {}
};

class TestHexDecoder : public TestColumnDecoder
{
public:
  TestHexDecoder() : TestColumnDecoder(ObColumnHeader::Type::HEX_PACKING) {}
  virtual ~TestHexDecoder() {}
};

class TestStringDiffDecoder : public TestColumnDecoder
{
public:
  TestStringDiffDecoder() : TestColumnDecoder(ObColumnHeader::Type::STRING_DIFF) {}
  virtual ~TestStringDiffDecoder() {}
};

class TestStringPrefixDecoder : public TestColumnDecoder
{
public:
  TestStringPrefixDecoder() : TestColumnDecoder(ObColumnHeader::Type::STRING_PREFIX) {}
  virtual ~TestStringPrefixDecoder() {}
};

class TestColumnEqualDecoder : public TestColumnDecoder
{
public:
  TestColumnEqualDecoder() : TestColumnDecoder(ObColumnHeader::Type::COLUMN_EQUAL) {}
  virtual ~TestColumnEqualDecoder() {}
};

class TestInterColumnSubstringDecoder : public TestColumnDecoder
{
public:
  TestInterColumnSubstringDecoder() : TestColumnDecoder(ObColumnHeader::Type::COLUMN_SUBSTR) {}
  virtual ~TestInterColumnSubstringDecoder() {}
};

class MockPaxBlackFilterExecutor : public sql::ObBlackFilterExecutor
{
public:
  MockPaxBlackFilterExecutor(
      common::ObIAllocator &allocator,
      sql::ObPushdownBlackFilterNode &filter_node,
      sql::ObPushdownOperator &op,
      ObStorageDatum *datums,
      const ObObjMeta &obj_meta)
    : sql::ObBlackFilterExecutor(allocator, filter_node, &op),
      datums_(datums),
      get_datums_call_count_(0),
      filter_batch_call_count_(0)
  {
    expr_.obj_meta_ = obj_meta;
    expr_.datum_meta_.type_ = obj_meta.get_type();
  }

  int get_datums_from_column(common::ObIArray<blocksstable::ObSqlDatumInfo> &datum_infos) override
  {
    int ret = OB_SUCCESS;
    ++get_datums_call_count_;
    datum_infos.reuse();
    if (OB_FAIL(datum_infos.push_back(blocksstable::ObSqlDatumInfo(datums_, &expr_)))) {
      LOG_WARN("failed to push datum info", K(ret));
    }
    return ret;
  }

  int filter(ObEvalCtx &, const sql::ObBitVector &, bool &filtered) override
  {
    filtered = false;
    return OB_SUCCESS;
  }

  int filter_batch(
      sql::ObPushdownFilterExecutor *parent,
      const int64_t start,
      const int64_t end,
      common::ObBitmap &result_bitmap) override
  {
    UNUSEDx(parent, start, end, result_bitmap);
    ++filter_batch_call_count_;
    return OB_SUCCESS;
  }

  ObStorageDatum *datums_;
  sql::ObExpr expr_;
  int64_t get_datums_call_count_;
  int64_t filter_batch_call_count_;
};

TEST_F(TestIntBaseDiffDecoder, filter_pushdown_comaprison_neg_test)
{
  filter_pushdown_comaprison_neg_test();
}

TEST_F(TestDictDecoder, black_filter_pushdown_disabled_for_encoding_row_store)
{
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10000 + (i % 4), row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row));
  }

  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data;
  ASSERT_EQ(OB_SUCCESS, data.init_with_prepare_micro_header(encoder_.data_buffer_.data(), encoder_.data_buffer_.length()));
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  ASSERT_EQ(common::ENCODING_ROW_STORE, static_cast<common::ObRowStoreType>(decoder.micro_header_->row_store_type_));

  int64_t dict_col_idx = -1;
  for (int64_t i = read_info_.get_rowkey_count(); i < full_column_cnt_; ++i) {
    if (decoder.decoders_[i].ctx_->col_header_->type_ == ObColumnHeader::DICT) {
      dict_col_idx = i;
      break;
    }
  }
  ASSERT_NE(-1, dict_col_idx);

  sql::PushdownFilterInfo pd_filter_info;
  pd_filter_info.start_ = 0;
  pd_filter_info.count_ = decoder.row_count_;
  pd_filter_info.batch_size_ = 2;
  pd_filter_info.allocator_ = &allocator_;
  pd_filter_info.cell_data_ptrs_ = static_cast<const char **>(allocator_.alloc(sizeof(const char *) * pd_filter_info.batch_size_));
  ASSERT_NE(nullptr, pd_filter_info.cell_data_ptrs_);

  ObBitmap result_bitmap(allocator_);
  ASSERT_EQ(OB_SUCCESS, result_bitmap.init(pd_filter_info.count_));

  ObStorageDatum datum_buf[2];
  sql::ObPushdownBlackFilterNode filter_node(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  MockPaxBlackFilterExecutor filter(allocator_, filter_node, op, datum_buf, col_descs_.at(dict_col_idx).col_type_);
  ASSERT_EQ(OB_SUCCESS, filter.col_offsets_.init(1));
  ASSERT_EQ(OB_SUCCESS, filter.col_params_.init(1));
  ASSERT_EQ(OB_SUCCESS, filter.col_offsets_.push_back(dict_col_idx));
  ASSERT_EQ(OB_SUCCESS, filter.col_params_.push_back(nullptr));
  filter.n_cols_ = 1;

  bool filter_applied = true;
  ASSERT_EQ(OB_SUCCESS, decoder.filter_black_filter_batch(nullptr, filter, pd_filter_info, result_bitmap, filter_applied));
  ASSERT_FALSE(filter_applied);
  ASSERT_EQ(0, filter.get_datums_call_count_);
  ASSERT_EQ(0, filter.filter_batch_call_count_);
}

TEST_F(TestDictDecoder, black_filter_pushdown_disabled_for_selective_encoding_row_store)
{
  encoder_.reuse();
  ctx_.row_store_type_ = common::SELECTIVE_ENCODING_ROW_STORE;
  ASSERT_EQ(OB_SUCCESS, encoder_.init(ctx_));

  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  for (int64_t i = 0; i < ROW_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(10000 + (i % 4), row));
    ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row));
  }

  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, build_block(buf, size));

  ObMicroBlockDecoder decoder;
  ObMicroBlockData data;
  ASSERT_EQ(OB_SUCCESS, data.init_with_prepare_micro_header(encoder_.data_buffer_.data(), encoder_.data_buffer_.length()));
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  ASSERT_EQ(common::SELECTIVE_ENCODING_ROW_STORE, static_cast<common::ObRowStoreType>(decoder.micro_header_->row_store_type_));

  int64_t dict_col_idx = -1;
  for (int64_t i = read_info_.get_rowkey_count(); i < full_column_cnt_; ++i) {
    if (decoder.decoders_[i].ctx_->col_header_->type_ == ObColumnHeader::DICT) {
      dict_col_idx = i;
      break;
    }
  }
  ASSERT_NE(-1, dict_col_idx);

  sql::PushdownFilterInfo pd_filter_info;
  pd_filter_info.start_ = 0;
  pd_filter_info.count_ = decoder.row_count_;
  pd_filter_info.batch_size_ = 2;
  pd_filter_info.allocator_ = &allocator_;
  pd_filter_info.cell_data_ptrs_ = static_cast<const char **>(allocator_.alloc(sizeof(const char *) * pd_filter_info.batch_size_));
  ASSERT_NE(nullptr, pd_filter_info.cell_data_ptrs_);

  ObBitmap result_bitmap(allocator_);
  ASSERT_EQ(OB_SUCCESS, result_bitmap.init(pd_filter_info.count_));

  ObStorageDatum datum_buf[2];
  sql::ObPushdownBlackFilterNode filter_node(allocator_);
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator op(eval_ctx, expr_spec);
  MockPaxBlackFilterExecutor filter(allocator_, filter_node, op, datum_buf, col_descs_.at(dict_col_idx).col_type_);
  ASSERT_EQ(OB_SUCCESS, filter.col_offsets_.init(1));
  ASSERT_EQ(OB_SUCCESS, filter.col_params_.init(1));
  ASSERT_EQ(OB_SUCCESS, filter.col_offsets_.push_back(dict_col_idx));
  ASSERT_EQ(OB_SUCCESS, filter.col_params_.push_back(nullptr));
  filter.n_cols_ = 1;

  bool filter_applied = true;
  ASSERT_EQ(OB_SUCCESS, decoder.filter_black_filter_batch(nullptr, filter, pd_filter_info, result_bitmap, filter_applied));
  ASSERT_FALSE(filter_applied);
  ASSERT_EQ(0, filter.get_datums_call_count_);
  ASSERT_EQ(0, filter.filter_batch_call_count_);
}

PUSHDOWN_GENERAL_TEST(TestRetroPDDecoder);
PUSHDOWN_GENERAL_TEST(TestDictDecoder);
PUSHDOWN_GENERAL_TEST(TestRLEDecoder);
PUSHDOWN_GENERAL_TEST(TestIntBaseDiffDecoder);

TEST_F(TestHexDecoder, basic_filter_pushdown_op_test_eq_ne_nu_nn)
{
  basic_filter_pushdown_eq_ne_nu_nn_test();
}

TEST_F(TestDictDecoder, batch_decode_to_datum_condense_test)
{
  batch_decode_to_datum_test(true);
}

TEST_F(TestDictDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestDictDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestDictDecoder, batch_decode_to_vector_test)
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

TEST_F(TestDictDecoder, batch_decode_single_var_len_dict) {
  const int64_t string_len = UINT16_MAX + 3;
  char *string_buf = nullptr;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, full_column_cnt_));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, row));
  int64_t varchar_col_idx = -1;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    if (col_descs_.at(i).col_type_.get_type() == ObVarcharType) {
      varchar_col_idx = i;
      break;
    }
  }
  ASSERT_NE(varchar_col_idx, -1);
  string_buf = static_cast<char *>(allocator_.alloc(string_len));
  ASSERT_TRUE(nullptr != string_buf);
  MEMSET(string_buf, 7, string_len);
  row.storage_datums_[varchar_col_idx].ptr_ = string_buf;
  row.storage_datums_[varchar_col_idx].pack_ = string_len;

  ASSERT_EQ(OB_SUCCESS, encoder_.append_row(row));
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, build_block(buf, size));
  ObMicroBlockDecoder decoder;
  ObMicroBlockData data;
  ASSERT_EQ(OB_SUCCESS, data.init_with_prepare_micro_header(encoder_.data_buffer_.data(), encoder_.data_buffer_.length()));
  ASSERT_EQ(OB_SUCCESS, decoder.init(data, read_info_));
  int32_t row_id = 0;
  const char *cell_data = nullptr;
  ObDatum datum;
  char datum_buf[40];
  datum.ptr_ = datum_buf;
  ASSERT_EQ(OB_SUCCESS, decoder.decoders_[varchar_col_idx].batch_decode(decoder.row_index_, &row_id, &cell_data, 1, &datum));

  ASSERT_EQ(datum.len_, string_len);
  ASSERT_EQ(0, MEMCMP(datum.ptr_, string_buf, string_len));

  // decode vector
  ObArenaAllocator test_allocator;
  ObArenaAllocator frame_allocator;
  sql::ObExecContext exec_context(test_allocator);
  sql::ObEvalCtx eval_ctx(exec_context);
  const char *ptr_arr[ROW_CNT];
  uint32_t len_arr[ROW_CNT];

  ObObjMeta col_meta = col_descs_.at(varchar_col_idx).col_type_;
  const int16_t precision = col_meta.is_decimal_int() ? col_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
  VecValueTypeClass vec_tc = common::get_vec_value_tc(
      col_meta.get_type(),
      col_meta.get_scale(),
      precision);
  ASSERT_EQ(col_meta.get_type(), ObVarcharType);

  sql::ObExpr col_expr;
  ASSERT_EQ(OB_SUCCESS, VectorDecodeTestUtil::generate_column_output_expr(
      ROW_CNT, col_meta, VEC_UNIFORM, eval_ctx, col_expr, frame_allocator));
  LOG_INFO("Current col: ", K(varchar_col_idx), K(col_meta),  K(*decoder.decoders_[varchar_col_idx].ctx_), K(precision), K(vec_tc));

  ObVectorDecodeCtx vector_ctx(ptr_arr, len_arr, &row_id, 1, 0, col_expr.get_vector_header(eval_ctx));
  ASSERT_EQ(OB_SUCCESS, decoder.decoders_[varchar_col_idx].decode_vector(decoder.row_index_, vector_ctx));
  ASSERT_TRUE(VectorDecodeTestUtil::verify_vector_and_datum_match(*(col_expr.get_vector_header(eval_ctx).get_vector()),
      0, row.storage_datums_[varchar_col_idx]));

}

TEST_F(TestRLEDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestRLEDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestRLEDecoder, batch_decode_to_vector_test)
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

TEST_F(TestIntBaseDiffDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestIntBaseDiffDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestIntBaseDiffDecoder, batch_decode_to_vector_test)
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
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestHexDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestHexDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestHexDecoder, batch_decode_to_vector_test)
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
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestStringDiffDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestStringDiffDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestStringDiffDecoder, batch_decode_to_vector_test)
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
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestStringPrefixDecoder, batch_decode_to_datum_test)
{
  batch_decode_to_datum_test();
}

TEST_F(TestStringPrefixDecoder, cell_decode_to_datum_test)
{
  cell_decode_to_datum_test();
}

TEST_F(TestStringPrefixDecoder, cell_decode_to_datum_test_without_hex)
{
  cell_decode_to_datum_test_without_hex();
}

TEST_F(TestStringPrefixDecoder, batch_decode_to_vector_test)
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
  TEST_ONE(VEC_DISCRETE);
  TEST_ONE(VEC_CONTINUOUS);
  #undef TEST_ONE
  #undef TEST_ONE_WITH_ALIGN
}

TEST_F(TestColumnEqualDecoder, cell_decode_to_datum_test)
{
  cell_column_equal_decode_to_datum_test();
}

TEST_F(TestColumnEqualDecoder, col_equal_batch_decode_to_vector_test)
{
  col_equal_batch_decode_to_vector_test(VEC_FIXED);
  col_equal_batch_decode_to_vector_test(VEC_UNIFORM);
  col_equal_batch_decode_to_vector_test(VEC_DISCRETE);
  col_equal_batch_decode_to_vector_test(VEC_CONTINUOUS);
}

TEST_F(TestInterColumnSubstringDecoder, cell_decode_to_datum_test)
{
  cell_inter_column_substring_to_datum_test();
}

TEST_F(TestInterColumnSubstringDecoder, col_substr_batch_decode_to_vector_test)
{
  col_substr_batch_decode_to_vector_test(VEC_UNIFORM);
  col_substr_batch_decode_to_vector_test(VEC_DISCRETE);
  col_substr_batch_decode_to_vector_test(VEC_CONTINUOUS);
}


// TEST_F(TestDictDecoder, batch_decode_perf_test)
// {
//   batch_get_row_perf_test();
// }

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_general_column_decoder.log*");
  OB_LOGGER.set_file_name("test_general_column_decoder.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

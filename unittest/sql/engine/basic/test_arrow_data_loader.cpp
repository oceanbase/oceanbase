/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include <apache-arrow/arrow/api.h>
#include "share/system_variable/ob_system_variable.h"
#include "sql/engine/basic/ob_arrow_data_loader.h"
#include "sql/ob_sql_init.h"
#include "utils/expr_maker.h"
#include "utils/test_op_base.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestArrowDataLoader : public TestOpBase
{
public:
  TestArrowDataLoader()
      : TestOpBase("TestArrowDataLoader"),
        expr_maker_(BATCH_SIZE, BATCH_SIZE, VEC_FIXED),
        loader_(nullptr)
  {}

protected:
  void SetUp() override
  {
    TestOpBase::SetUp();
    ExprMaker *maker = &expr_maker_;
    maker->set_frame_idx(0);
    eval_ctx_.frames_ = static_cast<char **>(allocator_.alloc(sizeof(char *)));
    ASSERT_NE(nullptr, eval_ctx_.frames_);
    eval_ctx_.frames_[0] = static_cast<char *>(allocator_.alloc(maker->frame_mem_size()));
    ASSERT_NE(nullptr, eval_ctx_.frames_[0]);
    MEMSET(eval_ctx_.frames_[0], 0, maker->frame_mem_size());
    exec_ctx_.set_frames(eval_ctx_.frames_);
    exec_ctx_.set_frame_cnt(1);
    eval_ctx_.max_batch_size_ = BATCH_SIZE;
    maker->make(expr_, eval_ctx_);

    reset_loader(*arrow::int64(), ObIntType, VEC_TC_INTEGER);
  }

  void TearDown() override
  {
    if (nullptr != loader_) {
      loader_->destroy();
      OB_DELETEx(ObArrowDataLoader, &allocator_, loader_);
      loader_ = nullptr;
    }
  }

  std::shared_ptr<arrow::Array> make_int64_array(const int64_t *values, const int64_t length)
  {
    arrow::Int64Builder builder;
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.AppendValues(values, length).ok());
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
  }

  void reset_loader(const arrow::DataType &arrow_type,
                    const ObObjType ob_type,
                    const VecValueTypeClass value_tc)
  {
    if (nullptr != loader_) {
      loader_->destroy();
      OB_DELETEx(ObArrowDataLoader, &allocator_, loader_);
      loader_ = nullptr;
    }
    expr_.datum_meta_.type_ = ob_type;
    expr_.vec_value_tc_ = value_tc;
    ObArrowDataLoaderFactory factory;
    ASSERT_EQ(OB_SUCCESS,
              factory.select_loader(allocator_, arrow_type, expr_.datum_meta_, loader_));
    ASSERT_NE(nullptr, loader_);
  }

  void reset_decimal_loader(
      const arrow::DataType &arrow_type,
      const int16_t precision,
      const int16_t scale)
  {
    if (nullptr != loader_) {
      loader_->destroy();
      OB_DELETEx(ObArrowDataLoader, &allocator_, loader_);
      loader_ = nullptr;
    }
    expr_.datum_meta_.type_ = ObDecimalIntType;
    expr_.datum_meta_.precision_ = precision;
    expr_.datum_meta_.scale_ = scale;
    expr_.vec_value_tc_ = get_vec_value_tc(ObDecimalIntType, scale, precision);
    expr_.basic_funcs_ = ObDatumFuncs::get_basic_func(
        expr_.datum_meta_.type_,
        expr_.datum_meta_.cs_type_,
        scale,
        lib::is_oracle_mode(),
        false,
        precision);
    ASSERT_NE(nullptr, expr_.basic_funcs_);
    ObArrowDataLoaderFactory factory;
    ASSERT_EQ(
        OB_SUCCESS,
        factory.select_loader(allocator_, arrow_type, expr_.datum_meta_, loader_));
    ASSERT_NE(nullptr, loader_);
  }

  template <typename DecimalBuilder>
  std::shared_ptr<arrow::Array> make_decimal_array(
      const std::shared_ptr<arrow::DataType> &arrow_type,
      const std::vector<int64_t> &values,
      const std::vector<bool> &nulls)
  {
    EXPECT_EQ(values.size(), nulls.size());
    DecimalBuilder builder(arrow_type);
    for (size_t i = 0; i < values.size(); ++i) {
      if (nulls[i]) {
        EXPECT_TRUE(builder.AppendNull().ok());
      } else {
        typename DecimalBuilder::ValueType value(values[i]);
        EXPECT_TRUE(builder.Append(value).ok());
      }
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
  }

  void expect_decimal_layout_values(
      const std::shared_ptr<arrow::Array> &array,
      const int16_t precision,
      const int16_t scale,
      const std::vector<bool> &expected_nulls)
  {
    ASSERT_NE(nullptr, array);
    ASSERT_EQ(array->length(), static_cast<int64_t>(expected_nulls.size()));
    reset_decimal_loader(*array->type(), precision, scale);
    ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, array->length()));
    ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
    ASSERT_NE(nullptr, out_vec);
    out_vec->get_nulls()->set_all(static_cast<int64_t>(0), array->length());
    out_vec->set_has_null();

    ASSERT_EQ(OB_SUCCESS, loader_->load(*array, eval_ctx_, &expr_));

    bool expected_has_null = false;
    for (int64_t i = 0; i < array->length(); ++i) {
      expected_has_null = expected_has_null || expected_nulls[i];
      EXPECT_EQ(expected_nulls[i], out_vec->is_null(i));
      if (!expected_nulls[i]) {
        const arrow::FixedSizeBinaryArray &decimal_array =
            static_cast<const arrow::FixedSizeBinaryArray &>(*array);
        EXPECT_EQ(
            0,
            MEMCMP(out_vec->get_payload(i), decimal_array.GetValue(i), decimal_array.byte_width()));
      }
    }
    EXPECT_EQ(expected_has_null, out_vec->has_null());
  }

  template <typename ArrowType>
  void expect_null_free_values(const std::shared_ptr<arrow::DataType> &arrow_type,
                               const ObObjType ob_type,
                               const VecValueTypeClass value_tc,
                               const std::vector<typename ArrowType::c_type> &values)
  {
    const int64_t length = static_cast<int64_t>(values.size());
    arrow::NumericBuilder<ArrowType> builder;
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.AppendValues(values.data(), length).ok());
    ASSERT_TRUE(builder.Finish(&array).ok());
    ASSERT_NE(nullptr, array);
    reset_loader(*arrow_type, ob_type, value_tc);
    ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, length));
    ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
    ASSERT_NE(nullptr, out_vec);
    out_vec->get_nulls()->set_all(static_cast<int64_t>(0), length);
    out_vec->set_has_null();

    ASSERT_EQ(OB_SUCCESS, loader_->load(*array, eval_ctx_, &expr_));

    EXPECT_FALSE(out_vec->has_null());
    EXPECT_EQ(sizeof(typename ArrowType::c_type), out_vec->get_length());
    for (int64_t i = 0; i < length; ++i) {
      EXPECT_FALSE(out_vec->is_null(i));
      EXPECT_EQ(0,
                MEMCMP(out_vec->get_data() + i * sizeof(typename ArrowType::c_type),
                       &values[i],
                       sizeof(typename ArrowType::c_type)));
    }
  }

  // Keep enough reserve space for DECIMAL256 vectors (32 bytes per value).
  static constexpr int64_t BATCH_SIZE = 32;
  SeqIntGenExprMaker expr_maker_;
  ObExpr expr_;
  ObArrowDataLoader *loader_;
};

TEST_F(TestArrowDataLoader, null_free_load_clears_previous_null_state)
{
  arrow::Int64Builder nullable_builder;
  std::shared_ptr<arrow::Array> nullable_array;
  const int64_t values[] = {11, 22, 33};
  std::shared_ptr<arrow::Array> array = make_int64_array(values, ARRAYSIZEOF(values));
  ASSERT_NE(nullptr, array);
  ASSERT_EQ(OB_SUCCESS,
            expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, ARRAYSIZEOF(values)));
  ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
  ASSERT_NE(nullptr, out_vec);
  ASSERT_TRUE(nullable_builder.AppendNull().ok());
  ASSERT_TRUE(nullable_builder.Append(2).ok());
  ASSERT_TRUE(nullable_builder.Append(3).ok());
  ASSERT_TRUE(nullable_builder.Finish(&nullable_array).ok());
  ASSERT_NE(nullptr, nullable_array);

  ASSERT_EQ(OB_SUCCESS, loader_->load(*nullable_array, eval_ctx_, &expr_));
  ASSERT_TRUE(out_vec->has_null());
  ASSERT_TRUE(out_vec->is_null(0));

  ASSERT_EQ(OB_SUCCESS, loader_->load(*array, eval_ctx_, &expr_));

  EXPECT_FALSE(out_vec->has_null());
  for (int64_t i = 0; i < ARRAYSIZEOF(values); ++i) {
    EXPECT_FALSE(out_vec->is_null(i));
    EXPECT_EQ(values[i], out_vec->get_int(i));
  }
}

TEST_F(TestArrowDataLoader, null_free_slice_loads_logical_values)
{
  const int64_t values[] = {5, 11, 22, 33, 44};
  std::shared_ptr<arrow::Array> array = make_int64_array(values, ARRAYSIZEOF(values));
  ASSERT_NE(nullptr, array);
  std::shared_ptr<arrow::Array> slice = array->Slice(1, 3);
  ASSERT_NE(nullptr, slice);
  ASSERT_EQ(1, slice->offset());
  ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, slice->length()));

  ASSERT_EQ(OB_SUCCESS, loader_->load(*slice, eval_ctx_, &expr_));

  ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
  ASSERT_NE(nullptr, out_vec);
  const int64_t expected[] = {11, 22, 33};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected); ++i) {
    EXPECT_FALSE(out_vec->is_null(i));
    EXPECT_EQ(expected[i], out_vec->get_int(i));
  }
}

TEST_F(TestArrowDataLoader, nullable_slice_preserves_values_and_nulls)
{
  arrow::Int64Builder builder;
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Append(7).ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append(29).ok());
  ASSERT_TRUE(builder.Append(41).ok());
  ASSERT_TRUE(builder.Finish(&array).ok());
  ASSERT_NE(nullptr, array);
  std::shared_ptr<arrow::Array> slice = array->Slice(1, 2);
  ASSERT_NE(nullptr, slice);
  ASSERT_EQ(1, slice->offset());
  ASSERT_EQ(1, slice->null_count());
  ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, slice->length()));

  ASSERT_EQ(OB_SUCCESS, loader_->load(*slice, eval_ctx_, &expr_));

  ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
  ASSERT_NE(nullptr, out_vec);
  EXPECT_TRUE(out_vec->has_null());
  EXPECT_TRUE(out_vec->is_null(0));
  EXPECT_FALSE(out_vec->is_null(1));
  EXPECT_EQ(29, out_vec->get_int(1));
}

TEST_F(TestArrowDataLoader, empty_array_loads_successfully)
{
  arrow::Int64Builder builder;
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());
  ASSERT_NE(nullptr, array);
  ASSERT_EQ(0, array->length());
  ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, 0));

  EXPECT_EQ(OB_SUCCESS, loader_->load(*array, eval_ctx_, &expr_));
  EXPECT_FALSE(expr_.get_vector(eval_ctx_)->has_null());
}

TEST_F(TestArrowDataLoader, supported_fixed_width_types_preserve_values)
{
  expect_null_free_values<arrow::UInt64Type>(
      arrow::uint64(), ObUInt64Type, VEC_TC_UINTEGER, {1, UINT64_MAX});
  expect_null_free_values<arrow::DoubleType>(
      arrow::float64(), ObDoubleType, VEC_TC_DOUBLE, {1.25, -9.5});
  expect_null_free_values<arrow::FloatType>(
      arrow::float32(), ObFloatType, VEC_TC_FLOAT, {2.5F, -4.25F});
  expect_null_free_values<arrow::Date32Type>(
      arrow::date32(), ObDateType, VEC_TC_DATE, {0, 20000});
}

TEST_F(TestArrowDataLoader, decimal128_layout_match_preserves_values_and_nulls)
{
  const std::shared_ptr<arrow::DataType> type = arrow::decimal128(20, 2);
  const std::vector<int64_t> values = {12345, -67890, 0, 42};
  const std::vector<bool> nulls = {false, true, false, false};
  const std::shared_ptr<arrow::Array> array =
      make_decimal_array<arrow::Decimal128Builder>(type, values, nulls);

  expect_decimal_layout_values(array, 20, 2, nulls);
}

TEST_F(TestArrowDataLoader, decimal256_layout_match_preserves_values_and_nulls)
{
  const std::shared_ptr<arrow::DataType> type = arrow::decimal256(40, 2);
  const std::vector<int64_t> values = {12345, -67890, 0, 42};
  const std::vector<bool> nulls = {false, true, false, false};
  const std::shared_ptr<arrow::Array> array =
      make_decimal_array<arrow::Decimal256Builder>(type, values, nulls);

  expect_decimal_layout_values(array, 40, 2, nulls);
}

TEST_F(TestArrowDataLoader, decimal_layout_match_handles_sliced_array)
{
  const std::shared_ptr<arrow::DataType> type = arrow::decimal128(20, 2);
  const std::vector<int64_t> values = {7, 12345, -67890, 42};
  const std::vector<bool> nulls = {false, true, false, false};
  const std::shared_ptr<arrow::Array> array =
      make_decimal_array<arrow::Decimal128Builder>(type, values, nulls);
  ASSERT_NE(nullptr, array);
  const std::shared_ptr<arrow::Array> slice = array->Slice(1, 2);
  ASSERT_EQ(1, slice->offset());

  expect_decimal_layout_values(slice, 20, 2, {true, false});
}

TEST_F(TestArrowDataLoader, decimal_layout_match_clears_reused_null_free_batch)
{
  const std::shared_ptr<arrow::DataType> type = arrow::decimal128(20, 2);
  const std::shared_ptr<arrow::Array> nullable =
      make_decimal_array<arrow::Decimal128Builder>(type, {12345, 67890, 42}, {true, false, false});
  const std::shared_ptr<arrow::Array> null_free =
      make_decimal_array<arrow::Decimal128Builder>(type, {11, 22, 33}, {false, false, false});
  ASSERT_NE(nullptr, nullable);
  ASSERT_NE(nullptr, null_free);
  reset_decimal_loader(*type, 20, 2);
  ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, 3));
  ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
  ASSERT_NE(nullptr, out_vec);
  ASSERT_EQ(OB_SUCCESS, loader_->load(*nullable, eval_ctx_, &expr_));
  ASSERT_TRUE(out_vec->has_null());
  ASSERT_TRUE(out_vec->is_null(0));

  ASSERT_EQ(OB_SUCCESS, loader_->load(*null_free, eval_ctx_, &expr_));

  EXPECT_FALSE(out_vec->has_null());
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_FALSE(out_vec->is_null(i));
    const arrow::FixedSizeBinaryArray &decimal_array =
        static_cast<const arrow::FixedSizeBinaryArray &>(*null_free);
    EXPECT_EQ(
        0,
        MEMCMP(out_vec->get_payload(i), decimal_array.GetValue(i), decimal_array.byte_width()));
  }
}

TEST_F(TestArrowDataLoader, decimal_layout_match_handles_all_null_and_empty_arrays)
{
  const std::shared_ptr<arrow::DataType> type = arrow::decimal128(20, 2);
  const std::shared_ptr<arrow::Array> all_null =
      make_decimal_array<arrow::Decimal128Builder>(type, {0, 0, 0}, {true, true, true});
  expect_decimal_layout_values(all_null, 20, 2, {true, true, true});

  arrow::Decimal128Builder builder(type);
  std::shared_ptr<arrow::Array> empty;
  ASSERT_TRUE(builder.Finish(&empty).ok());
  expect_decimal_layout_values(empty, 20, 2, {});
}

TEST_F(TestArrowDataLoader, decimal_layout_mismatch_uses_conversion_path)
{
  const std::shared_ptr<arrow::DataType> scale_mismatch_type = arrow::decimal128(20, 2);
  const std::shared_ptr<arrow::Array> scale_mismatch =
      make_decimal_array<arrow::Decimal128Builder>(scale_mismatch_type, {12345}, {false});
  reset_decimal_loader(*scale_mismatch_type, 20, 3);
  ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, 1));
  ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
  ASSERT_EQ(OB_SUCCESS, loader_->load(*scale_mismatch, eval_ctx_, &expr_));
  EXPECT_FALSE(out_vec->is_null(0));
  EXPECT_EQ(123450, *reinterpret_cast<const int64_t *>(out_vec->get_payload(0)));

  const std::shared_ptr<arrow::DataType> width_mismatch_type = arrow::decimal128(20, 2);
  const std::shared_ptr<arrow::Array> width_mismatch =
      make_decimal_array<arrow::Decimal128Builder>(width_mismatch_type, {12345}, {false});
  reset_decimal_loader(*width_mismatch_type, 9, 2);
  ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, 1));
  out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
  ASSERT_EQ(OB_SUCCESS, loader_->load(*width_mismatch, eval_ctx_, &expr_));
  EXPECT_FALSE(out_vec->is_null(0));
  EXPECT_EQ(12345, *reinterpret_cast<const int32_t *>(out_vec->get_payload(0)));

  const std::shared_ptr<arrow::DataType> precision_mismatch_type = arrow::decimal128(38, 2);
  const std::shared_ptr<arrow::Array> precision_mismatch =
      make_decimal_array<arrow::Decimal128Builder>(precision_mismatch_type, {12345}, {false});
  reset_decimal_loader(*precision_mismatch_type, 20, 2);
  ASSERT_EQ(OB_SUCCESS, expr_.init_vector_for_write(eval_ctx_, VEC_FIXED, 1));
  out_vec = static_cast<ObFixedLengthBase *>(expr_.get_vector(eval_ctx_));
  ASSERT_EQ(OB_SUCCESS, loader_->load(*precision_mismatch, eval_ctx_, &expr_));
  EXPECT_FALSE(out_vec->is_null(0));
  EXPECT_EQ(12345, *reinterpret_cast<const int64_t *>(out_vec->get_payload(0)));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  (void)oceanbase::ObPreProcessSysVars::init_sys_var();
  (void)oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}

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

  static constexpr int64_t BATCH_SIZE = 8;
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

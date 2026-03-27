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
#include "test_op_base.h"

#include <cstring>
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase
{
namespace sql
{

// Constructor for shared UT fixture context.
// `allocator_` is named per test suite and backs `exec_ctx_`.
TestOpBase::TestOpBase(const char *name)
    : allocator_(name),
      exec_ctx_(allocator_),
      eval_ctx_(exec_ctx_),
      child_utils_(exec_ctx_, eval_ctx_),
      phy_plan_()
{}

// Initialize minimal SQL session + tenant environment required by engine operators.
// Most UTs in this directory rely on the same bootstrap sequence.
void TestOpBase::SetUp()
{
  // Make allocations tenant-visible under sanity mode.
  allocator_.set_attr(common::ObMemAttr(common::OB_SYS_TENANT_ID, "UTBasicOp"));
  lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
  if (OB_NOT_NULL(malloc_allocator)) {
    (void)malloc_allocator->create_and_add_tenant_allocator(common::OB_SYS_TENANT_ID);
  }
  ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, &allocator_));
  ASSERT_EQ(OB_SUCCESS, session_.load_default_sys_variable(false, true));
  ASSERT_EQ(OB_SUCCESS, session_.init_tenant(ObString("test"), OB_SYS_TENANT_ID));
  exec_ctx_.set_my_session(&session_);
}

void TestOpBase::init_exec_env(
    const std::vector<GenExprMaker *> &gen_expr_makers,
    const std::vector<ExprMaker *> &extra_expr_makers,
    ObOpSpec &parent_spec,
    int64_t batch_size,
    const int64_t phy_op_count)
{
  std::vector<ExprMaker *> expr_makers(gen_expr_makers.begin(), gen_expr_makers.end());
  expr_makers.insert(expr_makers.end(), extra_expr_makers.begin(), extra_expr_makers.end());
  ASSERT_GT(phy_op_count, 0);
  ASSERT_FALSE(expr_makers.empty());
  for (size_t i = 0; i < expr_makers.size(); ++i) {
    ASSERT_NE(nullptr, expr_makers[i]);
    expr_makers[i]->set_frame_idx(static_cast<int64_t>(i));
    ASSERT_GE(expr_makers[i]->frame_idx(), 0);
  }
  ASSERT_TRUE(nullptr == eval_ctx_.frames_);
  ASSERT_EQ(OB_SUCCESS, exec_ctx_.init_phy_op(phy_op_count));
  ASSERT_EQ(OB_SUCCESS, exec_ctx_.create_physical_plan_ctx());
  eval_ctx_.frames_ = static_cast<char **>(
      exec_ctx_.get_allocator().alloc(sizeof(void *) * expr_makers.size()));
  ASSERT_NE(nullptr, eval_ctx_.frames_);
  for (size_t i = 0; i < expr_makers.size(); ++i) {
    const int64_t mem_size = expr_makers[i]->frame_mem_size();
    ASSERT_GT(mem_size, 0);
    eval_ctx_.frames_[i] = static_cast<char *>(exec_ctx_.get_allocator().alloc(mem_size));
    ASSERT_NE(nullptr, eval_ctx_.frames_[i]);
    MEMSET(eval_ctx_.frames_[i], 0, mem_size);
  }
  exec_ctx_.set_frames(eval_ctx_.frames_);
  exec_ctx_.set_frame_cnt(expr_makers.size());

  phy_plan_.set_batch_size(batch_size);
  parent_spec.plan_ = &phy_plan_;
  child_utils_.init(batch_size, gen_expr_makers);
  child_utils_.bind_plan(phy_plan_);
  eval_ctx_.max_batch_size_ = batch_size;
}

void TestOpBase::attach_mock_child(ObOpSpec &parent_spec, ObOperator &parent_op)
{
  child_utils_.attach_to_parent(parent_spec, parent_op);
}

// Validate one returned batch against expected active-row values.
// Example:
//   brs: size=4, skip={F,T,F,F}, expected={10,20,30}
//   compares row0->10, row2->20, row3->30
template <typename T>
void TestOpBase::validate_data(ObExpr *expr,
                              const ObBatchRows *brs,
                              const std::vector<std::optional<T>> &expected)
{
    common::ObIVector *vec = expr->get_vector(eval_ctx_);
    ASSERT_NE(nullptr, vec);
    ASSERT_NE(nullptr, brs);

    int64_t output_row_idx = 0;
    for (int64_t i = 0; i < brs->size_; ++i) {
      if (brs->all_rows_active_ || !brs->skip_->at(i)) {
        ASSERT_LT(output_row_idx, static_cast<int64_t>(expected.size()))
            << "got more than expected output rows";
        const std::optional<T> &exp = expected[output_row_idx];
        if (!exp.has_value()) {
          ASSERT_TRUE(vec->is_null(i))
              << "row " << output_row_idx << " should be null";
        } else {
          const T &val = exp.value();
          if constexpr (std::is_same_v<T, int64_t>) {
            ASSERT_FALSE(vec->is_null(i))
                << "row " << output_row_idx << " is null";
            ASSERT_EQ(exp.value(), vec->get_int(i))
                << "row " << output_row_idx << " value mismatch";
          } else if constexpr (std::is_same_v<T, std::string>) {
            common::ObString actual = vec->get_string(i);
            ASSERT_EQ(val.size(), static_cast<size_t>(actual.length()))
                << "row " << output_row_idx << " length mismatch, expected " << val;
            ASSERT_EQ(0, MEMCMP(val.data(), actual.ptr(), val.size()))
                << "row " << output_row_idx << " value mismatch";
          } else {
            static_assert(!sizeof(T), "validate_data only supports int, "
                                      "std::optional<int64_t>, std::string");
          }
        }
        ++output_row_idx;
      }
    }
    ASSERT_EQ(output_row_idx, static_cast<int64_t>(expected.size()));
}

// Fetch all output batches and validate them in one call.
// `skip_vec` is interpreted as flattened final output skip bits across batches.
// `expected_data` is interpreted as flattened active-row values across batches.
//
// Example:
//   skip_vec      = {F,F,T,F, F,T,F}
//   expected_data = {1,2,3,4,4}
// Each returned batch is sliced from these flattened vectors and then verified via
// `validate_brs` + `validate_data<T>`.
template <typename T>
void TestOpBase::get_next_all_and_validate(ObOperator *op,
                                          int64_t batch_size,
                                          const std::vector<std::optional<T>> &expected_data,
                                          const std::vector<bool> &skip_vec)
{
  ASSERT_NE(nullptr, op);
  ASSERT_GT(batch_size, 0);
  ASSERT_GT(op->get_spec().output_.count(), 0);
  ObExpr *out_expr = op->get_spec().output_.at(0);
  ASSERT_NE(nullptr, out_expr);

  const ObBatchRows *brs = nullptr;
  int64_t skip_pos = 0;
  int64_t data_pos = 0;
  while (true) {
    ASSERT_EQ(OB_SUCCESS, op->get_next_batch(batch_size, brs));
    ASSERT_NE(nullptr, brs);

    std::vector<std::optional<T>> batch_expected_data;

    if (!skip_vec.empty()) {
      // Build "this batch" expected skip/data slices from flattened vectors.
      std::vector<bool> batch_skip;
      batch_skip.reserve(brs->size_);
      for (int64_t i = 0; i < brs->size_; ++i) {
        ASSERT_LT(skip_pos, static_cast<int64_t>(skip_vec.size()));
        batch_skip.push_back(skip_vec[skip_pos]);
        if (!skip_vec[skip_pos]) {
          ASSERT_LT(data_pos, static_cast<int64_t>(expected_data.size()));
          batch_expected_data.push_back(expected_data[data_pos]);
          ++data_pos;
        }
        ++skip_pos;
      }
      // Validate current batch shape and values.
      validate_brs(brs, batch_skip, brs->size_, brs->end_);
    } else {
      for (int64_t i = 0; i < brs->size_; ++i) {
        if (!brs->skip_->at(i)) {
          batch_expected_data.push_back(expected_data[data_pos]);
          ++data_pos;
        }
      }
    }
    validate_data<T>(out_expr, brs, batch_expected_data);

    if (brs->end_) {
      break;
    }
  } // while get next batch

  if (!skip_vec.empty()) {
    ASSERT_EQ(skip_pos, static_cast<int64_t>(skip_vec.size()));
  }
  ASSERT_EQ(data_pos, static_cast<int64_t>(expected_data.size()));
}

template void TestOpBase::validate_data<int64_t>(ObExpr *expr,
                                                const ObBatchRows *brs,
                                                const std::vector<std::optional<int64_t>> &expected);
template void TestOpBase::validate_data<std::string>(ObExpr *expr,
                                                    const ObBatchRows *brs,
                                                    const std::vector<std::optional<std::string>> &expected);
template void TestOpBase::get_next_all_and_validate<int64_t>(
    ObOperator *op,
    int64_t batch_size,
    const std::vector<std::optional<int64_t>> &expected_data,
    const std::vector<bool> &skip_vec);
template void TestOpBase::get_next_all_and_validate<std::string>(
    ObOperator *op,
    int64_t batch_size,
    const std::vector<std::optional<std::string>> &expected_data,
    const std::vector<bool> &skip_vec);

// Validate batch metadata and optional skip bit pattern.
// If `skip_vec` is empty, only size/end are checked.
void TestOpBase::validate_brs(const ObBatchRows *brs,
                              const std::vector<bool> &skip_vec,
                              int64_t size,
                              bool is_end)
{
  ASSERT_NE(nullptr, brs);
  ASSERT_EQ(size, brs->size_);
  ASSERT_EQ(is_end, brs->end_);
  if (!skip_vec.empty()) {
    ASSERT_EQ(static_cast<int64_t>(skip_vec.size()), brs->size_)
        << "skip_vec size must match batch size";
    for (int64_t i = 0; i < brs->size_; ++i) {
      ASSERT_EQ(skip_vec[i], brs->skip_->at(i))
          << "brs.skip[" << i << "] mismatch: expected " << (skip_vec[i] ? "skip" : "active");
    }
  }
}

} // namespace sql
} // namespace oceanbase

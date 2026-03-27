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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <vector>
#include "sql/ob_sql_init.h"
#include "utils/expr_maker.h"
#include "utils/test_op_base.h"
#include "sql/engine/basic/ob_limit_vec_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_batch_rows.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/allocator/ob_allocator.h"
#include "share/datum/ob_datum.h"
#include "share/system_variable/ob_system_variable.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestObLimitVecOp : public TestOpBase
{
public:
  TestObLimitVecOp()
      : TestOpBase("TestObLimitVecOp"),
        limit_spec_(exec_ctx_.get_allocator(), PHY_VEC_LIMIT),
        limit_op_(nullptr),
        limit_expr_()
  {}

protected:
  void TearDown() override
  {
    if (nullptr != limit_op_) {
      limit_op_->destroy();
      limit_op_ = nullptr;
    }
  }

  static constexpr int64_t BATCH_SIZE = 256;

  void init_limit_vec_case(const std::vector<GenExprMaker *> &gen_expr_makers,
        int limit_count, int64_t sort_key_nums = 0)
  {
    ASSERT_FALSE(gen_expr_makers.empty());
    // add limit expr maker
    ConstIntExprMaker limit_expr_maker(limit_count, BATCH_SIZE);
    std::vector<ExprMaker *> extra_expr_makers = {&limit_expr_maker};
    init_exec_env(gen_expr_makers, extra_expr_makers, limit_spec_, BATCH_SIZE);
    MockChildSpec *child_spec = child_utils_.spec();
    ASSERT_NE(nullptr, child_spec);

    limit_spec_.max_batch_size_ = BATCH_SIZE;
    limit_expr_maker.make(limit_expr_, eval_ctx_);
    limit_spec_.limit_expr_ = &limit_expr_;
    limit_spec_.offset_expr_ = nullptr;
    limit_spec_.is_fetch_with_ties_ = (sort_key_nums > 0);
    // When sort_key_nums > 0, first n output columns are ORDER BY keys for tie comparison.
    if (sort_key_nums > 0) {
      ASSERT_LE(sort_key_nums, static_cast<int64_t>(gen_expr_makers.size()));
      ASSERT_EQ(OB_SUCCESS, limit_spec_.sort_columns_.init(sort_key_nums));
      for (int64_t i = 0; i < sort_key_nums; ++i) {
        ASSERT_EQ(OB_SUCCESS, limit_spec_.sort_columns_.push_back(child_spec->output_.at(i)));
      }
    }
    limit_spec_.output_.set_allocator(&exec_ctx_.get_allocator());
    ASSERT_EQ(OB_SUCCESS, limit_spec_.output_.init(gen_expr_makers.size()));
    for (size_t i = 0; i < gen_expr_makers.size(); ++i) {
      ASSERT_EQ(OB_SUCCESS, limit_spec_.output_.push_back(child_spec->output_.at(i)));
    }

    limit_op_ = new (exec_ctx_.get_allocator().alloc(sizeof(ObLimitVecOp)))
        ObLimitVecOp(exec_ctx_, limit_spec_, nullptr);
    ASSERT_NE(nullptr, limit_op_);

    attach_mock_child(limit_spec_, *limit_op_);
  }

  ObExpr* get_output_expr(int64_t index) const { return limit_spec_.output_.at(index); }

  ObLimitVecSpec limit_spec_;
  ObLimitVecOp *limit_op_;
  ObExpr limit_expr_;
};

/**
 * Case0:
 * LIMIT 4 (with_ties=false)
 * Input:
 * - 1st batch: 4 rows, 3/4 row active {0, 1, 2, x}
 * - 2nd batch: 1 skip row
 * - 3rd batch: 1 active row {5}
 * Output:
 * - 1st batch: returned as-is, {0, 1, 2, x}
 * - 2nd batch: 1 active row {5}
 * - 3rd batch: empty with end=true
 */
TEST_F(TestObLimitVecOp, two_batches_with_skip_rows)
{
  const int64_t limit_count = 4;

  // 1st batch: 4 rows, 3/4 row active {0, 1, 2, x}; 2nd batch: {x, 5, 6, 7}
  FixedGenExprMaker<int64_t> output_expr_maker(
      BATCH_SIZE,
      {{0, 1, 2, 0}, {0}, {5}},
      common::VEC_FIXED,
      {{false, false, false, true}, {true}, {false}});
  init_limit_vec_case({&output_expr_maker}, limit_count);
  ASSERT_EQ(OB_SUCCESS, limit_op_->open());
  const ObBatchRows *brs = nullptr;
  ObExpr *out_expr = get_output_expr(0);

  // 1st output: returned as-is, 4 rows, 3 active {0, 1, 2}, skip row 3
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_data<int64_t>(out_expr, brs, {0, 1, 2});
  validate_brs(brs, {false, false, false, true}, 4, false);

  // 2rd output: 1 active row
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_data<int64_t>(out_expr, brs, {5});
  validate_brs(brs, {false}, 1, false);

  // 3rd batch: empty with end=true
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_brs(brs, {}, 0, true);
}

/**
 * Case1:
 * Same as Case0 but vector_format is VEC_CONTINUOUS and data type is string.
 */
TEST_F(TestObLimitVecOp, two_batches_string_continuous_with_skip_rows)
{
  const int64_t limit_count = 4;

  AllAtOnceGenExprMaker<std::string> output_expr_maker(
      BATCH_SIZE,
      {"zero", "one", "two", "NAN", "NAN", "five"},
      common::VEC_CONTINUOUS,
      {false, false, false, true, true, false});
  init_limit_vec_case({&output_expr_maker}, limit_count);

  ASSERT_EQ(OB_SUCCESS, limit_op_->open());

  get_next_all_and_validate<std::string>(
      limit_op_,
      BATCH_SIZE,
      {"zero", "one", "two", "five"});
}

/**
* Case3:
* LIMIT 2(with_ties=false), VEC_UNIFORM format
* input: 2 rows, {0, 1}
* output: all active rows
*/
TEST_F(TestObLimitVecOp, one_batch_all_active_without_ties_uniform)
{
  const int64_t row_count = 4;
  const int64_t limit_count = 2;

  SeqIntGenExprMaker output_expr_maker(BATCH_SIZE, row_count, common::VEC_UNIFORM);
  init_limit_vec_case({&output_expr_maker}, limit_count);
  ASSERT_EQ(OB_SUCCESS, limit_op_->open());
  const ObBatchRows *brs = nullptr;
  ObExpr *out_expr = get_output_expr(0);

  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_data<int64_t>(out_expr, brs, {0, 1});
  validate_brs(brs, {false, false}, 2, false);

  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_brs(brs, {}, 0, true);
}

/**
 * Case4:
 * Same as Case3 but with VEC_FIXED format vector instead of VEC_UNIFORM.
 * LIMIT 2 (with_ties=false)
 * input: 2 rows, {0, 1}
 * output: all active rows
 */
TEST_F(TestObLimitVecOp, one_batch_all_active_without_ties)
{
  const int64_t row_count = 4;
  const int64_t limit_count = 2;

  SeqIntGenExprMaker output_expr_maker(BATCH_SIZE, row_count, common::VEC_FIXED);
  init_limit_vec_case({&output_expr_maker}, limit_count);
  ASSERT_EQ(OB_SUCCESS, limit_op_->open());

  get_next_all_and_validate<int64_t>(
      limit_op_,
      BATCH_SIZE,
      {0, 1});
}

/**
 * Case5:
 * LIMIT 2 (with_ties=true)
 * Input: 2 rows with values {1, 3}
 * Output:
 * - 1st batch: all active rows returned
 * - 2nd batch: empty with end=true
 *
 * This test verifies the WITH TIES path where save_store_row is called
 * to save sort keys for tie comparison.
 */
TEST_F(TestObLimitVecOp, one_batch_all_active_with_ties)
{
  const int64_t limit_count = 2;
  const int64_t sort_key_nums = 1;

  FixedGenExprMaker<int64_t> output_expr_maker(BATCH_SIZE, {{1, 3}});
  init_limit_vec_case({&output_expr_maker}, limit_count, sort_key_nums);
  ASSERT_EQ(OB_SUCCESS, limit_op_->open());
  const ObBatchRows *brs = nullptr;
  ObExpr *out_expr = get_output_expr(0);

  // expected 1st batch: 2 rows active {1, 3}
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_data<int64_t>(out_expr, brs, {1, 3});
  validate_brs(brs, {false, false}, 2, false);

  // expected 2nd batch: empty with end=true
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_brs(brs, {}, 0, true);
}

/**
 * Case6:
 * LIMIT 4 (with_ties=true)
 * Input:
 * - 1st batch: 4 rows, 3/4 row active {1, 2, x, 3}
 * - 2nd batch: 1 row, 1/1 row active {4}
 * - 3rd batch: 3 rows, 2/3 row active {x, 4, 5}
 * Output:
 * - 1st batch: same as input, skip row 2
 * - 2nd batch: one row with value 4
 * - 3rd batch: 1/2 active {x, 4}
 * - 4th batch: empty with end=true
 */
TEST_F(TestObLimitVecOp, two_batches_with_skip_rows_with_ties)
{
  const int64_t limit_count = 4;
  const int64_t sort_key_nums = 1;

  // 1st batch: {1, 2, x, 3}; 2nd batch: {4}; 3rd batch: {x, 4, 5}
  FixedGenExprMaker<int64_t> output_expr_maker(
      BATCH_SIZE,
      {{1, 2, 0, 3}, {4}, {0, 4, 5}},
      common::VEC_FIXED,
      {{false, false, true, false}, {false}, {true, false, false}});
  init_limit_vec_case({&output_expr_maker}, limit_count, sort_key_nums);
  ASSERT_EQ(OB_SUCCESS, limit_op_->open());
  ObExpr *out_expr = get_output_expr(0);
  const ObBatchRows *brs = nullptr;

  // expected 1st output: same as input, skip row 2
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_data<int64_t>(out_expr, brs, {1, 2, 3});
  validate_brs(brs, {false, false, true, false}, 4, false);

  // expected 2nd batch: one row with value 4
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_data<int64_t>(out_expr, brs, {4});
  validate_brs(brs, {false}, 1, false);

  // expected 3rd batch: 1/2 active {x, 4}
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_data<int64_t>(out_expr, brs, {4});
  validate_brs(brs, {true, false}, 2, false);

  // expected 4th batch: empty with end=true
  ASSERT_EQ(OB_SUCCESS, limit_op_->get_next_batch(BATCH_SIZE, brs));
  validate_brs(brs, {}, 0, true);
}

/**
 * Case7:
 * Same input as Case6 but with string AllAtOnceGenExprMaker + get_next_all_and_validate helper.
 * LIMIT 4 (with_ties=true) with string data type.
 */
TEST_F(TestObLimitVecOp, two_batches_string_discrete_with_ties)
{
  const int64_t limit_count = 4;
  const int64_t sort_key_nums = 1;

  AllAtOnceGenExprMaker<std::string> output_expr_maker(
      BATCH_SIZE,
      {"one", "two", "", "three", "four", "", "four", "five"},
      common::VEC_DISCRETE,
      {false, false, true, false, false, true, false, false});
  init_limit_vec_case({&output_expr_maker}, limit_count, sort_key_nums);
  ASSERT_EQ(OB_SUCCESS, limit_op_->open());

  get_next_all_and_validate<std::string>(
      limit_op_,
      BATCH_SIZE,
      {"one", "two", "three", "four", "four"},
      {false, false, true, false, false, true, false});
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

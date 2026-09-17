/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
// Keep private-state inspection inside this test translation unit.
#include "sql/engine/aggregate/ob_groupby_vec_op.h"
#include "sql/engine/aggregate/ob_hash_agg_variant.h"
#include "sql/engine/basic/ob_hp_infras_vec_op.h"
#define private public
#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"
#undef private
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_hash_groupby.h"

namespace oceanbase
{
namespace sql
{

class HashGroupByOpTest : public OpTestKit
{
};

// Probe after the real inner_open() has bound candidate hash functions. Fixed
// candidates make this check independent of adaptive sampling thresholds.
class HashGroupByProbeOp : public ObHashGroupByVecOp
{
public:
  using ObHashGroupByVecOp::ObHashGroupByVecOp;
  int *probe_count_ = nullptr;

  int inner_open() override
  {
    int ret = ObHashGroupByVecOp::inner_open();
    if (OB_SUCCESS == ret) {
      check_candidate_hash();
      popular_array_temp_.reuse();
      popular_map_.reuse();
      ret = child_->rescan();
      clear_evaluated_flag();
    }
    return ret;
  }

  void check_candidate_hash()
  {
    ASSERT_TRUE(skew_detection_enabled_);
    const ObBatchRows *brs = nullptr;
    ASSERT_EQ(OB_SUCCESS, child_->get_next_batch(get_spec().max_batch_size_, brs));
    ASSERT_NE(nullptr, brs);
    ASSERT_EQ(2, brs->size_);
    ObTempRowStore candidates(&ctx_.get_allocator());
    ASSERT_EQ(OB_SUCCESS, candidates.init(local_group_rows_.get_row_meta(), 2,
        ObMemAttr(OB_SERVER_TENANT_ID, "HashProbeTest"), 0, false, NONE_COMPRESSOR));
    ObCompactRow *rows[2] = {};
    int64_t stored_count = 0;
    ASSERT_EQ(OB_SUCCESS, candidates.add_batch(all_groupby_exprs_, eval_ctx_, *brs, stored_count, rows));
    ASSERT_EQ(2, stored_count);
    popular_array_temp_.reuse();
    ASSERT_EQ(OB_SUCCESS, popular_array_temp_.push_back(std::make_pair(rows[0], 100)));
    ASSERT_EQ(OB_SUCCESS, update_popular_map());
    ASSERT_EQ(OB_SUCCESS, calc_groupby_exprs_hash_batch(dup_groupby_exprs_, *brs));
    for (int row = 0; row < 2; ++row) {
      uint64_t count = 0;
      ASSERT_EQ(OB_SUCCESS, popular_map_.get_refactored(
          hash_vals_[row] & ObGroupRowBucketBase::HASH_VAL_MASK, count));
      EXPECT_EQ(100, count);
      ++*probe_count_;
    }
  }
};

// The regular builder still creates the expressions, comparator and operator
// environment. Override only test-specific hashing metadata and observation.
class HashGroupByHashSpec : public HashGroupByTestSpec
{
public:
  ObVecHashAlgo first_algo_ = VEC_HASH_ALGO_MURMUR;
  ObVecHashAlgo second_algo_ = VEC_HASH_ALGO_MURMUR;
  ObCollationType collation_ = CS_TYPE_UTF8MB4_BIN;
  int *probe_count_ = nullptr;

  ObOpSpec *create_spec(ObIAllocator &alloc, MockDataSourceSpec *child_spec,
      const ExprFixedArray &output_exprs, ObExpr *limit_expr, ObExpr *offset_expr,
      bool use_rich_format) override
  {
    auto *spec = static_cast<ObHashGroupByVecSpec *>(HashGroupByTestSpec::create_spec(
        alloc, child_spec, output_exprs, limit_expr, offset_expr, use_rich_format));
    if (spec != nullptr) {
      for (int64_t i = 0; i < spec->group_exprs_.count(); ++i) {
        ObExpr &expr = *spec->group_exprs_.at(i);
        expr.set_vec_hash_algo(i == 0 ? first_algo_ : second_algo_);
        if (expr.obj_meta_.is_string_type()) {
          expr.obj_meta_.set_collation_type(collation_);
          expr.datum_meta_.cs_type_ = collation_;
          expr.basic_funcs_ = ObDatumFuncs::get_basic_func(expr.datum_meta_.type_, collation_,
              expr.datum_meta_.scale_, false, false, expr.datum_meta_.precision_);
          EXPECT_NE(nullptr, expr.basic_funcs_);
          EXPECT_FALSE(expr.obj_meta_.is_calc_end_space());
          spec->cmp_funcs_.at(i).cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
              expr.datum_meta_.type_, expr.datum_meta_.type_, NULL_LAST, collation_,
              expr.datum_meta_.scale_, false, expr.datum_meta_.precision_, expr.datum_meta_.precision_);
        }
      }
      spec->skew_detection_enabled_ = probe_count_ != nullptr;
      spec->by_pass_enabled_ = probe_count_ != nullptr;
    }
    return spec;
  }

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op) override
  {
    ObOperator *op = nullptr;
    if (probe_count_ != nullptr) {
      auto *probe = static_cast<HashGroupByProbeOp *>(
          default_create_op<HashGroupByProbeOp>(ctx, spec, child_op));
      if (probe != nullptr) { probe->probe_count_ = probe_count_; }
      op = probe;
    } else {
      op = HashGroupByTestSpec::create_op(ctx, spec, child_op);
    }
    return op;
  }
};

TEST_F(HashGroupByOpTest, PopularCandidateHashMatchesVector)
{
  for (auto first : {VEC_HASH_ALGO_CRC, VEC_HASH_ALGO_MURMUR, VEC_HASH_ALGO_DEFAULT}) {
    for (auto second : {VEC_HASH_ALGO_CRC, VEC_HASH_ALGO_MURMUR, VEC_HASH_ALGO_DEFAULT}) {
      for (int null_col : {-1, 0, 1, 2}) {
        SCOPED_TRACE(::testing::Message() << static_cast<int>(first) << ":"
            << static_cast<int>(second) << ":" << null_col);
        TestValue key = (null_col == 0 || null_col == 2) ? TestValue::null() : TestValue(0);
        TestValue value = (null_col == 1 || null_col == 2) ? TestValue::null() : TestValue("abc ");
        int probes = 0;
        HashGroupByHashSpec spec;
        spec.first_algo_ = first;
        spec.second_algo_ = second;
        spec.probe_count_ = &probes;
        auto result = spec.table("t", "a int, b varchar(32)").select("a, b, COUNT(*)").group_by("a, b")
            .with_rich_format(true).with_batch_size(2)
            .with_data({{key, value}, {key, value}, {1, "cold"}, {key, value}}).run(engine_);
        ASSERT_EQ(OB_SUCCESS, result.get_ret_code());
        EXPECT_EQ(2, probes);
        EXPECT_TRUE(result.verify_unordered({{key, value, 3}, {1, "cold", 1}}));
      }
    }
  }
}

// ============================================================================
// TC1: SingleCol_CountStar - unordered output
// ============================================================================
TEST_F(HashGroupByOpTest, SingleCol_CountStar)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_data({{2}, {1}, {2}, {1}, {3}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 2}, {2, 2}, {3, 1}}));
}

// ============================================================================
// TC2: SingleCol_Sum
// ============================================================================
TEST_F(HashGroupByOpTest, SingleCol_Sum)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, SUM(b)")
      .group_by("a")
      .with_data({{1, 10}, {2, 30}, {1, 20}, {2, 40}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 30}, {2, 70}}));
}

// ============================================================================
// TC3: SingleCol_MultiAggr
// ============================================================================
TEST_F(HashGroupByOpTest, SingleCol_MultiAggr)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60, 10, 30},
                                       {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC4: MultiCol_GroupBy
// ============================================================================
TEST_F(HashGroupByOpTest, MultiCol_GroupBy)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int, c int")
      .select("a, b, SUM(c)")
      .group_by("a, b")
      .with_data({{1, 2, 20}, {2, 1, 30}, {1, 1, 10}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 1, 10},
                                       {1, 2, 20},
                                       {2, 1, 30}}));
}

// ============================================================================
// TC5: EmptyInput
// ============================================================================
TEST_F(HashGroupByOpTest, EmptyInput)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_data({})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
  EXPECT_TRUE(result.verify_unordered({}));
}

// ============================================================================
// TC6: SingleGroup
// ============================================================================
TEST_F(HashGroupByOpTest, SingleGroup)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_data({{1, 10}, {1, 20}, {1, 30}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60}}));
}

// ============================================================================
// TC7: AllDistinctGroups
// ============================================================================
TEST_F(HashGroupByOpTest, AllDistinctGroups)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 1, 10},
                                       {2, 1, 20},
                                       {3, 1, 30}}));
}

// ============================================================================
// TC8: LargeData - 10000 rows, 100 groups
// ============================================================================
TEST_F(HashGroupByOpTest, LargeData)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 100, i});
  }

  // Build expected: for group g, b values are g, g+100, g+200, ..., g+9900
  // sum = 100*g + (0+100+200+...+9900) = 100*g + 100*(0+1+...+99) = 100*(g + 4950)
  std::vector<TestRow> expected;
  for (int g = 0; g < 100; ++g) {
    int64_t count = 100;
    int64_t sum = 100LL * (g + 4950);
    expected.push_back({g, count, sum});
  }

  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_data(std::move(data))
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC9: SmallBatchSize - batch_size=3, same data as TC3
// ============================================================================
TEST_F(HashGroupByOpTest, SmallBatchSize)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_batch_size(3)
      .with_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60, 10, 30},
                                       {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC10: DualFormatCheck - same data as TC3, explicit dual format check
// ============================================================================
TEST_F(HashGroupByOpTest, DualFormatCheck)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b), MIN(b), MAX(b)")
      .group_by("a")
      .with_data({{1, 10}, {1, 20}, {1, 30}, {2, 5}, {2, 15}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 3, 60, 10, 30},
                                       {2, 2, 20, 5, 15}}));
}

// ============================================================================
// TC11: Avg - single column GROUP BY + AVG(b)
// Group a=1: b values 10,20,30 -> avg=20
// Group a=2: b values 40,50    -> avg=45
// ============================================================================
TEST_F(HashGroupByOpTest, Avg)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, AVG(b)")
      .group_by("a")
      .with_data({{1, 10}, {2, 40}, {1, 20}, {2, 50}, {1, 30}})
      .enable_dual_format_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 20}, {2, 45}}));
}

// ============================================================================
// TC12: Dump - large data with dump enabled
// Same 10000 rows / 100 groups as TC8
// ============================================================================
TEST_F(HashGroupByOpTest, Dump)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 10000; ++i) {
    data.push_back({i % 100, i});
  }

  std::vector<TestRow> expected;
  for (int g = 0; g < 100; ++g) {
    int64_t count = 100;
    int64_t sum = 100LL * (g + 4950);
    expected.push_back({g, count, sum});
  }

  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*), SUM(b)")
      .group_by("a")
      .with_sql_operator_dump(true)
      .with_hash_area_size(64 * 1024)
      .with_data(std::move(data))
      .enable_dual_format_unordered_check().run(engine_);

  EXPECT_TRUE(result.verify_unordered(expected));
}

// ============================================================================
// TC13: LimitPushdown - limit pushed into hash gby (2.0 only)
// 1000 rows, 20 groups; LIMIT 5 -> at most 5 groups returned
// ============================================================================
TEST_F(HashGroupByOpTest, LimitPushdown)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 1000; ++i) {
    data.push_back({i % 20, i});
  }

  OpTestResult result = HashGroupByTestSpec()
      .table("t", "a int, b int")
      .select("a, COUNT(*)")
      .group_by("a")
      .with_limit(5)
      .with_rich_format(true)
      .with_data(std::move(data))
      .run(engine_);

  EXPECT_LE(result.row_count(), 5);
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_hash_groupby_op.log*");
  // OB_LOGGER.set_file_name("test_hash_groupby_op.log", true, true);
  // OB_LOGGER.set_log_level("INFO");
  // common::ObPLogWriterCfg log_cfg;
  // OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

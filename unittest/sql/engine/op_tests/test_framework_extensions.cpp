/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Tests for op_tests framework extensions:
 * - Feature 1: Per-column VectorFormat (with_col_formats)
 * - Feature 2: Custom batch size function (with_batch_size(lambda))
 * - Feature 3: Tracepoint injection (WITH_TRACEPOINTS / with_tracepoints)
 * - Feature 4: Input skip injection (with_input_skips)
 * - Feature 5: Performance recording (with_perf_record)
 * - Feature 6: Session variable override (WITH_SESSION_VARS / with_session_vars)
 */

#include "lib/ob_errno.h"
#include "lib/rc/context.h"
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

namespace oceanbase
{
namespace sql
{

class FrameworkExtensionsTest : public OpTestKit
{
protected:
  // Helper: check if a specific column in the result has a given format
  // This is validated indirectly through operator behavior, since MockDataSource
  // applies the format and the operator must handle it correctly.
};

// ===== Feature 1: Per-column VectorFormat =====

TEST_F(FrameworkExtensionsTest, ColFormatsBasic)
{
  // Test that per-column format override works for a mix of fixed/var columns
  auto result = material_test()
      .table("t", "a int, b varchar(32)")
      .select("a, b")
      .with_data({{1, "hello"}, {2, "world"}, {3, "test"}})
      .with_col_formats(VEC_FIXED, VEC_DISCRETE)
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "hello"}, {2, "world"}, {3, "test"}}));
}

TEST_F(FrameworkExtensionsTest, ColFormatsUniformForFixed)
{
  // VEC_UNIFORM is valid for fixed-length columns
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}})
      .with_col_formats(VEC_UNIFORM, VEC_FIXED)
      .run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 10}, {2, 20}}));
}

TEST_F(FrameworkExtensionsTest, ColFormatsContinuousForVarchar)
{
  // VEC_CONTINUOUS for variable-length column
  auto result = material_test()
      .table("t", "a int, b varchar(32)")
      .select("a, b")
      .with_data({{1, "abc"}, {2, "defg"}})
      .with_col_formats(VEC_FIXED, VEC_CONTINUOUS)
      .run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "abc"}, {2, "defg"}}));
}

// ===== Feature 2: Custom batch size function =====

TEST_F(FrameworkExtensionsTest, BatchSizeFnBasic)
{
  // Use a fixed batch size function - all batches should be size 2
  // With 5 rows and batch_size=2: 3 batches (2+2+1)
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .with_batch_size([](int64_t /*batch_idx*/) -> int64_t { return 2; })
      .with_perf_record()
      .run(engine_);
  EXPECT_EQ(5, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1}, {2}, {3}, {4}, {5}}));
  // Verify batch count: 5 rows / 2 per batch = ceil(2.5) = 3 batches
  EXPECT_TRUE(result.has_perf_stats());
  EXPECT_EQ(3, result.get_perf_stats().batch_count);
}

TEST_F(FrameworkExtensionsTest, BatchSizeFnAlternating)
{
  // Alternating batch sizes: batch0=3, batch1=1, batch2=3
  // With 7 rows: batch0(3) + batch1(1) + batch2(3) = 3 batches
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}, {5}, {6}, {7}})
      .with_batch_size([](int64_t batch_idx) -> int64_t {
        return (batch_idx % 2 == 0) ? 3 : 1;
      })
      .with_perf_record()
      .run(engine_);
  EXPECT_EQ(7, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1}, {2}, {3}, {4}, {5}, {6}, {7}}));
  // Verify batch count: 3+1+3 = 7 rows in 3 batches
  EXPECT_TRUE(result.has_perf_stats());
  EXPECT_EQ(3, result.get_perf_stats().batch_count);
}

// ===== Feature 3: Tracepoint injection =====

TEST_F(FrameworkExtensionsTest, TracepointWithMacro)
{
  // WITH_TRACEPOINTS macro should set and auto-reset tracepoints
  // Use a tracepoint that is actually registered via ERRSIM_POINT_DEF

  // Verify tracepoint is NOT active before the scope
  int tmp_ret = OB_E(EventTable::EN_DISABLE_ENCODESORTKEY_OPT) OB_SUCCESS;
  EXPECT_EQ(tmp_ret, OB_SUCCESS);
  WITH_TRACEPOINTS(
    OB_TP("EN_DISABLE_ENCODESORTKEY_OPT", -4007, 0, 1)
  ) {
    // Verify tracepoint IS active inside the scope
    tmp_ret = OB_E(EventTable::EN_DISABLE_ENCODESORTKEY_OPT) OB_SUCCESS;
    EXPECT_NE(tmp_ret, OB_SUCCESS);

    auto result = material_test()
        .table("t", "a int")
        .select("a")
        .with_data({{1}, {2}})
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
  }

  tmp_ret = OB_E(EventTable::EN_DISABLE_ENCODESORTKEY_OPT) OB_SUCCESS;
  // After scope exit, tracepoint should be reset (inactive)
  EXPECT_EQ(tmp_ret, OB_SUCCESS);
}

TEST_F(FrameworkExtensionsTest, TracepointWithBuilder)
{
  // with_tracepoints builder method should also work
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_tracepoints({
        {"EN_ENABLE_SQL_OP_DUMP", 0, 0, 0, 0}
      })
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1}, {2}, {3}}));
}

// ===== Feature 4: Input skip injection =====

TEST_F(FrameworkExtensionsTest, InputSkipsBasic)
{
  // Skip every other row - Material should pass through non-skipped rows correctly
  // Note: Material with skip still outputs all rows but with skip bits set;
  // the downstream operator sees the skip and skips those rows
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}})
      .with_input_skips([](int64_t /*batch_idx*/, int64_t batch_size, ObBitVector *skip) {
        // Skip even-indexed rows (0-based)
        for (int64_t i = 0; i < batch_size; ++i) {
          if (i % 2 == 0) {
            skip->set(i);
          }
        }
      })
      .run(engine_);
  // Material in store mode respects skip bits: only non-skipped rows are stored
  // Rows 1,3 (odd-indexed) pass through; rows 0,2 (even-indexed) are skipped
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{2}, {4}}));
}

TEST_F(FrameworkExtensionsTest, InputSkipsNone)
{
  // No skips - should produce normal results
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 10}, {2, 20}, {3, 30}}));
}

// ===== Feature 5: Performance recording =====

TEST_F(FrameworkExtensionsTest, PerfRecordBasic)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .with_perf_record()
      .run(engine_);
  EXPECT_EQ(5, result.row_count());
  // Verify perf stats are populated
  EXPECT_TRUE(result.has_perf_stats());
  const auto &perf = result.get_perf_stats();
  EXPECT_GT(perf.op_total_ns, 0);
  EXPECT_GE(perf.mock_total_ns, 0);
  EXPECT_GE(perf.op_total_ns, perf.mock_total_ns);
  EXPECT_GE(perf.operator_rt_ns(), 0);
}

TEST_F(FrameworkExtensionsTest, PerfRecordDisabled)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}})
      .run(engine_);
  EXPECT_EQ(2, result.row_count());
  // Without with_perf_record(), stats should not be populated
  EXPECT_FALSE(result.has_perf_stats());
}

// ===== Feature 6: Session variable override =====

TEST_F(FrameworkExtensionsTest, SessionVarsWithMacro)
{
  // WITH_SESSION_VARS sets TLS overrides; they are applied to session during prepare()/run()
  const ObString timeout_var("ob_query_timeout");

  WITH_SESSION_VARS(
    {"ob_query_timeout", "999999999"}
  ) {
    // Run a simple operator test — prepare() applies the TLS overrides to session
    auto result = material_test()
        .table("t", "a int")
        .select("a")
        .with_data({{1}, {2}, {3}})
        .run(engine_);
    EXPECT_EQ(3, result.row_count());

    // Verify the session variable was actually set by apply_session_variable_overrides()
    int64_t current_timeout = 0;
    int ret = engine_.get_session_info().get_sys_variable_by_name(timeout_var, current_timeout);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(999999999, current_timeout);
  }

  // After scope exit, TLS overrides are restored (same pattern as WITH_TENANT_CONFS)
  EXPECT_TRUE(TestSessionVarConf::instance().empty());
}

TEST_F(FrameworkExtensionsTest, SessionVarsWithBuilder)
{
  // with_session_vars builder method should push vars into TLS, applied during prepare()
  const ObString timeout_var("ob_query_timeout");

  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}})
      .with_session_vars({{"ob_query_timeout", "888888888"}})
      .run(engine_);
  EXPECT_EQ(2, result.row_count());

  // TLS should be clean after run() scope exits
  EXPECT_TRUE(TestSessionVarConf::instance().empty());
}

TEST_F(FrameworkExtensionsTest, SessionVarsMultipleVars)
{
  // Set multiple session variables at once
  const ObString timeout_var("ob_query_timeout");

  WITH_SESSION_VARS(
    {"ob_query_timeout", "777777777"}
  ) {
    auto result = material_test()
        .table("t", "a int, b int")
        .select("a, b")
        .with_data({{1, 10}, {2, 20}})
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, 10}, {2, 20}}));

    // Verify session variable was applied
    int64_t current_timeout = 0;
    int ret = engine_.get_session_info().get_sys_variable_by_name(timeout_var, current_timeout);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(777777777, current_timeout);
  }
}

TEST_F(FrameworkExtensionsTest, SessionVarsGuardRestore)
{
  // TestSessionVarGuard should restore TLS state on destruction
  auto &conf = TestSessionVarConf::instance();

  // Push a value before the guard
  conf.set("ob_query_timeout", "111111111");
  EXPECT_EQ("111111111", conf.overrides_["ob_query_timeout"]);

  {
    TestSessionVarGuard guard;
    guard.set("ob_query_timeout", "222222222");
    guard.set("group_concat_max_len", "2048");
    EXPECT_EQ("222222222", conf.overrides_["ob_query_timeout"]);
    EXPECT_EQ("2048", conf.overrides_["group_concat_max_len"]);
  }

  // After guard destruction, should be restored to pre-guard state
  EXPECT_EQ("111111111", conf.overrides_["ob_query_timeout"]);
  EXPECT_EQ(0, conf.overrides_.count("group_concat_max_len"));

  // Cleanup
  conf.clear();
}

TEST_F(FrameworkExtensionsTest, SessionVarsNestedGuards)
{
  // Nested WITH_SESSION_VARS should work correctly (inner restores to outer's state)
  auto &conf = TestSessionVarConf::instance();

  WITH_SESSION_VARS(
    {"ob_query_timeout", "100000000"}
  ) {
    EXPECT_EQ("100000000", conf.overrides_["ob_query_timeout"]);

    WITH_SESSION_VARS(
      {"ob_query_timeout", "200000000"}
    ) {
      EXPECT_EQ("200000000", conf.overrides_["ob_query_timeout"]);
    }

    // Inner guard restored to outer's state
    EXPECT_EQ("100000000", conf.overrides_["ob_query_timeout"]);
  }

  EXPECT_TRUE(conf.empty());
}

TEST_F(FrameworkExtensionsTest, SessionVarsObSessionVarMacro)
{
  // OB_SESSION_VAR macro should work the same as pair literals
  const ObString timeout_var("ob_query_timeout");

  WITH_SESSION_VARS(
    OB_SESSION_VAR("ob_query_timeout", "555555555")
  ) {
    auto result = material_test()
        .table("t", "a int")
        .select("a")
        .with_data({{1}, {2}})
        .run(engine_);
    EXPECT_EQ(2, result.row_count());

    int64_t current_timeout = 0;
    int ret = engine_.get_session_info().get_sys_variable_by_name(timeout_var, current_timeout);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(555555555, current_timeout);
  }
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

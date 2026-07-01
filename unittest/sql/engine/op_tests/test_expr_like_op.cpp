/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * End-to-end regression for the LIKE 'prefix%' fast-path bug.
 *
 * A literal pattern ending in '%' whose prefix has no '_'/'%'/escape routes
 * ObExprLike to the instr-mode fast path. On x86 with AVX2 (and a utf8mb4_bin
 * comparison collation) a single-segment prefix is matched by
 * ObStringSearcher::start_with -> memequal_opt (SIMD). The SIMD tail compare was
 * broken in two ways:
 *   - prefix length leaving a 1..7 byte tail after the 32-byte blocks (e.g. 34)
 *     did a forward 8-byte read that over-ran the buffer and compared wrong
 *     bytes -> a row that DOES start with the prefix was wrongly excluded
 *     (false negative; this is the originally reported symptom for
 *     LIKE 'insert into t1(c1,c2,c3,c5) values%').
 *   - prefix length in [17,31] left an uncompared middle gap -> a row that does
 *     NOT start with the prefix could be wrongly included (false positive).
 *
 * The column charset in this harness is utf8mb4 (collation utf8mb4_general_ci),
 * so each expression forces the comparison collation with `COLLATE utf8mb4_bin`
 * to reach the SIMD path that set_instr_info only enables for utf8mb4_bin /
 * utf8mb4_0900_bin.
 */

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

class LikeExprOpTest : public OpTestKit
{
protected:
  // Evaluate `c COLLATE utf8mb4_bin LIKE '<prefix>%'` over one varchar column of
  // the given texts; return the per-row result ("1" match / "0" no match).
  std::vector<std::string> run_prefix_like(const std::string &prefix,
                                           const std::vector<std::string> &texts)
  {
    std::vector<TestRow> rows;
    rows.reserve(texts.size());
    for (const std::string &t : texts) {
      rows.emplace_back(std::vector<TestValue>{TestValue(t)});
    }
    std::string expr = "c COLLATE utf8mb4_bin LIKE '" + prefix + "%'";
    auto result = expr_unit_test()
        .columns("c varchar(128)")
        .with_expr(expr.c_str())
        .with_data(rows)
        .run(engine_);
    std::vector<std::string> out;
    for (int64_t i = 0; i < result.row_count(); ++i) {
      out.push_back(result.get_row(i)[0]);
    }
    return out;
  }
};

// TC1: the originally reported case. Prefix length 34 leaves a 2-byte SIMD tail.
// The first row begins with the prefix and MUST match; the buggy fast path
// returned 0 for it.
TEST_F(LikeExprOpTest, ReportedCase_Len34Prefix)
{
  const std::string prefix = "insert into t1(c1,c2,c3,c5) values";  // length 34
  ASSERT_EQ(34u, prefix.size());
  auto r = run_prefix_like(prefix, {
      "insert into t1(c1,c2,c3,c5) values(?,?,?,?)",  // starts with prefix -> 1
      "insert into t1(c1,c2,c3,c5) values",           // exactly prefix, % = empty -> 1
      "insert into t1(c1,c2,c3,c5) value",            // 33 chars, shorter -> 0
      "insert into t1(c1,c2,c3,c5) valuex",           // len 34, differs at tail -> 0
      "insert into t2(c1,c2,c3,c5) values(?)",        // differs inside prefix -> 0
  });
  ASSERT_EQ(5u, r.size());
  EXPECT_EQ("1", r[0]);
  EXPECT_EQ("1", r[1]);
  EXPECT_EQ("0", r[2]);
  EXPECT_EQ("0", r[3]);
  EXPECT_EQ("0", r[4]);
}

// TC2: prefix length 20 falls in [17,31], where the buggy tail left an
// uncompared middle gap [8,12). A row differing only at index 9 does NOT start
// with the prefix and MUST NOT match; the buggy fast path returned 1.
TEST_F(LikeExprOpTest, MiddleGap_Len20Prefix_NoFalsePositive)
{
  const std::string prefix = "ABCDEFGHIJKLMNOPQRST";  // length 20
  ASSERT_EQ(20u, prefix.size());
  auto r = run_prefix_like(prefix, {
      "ABCDEFGHIJKLMNOPQRSTzzzz",  // starts with prefix -> 1
      "ABCDEFGHIXKLMNOPQRSTzzzz",  // index 9 'J'->'X' (in middle gap) -> 0
      "ABCDEFGHIJKLMNOPQRSX",      // index 19 'T'->'X' (prefix last byte) -> 0
      "ABCDEFGHIJKLMNOPQRS",       // 19 chars, shorter -> 0
  });
  ASSERT_EQ(4u, r.size());
  EXPECT_EQ("1", r[0]);
  EXPECT_EQ("0", r[1]);
  EXPECT_EQ("0", r[2]);
  EXPECT_EQ("0", r[3]);
}

// TC3: tail-length boundaries n == 1 (prefix len 33) and n == 7 (prefix len 39);
// a row that begins with the prefix MUST match (the over-read bug failed these).
TEST_F(LikeExprOpTest, TailBoundaries_Len33_Len39)
{
  const std::string p33 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456";          // length 33, tail 1
  const std::string p39 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABC";    // length 39, tail 7
  ASSERT_EQ(33u, p33.size());
  ASSERT_EQ(39u, p39.size());

  auto r33 = run_prefix_like(p33, {
      p33 + "tail",                                 // starts with prefix -> 1
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345X",          // index 32 '6'->'X' -> 0
  });
  ASSERT_EQ(2u, r33.size());
  EXPECT_EQ("1", r33[0]);
  EXPECT_EQ("0", r33[1]);

  auto r39 = run_prefix_like(p39, {
      p39 + "tail",                                 // starts with prefix -> 1
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABX",    // index 38 'C'->'X' -> 0
  });
  ASSERT_EQ(2u, r39.size());
  EXPECT_EQ("1", r39[0]);
  EXPECT_EQ("0", r39[1]);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  oceanbase::common::init_arches();  // enable AVX2/AVX512 runtime dispatch
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

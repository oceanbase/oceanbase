/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

namespace oceanbase
{
namespace sql
{

class NestedLoopJoinOpTest : public OpTestKit {};

// NLJ iterates left rows in order, rescans right for each.
// Output order: left row order, then for each left row right scan order.

// Left: {1,10},{2,20},{3,30}; right: {1,100},{3,300},{5,500}; join on a=c
// outer {1,10} -> match {1,100}; outer {2,20} -> no match; outer {3,30} -> match {3,300}
TEST_F(NestedLoopJoinOpTest, BasicInnerJoin)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}, {3, 300}, {5, 500}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{1, 10, 1, 100}, {3, 30, 3, 300}}));
}

// Left outer: unmatched left rows (2,20) and (4,40) emit NULL on right side
// order: {1,10,1,100}, {2,20,NULL,NULL}, {4,40,NULL,NULL}
TEST_F(NestedLoopJoinOpTest, LeftOuterJoin)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}, {4, 40}})
      .with_right_data({{1, 100}, {3, 300}})
      .join_type(LEFT_OUTER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({
      {1, 10, 1, 100},
      {2, 20, NULL_VAL, NULL_VAL},
      {4, 40, NULL_VAL, NULL_VAL}}));
}

// Note: NLJ does not support RIGHT_OUTER_JOIN or FULL_OUTER_JOIN.
// The optimizer guarantees no RIGHT/FULL OUTER JOIN for NLJ.
// These join types are tested in test_hash_join_op.cpp instead.

// Cross join (no ON clause): all 2×3 = 6 combinations
// order: outer {1} drives inner scan {10},{20},{30}; then outer {2} drives same
TEST_F(NestedLoopJoinOpTest, CrossJoin)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int")
      .right_table("t2", "c int")
      .select("t1.a, t2.c")
      .with_left_data({{1}, {2}})
      .with_right_data({{10}, {20}, {30}})
      .join_type(INNER_JOIN)
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({
      {1, 10}, {1, 20}, {1, 30},
      {2, 10}, {2, 20}, {2, 30}}));
}

// Non-equi join (t1.a > t2.c):
// outer {1,10}: 1>2✗, 1>4✗
// outer {3,30}: 3>2✓ -> {3,30,2,100}; 3>4✗
// outer {5,50}: 5>2✓ -> {5,50,2,100}; 5>4✓ -> {5,50,4,200}
TEST_F(NestedLoopJoinOpTest, NonEqualCondition)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {3, 30}, {5, 50}})
      .with_right_data({{2, 100}, {4, 200}})
      .join_type(INNER_JOIN)
      .on("t1.a > t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({
      {3, 30, 2, 100},
      {5, 50, 2, 100},
      {5, 50, 4, 200}}));
}

// NULL in join key: NULL=1 and NULL=NULL are both false; only (a=1,c=1) matches
TEST_F(NestedLoopJoinOpTest, NullHandling)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {NULL_VAL, 20}})
      .with_right_data({{1, 100}, {NULL_VAL, 200}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{1, 10, 1, 100}}));
}

// No matching keys at all: result must be empty
TEST_F(NestedLoopJoinOpTest, EmptyResult)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}})
      .with_right_data({{3, 300}, {4, 400}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({}));
}

// Rescan: result after rescan must equal the original single-run result
TEST_F(NestedLoopJoinOpTest, Rescan)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}})
      .with_right_data({{1, 100}, {2, 200}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .with_rescan_times(3)
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{1, 10, 1, 100}, {2, 20, 2, 200}}));
}

// Large data: left[i]={i%10, i*10} for i in [0,100);
//             right[j]={j%10, j*100} for j in [0,100)
// join on a=c (i%10 == j%10): each value v in [0,10) has 10 left × 10 right = 100 matches
// total 1000 output rows; use unordered verification since exact ordering is complex
TEST_F(NestedLoopJoinOpTest, LargeDataInnerJoin)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data_generator(100,
          [](int64_t i) -> TestValue { return (int)(i % 10); },
          [](int64_t i) -> TestValue { return (int)(i * 10); })
      .with_right_data_generator(100,
          [](int64_t i) -> TestValue { return (int)(i % 10); },
          [](int64_t i) -> TestValue { return (int)(i * 100); })
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  // Build expected: for each left row i and each right row j where i%10 == j%10
  std::vector<TestRow> expected;
  expected.reserve(1000);
  for (int i = 0; i < 100; ++i) {
    int a = i % 10, b = i * 10;
    for (int j = 0; j < 100; ++j) {
      if (j % 10 == a) {
        expected.push_back({a, b, j % 10, j * 100});
      }
    }
  }
  EXPECT_TRUE(result.verify_unordered(expected));
}

// Dual format check: same as BasicInnerJoin, verifies 1.0 == 2.0 output
TEST_F(NestedLoopJoinOpTest, DualFormatCheck)
{
  auto result = nested_loop_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}, {3, 300}, {5, 500}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .enable_dual_format_check()
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{1, 10, 1, 100}, {3, 30, 3, 300}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

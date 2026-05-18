/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

namespace oceanbase
{
namespace sql
{

class HashJoinOpTest : public OpTestKit {};

// Inner join: t1.a in {1,2,3}, t2.c in {1,3,5}
// matches: (1,10,1,100), (3,30,3,300)
TEST_F(HashJoinOpTest, BasicInnerJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}, {3, 300}, {5, 500}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10, 1, 100}, {3, 30, 3, 300}}));
}

// Left outer: unmatched left rows (2,20) and (4,40) get NULL on right side
TEST_F(HashJoinOpTest, LeftOuterJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}, {4, 40}})
      .with_right_data({{1, 100}, {3, 300}})
      .join_type(LEFT_OUTER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({
      {1, 10, 1, 100},
      {2, 20, NULL_VAL, NULL_VAL},
      {4, 40, NULL_VAL, NULL_VAL}}));
}

// Right outer: unmatched right rows (2,200) and (4,400) get NULL on left side
TEST_F(HashJoinOpTest, RightOuterJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {3, 30}})
      .with_right_data({{1, 100}, {2, 200}, {4, 400}})
      .join_type(RIGHT_OUTER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({
      {1, 10, 1, 100},
      {NULL_VAL, NULL_VAL, 2, 200},
      {NULL_VAL, NULL_VAL, 4, 400}}));
}

// Full outer: matched (1,10,1,100), left-only (2,20), right-only (3,300)
TEST_F(HashJoinOpTest, FullOuterJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}})
      .with_right_data({{1, 100}, {3, 300}})
      .join_type(FULL_OUTER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({
      {1, 10, 1, 100},
      {2, 20, NULL_VAL, NULL_VAL},
      {NULL_VAL, NULL_VAL, 3, 300}}));
}

// Left semi: left rows that have at least one match (a=1, a=3); each returned once
TEST_F(HashJoinOpTest, LeftSemiJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}, {1, 200}, {3, 300}})
      .join_type(LEFT_SEMI_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10}, {3, 30}}));
}

// Right semi: right rows that have at least one match (c=1, c=3)
TEST_F(HashJoinOpTest, RightSemiJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t2.c, t2.d")
      .with_left_data({{1, 10}, {1, 20}, {3, 30}})
      .with_right_data({{1, 100}, {2, 200}, {3, 300}})
      .join_type(RIGHT_SEMI_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 100}, {3, 300}}));
}

// Left anti: left rows with no match in right (only a=2)
TEST_F(HashJoinOpTest, LeftAntiJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}, {3, 300}})
      .join_type(LEFT_ANTI_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{2, 20}}));
}

// Right anti: right rows with no match in left (only c=2)
TEST_F(HashJoinOpTest, RightAntiJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t2.c, t2.d")
      .with_left_data({{1, 10}, {3, 30}})
      .with_right_data({{1, 100}, {2, 200}, {3, 300}})
      .join_type(RIGHT_ANTI_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{2, 200}}));
}

// NAAJ: right side has NULL → all left rows are eliminated
TEST_F(HashJoinOpTest, NullAwareAntiJoinBasic)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}, {NULL_VAL, 200}})
      .join_type(LEFT_ANTI_JOIN)
      .enable_naaj()
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({}));
}

// NAAJ with no NULLs in right: behaves like regular anti join (a=2, a=3 have no match)
TEST_F(HashJoinOpTest, NullAwareAntiJoinNoNull)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}})
      .join_type(LEFT_ANTI_JOIN)
      .enable_naaj()
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{2, 20}, {3, 30}}));
}

// Multi-key join: t1.a=t2.c AND t1.b=t2.d
// right_table has 2 columns (c, d); extra 3rd element in with_right_data is ignored
// right rows: {c=1,d=10}, {c=1,d=20}, {c=2,d=10}
// each left row matches exactly one right row
TEST_F(HashJoinOpTest, MultiKeyJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {1, 20}, {2, 10}})
      .with_right_data({{1, 10, 100}, {1, 20, 200}, {2, 10, 300}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c AND t1.b = t2.d")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({
      {1, 10, 1, 10},
      {1, 20, 1, 20},
      {2, 10, 2, 10}}));
}

// NULL in join key: NULL=NULL is false, so only (a=1, c=1) matches
TEST_F(HashJoinOpTest, NullHandling)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {NULL_VAL, 20}, {3, NULL_VAL}})
      .with_right_data({{1, 100}, {NULL_VAL, 200}, {4, 400}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10, 1, 100}}));
}

// Duplicate keys: 2 left × 2 right = 4 output rows (Cartesian on matching key)
TEST_F(HashJoinOpTest, DuplicateKeys)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {1, 20}})
      .with_right_data({{1, 100}, {1, 200}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({
      {1, 10, 1, 100},
      {1, 10, 1, 200},
      {1, 20, 1, 100},
      {1, 20, 1, 200}}));
}

// Rescan: result should be the same as a single run
TEST_F(HashJoinOpTest, Rescan)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}})
      .with_right_data({{1, 100}, {2, 200}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .with_rescan_times(3)
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10, 1, 100}, {2, 20, 2, 200}}));
}

// Large data: left[i]={i, i*10}, right[i]={i, i*100}; each row i matches exactly once
// expected output set: {i, i*10, i, i*100} for i in [0, 1000)
TEST_F(HashJoinOpTest, LargeDataInnerJoin)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data_generator(1000,
          [](int64_t i) -> TestValue { return (int)(i); },
          [](int64_t i) -> TestValue { return (int)(i * 10); })
      .with_right_data_generator(1000,
          [](int64_t i) -> TestValue { return (int)(i); },
          [](int64_t i) -> TestValue { return (int)(i * 100); })
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered(1000,
      gen::sequential(0),       // a = i
      gen::sequential(0, 10),   // b = i * 10
      gen::sequential(0),       // c = i
      gen::sequential(0, 100))); // d = i * 100
}

// Dual format check: same as BasicInnerJoin but verifies 1.0 == 2.0
TEST_F(HashJoinOpTest, DualFormatCheck)
{
  auto result = hash_join_test()
      .left_table("t1", "a int, b int")
      .right_table("t2", "c int, d int")
      .select("t1.a, t1.b, t2.c, t2.d")
      .with_left_data({{1, 10}, {2, 20}, {3, 30}})
      .with_right_data({{1, 100}, {3, 300}, {5, 500}})
      .join_type(INNER_JOIN)
      .on("t1.a = t2.c")
      .enable_dual_format_unordered_check()
      .run(engine_);

  EXPECT_TRUE(result.verify_unordered({{1, 10, 1, 100}, {3, 30, 3, 300}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

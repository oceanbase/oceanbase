/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_scalar_aggregate.h"
#include "unittest/sql/engine/op_tests/ob_op_test_merge_groupby.h"
#include "unittest/sql/engine/op_tests/ob_op_test_hash_groupby.h"

namespace oceanbase
{
namespace sql
{

class StringAggOpTest : public OpTestKit {};

// ============================================================================
// Scalar Aggregate tests
// STRING_AGG is resolved to GROUP_CONCAT internally; scalar agg = no GROUP BY.
// ============================================================================

// Basic STRING_AGG with comma separator, rows concatenated in input order.
TEST_F(StringAggOpTest, Scalar_BasicComma)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"apple"}, {"banana"}, {"cherry"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"apple,banana,cherry"}}));
}

// Pipe separator with spaces.
TEST_F(StringAggOpTest, Scalar_PipeSeparator)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ' | ')")
      .with_data({{"a"}, {"b"}, {"c"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"a | b | c"}}));
}

// Empty-string separator produces direct concatenation without delimiter.
TEST_F(StringAggOpTest, Scalar_EmptySeparator)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, '')")
      .with_data({{"foo"}, {"bar"}, {"baz"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"foobarbaz"}}));
}

// NULL values in the first argument are silently skipped.
TEST_F(StringAggOpTest, Scalar_NullSkipped)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"hello"}, {TestValue::null()}, {"world"}, {TestValue::null()}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello,world"}}));
}

// When every row is NULL the result is NULL (no non-null value to aggregate).
TEST_F(StringAggOpTest, Scalar_AllNull)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{TestValue::null()}, {TestValue::null()}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"NULL"}}));
}

// Empty table: STRING_AGG returns NULL (consistent with GROUP_CONCAT).
TEST_F(StringAggOpTest, Scalar_EmptyTable)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"NULL"}}));
}

// Single row: no separator appended, result equals the single value.
TEST_F(StringAggOpTest, Scalar_SingleRow)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"only"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"only"}}));
}

// ORDER BY inside STRING_AGG — ascending.
TEST_F(StringAggOpTest, Scalar_OrderByAsc)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',' ORDER BY val ASC)")
      .with_data({{"cherry"}, {"apple"}, {"banana"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"apple,banana,cherry"}}));
}

// ORDER BY inside STRING_AGG — descending.
TEST_F(StringAggOpTest, Scalar_OrderByDesc)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',' ORDER BY val DESC)")
      .with_data({{"apple"}, {"cherry"}, {"banana"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"cherry,banana,apple"}}));
}

// ORDER BY a separate numeric column.
TEST_F(StringAggOpTest, Scalar_OrderByNumericCol)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50), num int")
      .select("STRING_AGG(val, ',' ORDER BY num ASC)")
      .with_data({{"c", 3}, {"a", 1}, {"b", 2}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"a,b,c"}}));
}

// Multiple aggregates in one select — STRING_AGG alongside COUNT and SUM.
TEST_F(StringAggOpTest, Scalar_WithOtherAggrs)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50), num int")
      .select("COUNT(*), STRING_AGG(val, ';'), SUM(num)")
      .with_data({{"x", 1}, {"y", 2}, {"z", 3}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"3", "x;y;z", "6"}}));
}

// STRING_AGG and GROUP_CONCAT produce identical output (both use comma default).
TEST_F(StringAggOpTest, Scalar_GroupConcatEquiv)
{
  OpTestResult r_string_agg = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"a"}, {"b"}, {"c"}})
      .run(engine_);

  OpTestResult r_group_concat = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("GROUP_CONCAT(val SEPARATOR ',')")
      .with_data({{"a"}, {"b"}, {"c"}})
      .run(engine_);

  EXPECT_EQ(r_string_agg.row_count(), r_group_concat.row_count());
  EXPECT_TRUE(r_string_agg.equals(r_group_concat));
}

// Small batch size to exercise multi-batch accumulation.
TEST_F(StringAggOpTest, Scalar_SmallBatchSize)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_batch_size(2)
      .with_data({{"a"}, {"b"}, {"c"}, {"d"}, {"e"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"a,b,c,d,e"}}));
}

// ============================================================================
// Merge Group By tests
// Input must be pre-sorted by the GROUP BY key.
// ============================================================================

// Basic: two groups, concatenation in input order.
TEST_F(StringAggOpTest, Merge_BasicGroupBy)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data({{1, "a"}, {1, "b"}, {2, "x"}, {2, "y"}}, "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "a,b"}, {2, "x,y"}}));
}

// Multiple groups, each with a single value (separator never applied).
TEST_F(StringAggOpTest, Merge_SingleRowPerGroup)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data({{1, "only1"}, {2, "only2"}, {3, "only3"}}, "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "only1"}, {2, "only2"}, {3, "only3"}}));
}

// ORDER BY a separate numeric column within each group.
TEST_F(StringAggOpTest, Merge_WithOrderByNum)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50), num int")
      .select("grp, STRING_AGG(val, ',' ORDER BY num ASC)")
      .group_by("grp")
      .with_sorted_data(
          {{1, "cherry", 3}, {1, "apple", 1}, {1, "banana", 2},
           {2, "dog", 2},    {2, "cat", 1}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "apple,banana,cherry"}, {2, "cat,dog"}}));
}

// ORDER BY val DESC within each group.
TEST_F(StringAggOpTest, Merge_WithOrderByDesc)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',' ORDER BY val DESC)")
      .group_by("grp")
      .with_sorted_data(
          {{1, "apple"}, {1, "cherry"}, {1, "banana"},
           {2, "zoo"},   {2, "ant"}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "cherry,banana,apple"}, {2, "zoo,ant"}}));
}

// NULL values in the aggregated column are ignored; group still produces a result.
TEST_F(StringAggOpTest, Merge_NullInGroup)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "hello"}, {1, TestValue::null()}, {1, "world"},
           {2, TestValue::null()}, {2, "ok"}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "hello,world"}, {2, "ok"}}));
}

// When all values in a group are NULL, that group's STRING_AGG is NULL.
TEST_F(StringAggOpTest, Merge_AllNullInGroup)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "a"},
           {2, TestValue::null()}, {2, TestValue::null()}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "a"}, {2, "NULL"}}));
}

// Empty input produces no output rows (GROUP BY with no rows).
TEST_F(StringAggOpTest, Merge_EmptyInput)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_data({})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// Pipe separator in a grouped context.
TEST_F(StringAggOpTest, Merge_PipeSeparator)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ' | ')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "dog"}, {1, "elephant"},
           {2, "fig"}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "dog | elephant"}, {2, "fig"}}));
}

// STRING_AGG alongside COUNT(*) in the same GROUP BY.
TEST_F(StringAggOpTest, Merge_WithOtherAggrs)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50), num int")
      .select("grp, COUNT(*), STRING_AGG(val, ';'), SUM(num)")
      .group_by("grp")
      .with_sorted_data(
          {{1, "a", 10}, {1, "b", 20},
           {2, "x", 5},  {2, "y", 15}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 2, "a;b", 30}, {2, 2, "x;y", 20}}));
}

// Small batch size exercises cross-batch group accumulation.
TEST_F(StringAggOpTest, Merge_SmallBatchSize)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_batch_size(2)
      .with_sorted_data(
          {{1, "a"}, {1, "b"}, {1, "c"},
           {2, "x"}, {2, "y"}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "a,b,c"}, {2, "x,y"}}));
}

// ============================================================================
// Hash Group By tests
// NOTE: GROUP_CONCAT/STRING_AGG accumulation is not yet supported in the 2.0
// vec path for HashGroupBy (only the last-processed row survives per group).
// All Hash_ tests therefore run with rich_format=false (1.0 datum path).
// Input order is arbitrary; use ORDER BY inside STRING_AGG for determinism.
// ============================================================================

// Basic hash group by — ORDER BY val inside for deterministic output.
TEST_F(StringAggOpTest, Hash_BasicGroupBy)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
      .group_by("grp")
      .with_data({{1, "b"}, {2, "y"}, {1, "a"}, {2, "x"}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "a,b"}, {2, "x,y"}}));
}

// Three groups, each row arrives out of group order.
TEST_F(StringAggOpTest, Hash_MultiGroup)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
      .group_by("grp")
      .with_data(
          {{3, "fig"},  {1, "apple"}, {2, "dog"},
           {1, "cherry"}, {2, "elephant"}, {1, "banana"}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_unordered({
      {1, "apple,banana,cherry"},
      {2, "dog,elephant"},
      {3, "fig"}
  }));
}

// Single row per group: separator is never inserted.
TEST_F(StringAggOpTest, Hash_SingleRowPerGroup)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_data({{1, "solo1"}, {2, "solo2"}, {3, "solo3"}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "solo1"}, {2, "solo2"}, {3, "solo3"}}));
}

// NULL values are ignored; non-null values of the group still aggregate.
TEST_F(StringAggOpTest, Hash_NullInGroup)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
      .group_by("grp")
      .with_data(
          {{1, "hello"}, {1, TestValue::null()}, {1, "world"},
           {2, TestValue::null()}, {2, "ok"}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "hello,world"}, {2, "ok"}}));
}

// Group where all values are NULL → STRING_AGG returns NULL for that group.
TEST_F(StringAggOpTest, Hash_AllNullInGroup)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_data(
          {{1, "a"},
           {2, TestValue::null()}, {2, TestValue::null()}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "a"}, {2, "NULL"}}));
}

// Empty input produces no output rows.
TEST_F(StringAggOpTest, Hash_EmptyInput)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_data({})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// ORDER BY numeric column descending inside STRING_AGG.
TEST_F(StringAggOpTest, Hash_OrderByNumDesc)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50), num int")
      .select("grp, STRING_AGG(val, ',' ORDER BY num DESC)")
      .group_by("grp")
      .with_data(
          {{1, "a", 1}, {1, "b", 2}, {1, "c", 3},
           {2, "x", 10}, {2, "y", 20}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "c,b,a"}, {2, "y,x"}}));
}

// STRING_AGG alongside COUNT(*) in hash group by.
TEST_F(StringAggOpTest, Hash_WithOtherAggrs)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50), num int")
      .select("grp, COUNT(*), STRING_AGG(val, ';' ORDER BY val ASC), SUM(num)")
      .group_by("grp")
      .with_data(
          {{1, "b", 20}, {2, "x", 5},
           {1, "a", 10}, {2, "y", 15}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, 2, "a;b", 30}, {2, 2, "x;y", 20}}));
}

// Small batch size: cross-batch group accumulation in hash groupby.
TEST_F(StringAggOpTest, Hash_SmallBatchSize)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
      .group_by("grp")
      .with_batch_size(2)
      .with_data(
          {{1, "c"}, {1, "a"}, {2, "z"},
           {2, "m"}, {1, "b"}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "a,b,c"}, {2, "m,z"}}));
}

// ============================================================================
// Character Set tests (utf8mb4)
// STRING_AGG must correctly handle multi-byte encodings: CJK (3 bytes/char),
// emoji (4 bytes/char in utf8mb4), Latin-extended (2 bytes/char), and mixed.
// All tests run under the default utf8mb4 session charset.
// ============================================================================

// CJK characters (3 bytes/char in UTF-8): basic scalar concatenation.
TEST_F(StringAggOpTest, Charset_UTF8_CJK_Basic)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"苹果"}, {"香蕉"}, {"樱桃"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"苹果,香蕉,樱桃"}}));
}

// CJK separator (U+3001 IDEOGRAPHIC COMMA, 3 bytes): separator is itself multi-byte.
TEST_F(StringAggOpTest, Charset_UTF8_CJK_Separator)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, '、')")
      .with_data({{"甲"}, {"乙"}, {"丙"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"甲、乙、丙"}}));
}

// CJK ORDER BY a numeric column: encoding is preserved, order is deterministic.
TEST_F(StringAggOpTest, Charset_UTF8_CJK_OrderByNum)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50), num int")
      .select("STRING_AGG(val, ',' ORDER BY num ASC)")
      .with_data({{"丙", 3}, {"甲", 1}, {"乙", 2}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"甲,乙,丙"}}));
}

// CJK NULL handling: NULL rows are skipped, remaining CJK values are intact.
TEST_F(StringAggOpTest, Charset_UTF8_CJK_NullSkipped)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"你好"}, {TestValue::null()}, {"世界"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"你好,世界"}}));
}

// 4-byte emoji (utf8mb4-specific, U+1F600 etc.): verify 4-byte sequences handled.
TEST_F(StringAggOpTest, Charset_UTF8_Emoji_Basic)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, '-')")
      .with_data({{"😀"}, {"🎉"}, {"🌟"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"😀-🎉-🌟"}}));
}

// 4-byte emoji as separator: separator itself is 4 bytes.
TEST_F(StringAggOpTest, Charset_UTF8_Emoji_Separator)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, '🔥')")
      .with_data({{"hello"}, {"world"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello🔥world"}}));
}

// Latin-extended characters (2 bytes/char in UTF-8): é, ü, ñ etc.
TEST_F(StringAggOpTest, Charset_UTF8_Latin_Extended)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ', ')")
      .with_data({{"café"}, {"résumé"}, {"naïve"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"café, résumé, naïve"}}));
}

// Mixed ASCII and CJK: encoding boundaries must not bleed across values.
TEST_F(StringAggOpTest, Charset_UTF8_Mixed_ASCII_CJK)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, '|')")
      .with_data({{"hello"}, {"世界"}, {"world"}, {"你好"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello|世界|world|你好"}}));
}

// Mixed ASCII and emoji: 1-byte and 4-byte sequences interleaved.
TEST_F(StringAggOpTest, Charset_UTF8_Mixed_ASCII_Emoji)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"ok"}, {"😀"}, {"done"}, {"🎉"}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"ok,😀,done,🎉"}}));
}

// MergeGroupBy with CJK values and ASCII separator.
TEST_F(StringAggOpTest, Charset_Merge_CJK_GroupBy)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, '|')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "北京"}, {1, "上海"},
           {2, "广州"}, {2, "深圳"}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "北京|上海"}, {2, "广州|深圳"}}));
}

// MergeGroupBy with CJK values, CJK separator, ORDER BY numeric col.
TEST_F(StringAggOpTest, Charset_Merge_CJK_OrderByNum)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50), num int")
      .select("grp, STRING_AGG(val, '、' ORDER BY num ASC)")
      .group_by("grp")
      .with_sorted_data(
          {{1, "丙", 3}, {1, "甲", 1}, {1, "乙", 2},
           {2, "秋", 3}, {2, "春", 1}, {2, "夏", 2}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "甲、乙、丙"}, {2, "春、夏、秋"}}));
}

// MergeGroupBy: one group has only NULL CJK values → STRING_AGG is NULL.
TEST_F(StringAggOpTest, Charset_Merge_CJK_AllNullGroup)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "你好"},
           {2, TestValue::null()}, {2, TestValue::null()}},
          "grp ASC")
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "你好"}, {2, "NULL"}}));
}

// HashGroupBy with CJK values, ORDER BY num for determinism; 1.0 path only.
TEST_F(StringAggOpTest, Charset_Hash_CJK_GroupBy)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50), num int")
      .select("grp, STRING_AGG(val, ',' ORDER BY num ASC)")
      .group_by("grp")
      .with_data(
          {{1, "上海", 2}, {2, "深圳", 2},
           {1, "北京", 1}, {2, "广州", 1}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "北京,上海"}, {2, "广州,深圳"}}));
}

// HashGroupBy with emoji values, ORDER BY num; 1.0 path only.
TEST_F(StringAggOpTest, Charset_Hash_Emoji_GroupBy)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50), num int")
      .select("grp, STRING_AGG(val, '-' ORDER BY num ASC)")
      .group_by("grp")
      .with_data(
          {{1, "🎉", 2}, {1, "😀", 1},
           {2, "🌟", 2}, {2, "🔥", 1}})
      .with_rich_format(false).run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "😀-🎉"}, {2, "🔥-🌟"}}));
}

// ============================================================================
// Execution Format Explicit Tests
// Three orthogonal execution paths for each operator context:
//   Vec 1.0  : datum/flat format   -- .with_rich_format(false)
//   Vec 2.0  : rich vector format  -- .with_rich_format(true)
//   Non-vec  : _rowsets_enabled=False via WITH_TENANT_CONFS
// ============================================================================

// Scalar / Vec 1.0
TEST_F(StringAggOpTest, Fmt_Scalar_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"a"}, {"b"}, {"c"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"a,b,c"}}));
}

// Scalar / Vec 2.0
TEST_F(StringAggOpTest, Fmt_Scalar_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val varchar(50)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"a"}, {"b"}, {"c"}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"a,b,c"}}));
}

// Scalar / Non-vectorized (_rowsets_enabled=False)
TEST_F(StringAggOpTest, Fmt_Scalar_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = ScalarAggTestSpec()
        .table("t", "val varchar(50)")
        .select("STRING_AGG(val, ',')")
        .with_data({{"a"}, {"b"}, {"c"}})
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"a,b,c"}}));
  }
}

// Scalar / Non-vec + ORDER BY
TEST_F(StringAggOpTest, Fmt_Scalar_NonVec_OrderBy)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = ScalarAggTestSpec()
        .table("t", "val varchar(50), num int")
        .select("STRING_AGG(val, ',' ORDER BY num ASC)")
        .with_data({{"c", 3}, {"a", 1}, {"b", 2}})
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"a,b,c"}}));
  }
}

// Merge / Vec 1.0
TEST_F(StringAggOpTest, Fmt_Merge_Vec1)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data({{1, "a"}, {1, "b"}, {2, "x"}}, "grp ASC")
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "a,b"}, {2, "x"}}));
}

// Merge / Vec 2.0
TEST_F(StringAggOpTest, Fmt_Merge_Vec2)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data({{1, "a"}, {1, "b"}, {2, "x"}}, "grp ASC")
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "a,b"}, {2, "x"}}));
}

// Merge / Non-vectorized
TEST_F(StringAggOpTest, Fmt_Merge_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = MergeGroupByTestSpec()
        .table("t", "grp int, val varchar(50)")
        .select("grp, STRING_AGG(val, ',')")
        .group_by("grp")
        .with_sorted_data({{1, "a"}, {1, "b"}, {1, "c"}, {2, "x"}}, "grp ASC")
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, "a,b,c"}, {2, "x"}}));
  }
}

// Merge / Non-vec + NULL rows
TEST_F(StringAggOpTest, Fmt_Merge_NonVec_Null)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = MergeGroupByTestSpec()
        .table("t", "grp int, val varchar(50)")
        .select("grp, STRING_AGG(val, ',')")
        .group_by("grp")
        .with_sorted_data(
            {{1, "a"}, {1, TestValue::null()}, {1, "b"},
             {2, TestValue::null()}, {2, TestValue::null()}},
            "grp ASC")
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, "a,b"}, {2, "NULL"}}));
  }
}

// Hash / Vec 1.0 — Hash+Vec2 matrix row omitted (concat agg disables hash in get_valid_aggr_algo).
TEST_F(StringAggOpTest, Fmt_Hash_Vec1)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val varchar(50)")
      .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
      .group_by("grp")
      .with_data({{1, "b"}, {1, "a"}, {2, "y"}, {2, "x"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "a,b"}, {2, "x,y"}}));
}

// Hash / Non-vectorized
TEST_F(StringAggOpTest, Fmt_Hash_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = HashGroupByTestSpec()
        .table("t", "grp int, val varchar(50)")
        .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
        .group_by("grp")
        .with_data({{1, "c"}, {2, "y"}, {1, "a"}, {2, "x"}, {1, "b"}})
        .with_rich_format(false).run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_unordered({{1, "a,b,c"}, {2, "x,y"}}));
  }
}

// ============================================================================
// Data Type Tests
// STRING_AGG over CHAR, TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT.
// Each type is tested across all three execution formats where applicable.
// ============================================================================

// CHAR(20) / Scalar / Vec 1.0
TEST_F(StringAggOpTest, Type_Char_Scalar_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val char(20)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"apple"}, {"banana"}, {"cherry"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"apple,banana,cherry"}}));
}

// CHAR(20) / Scalar / Vec 2.0
TEST_F(StringAggOpTest, Type_Char_Scalar_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val char(20)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"apple"}, {"banana"}, {"cherry"}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"apple,banana,cherry"}}));
}

// CHAR(20) / Scalar / Non-vec
TEST_F(StringAggOpTest, Type_Char_Scalar_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = ScalarAggTestSpec()
        .table("t", "val char(20)")
        .select("STRING_AGG(val, ',')")
        .with_data({{"apple"}, {"banana"}, {"cherry"}})
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"apple,banana,cherry"}}));
  }
}

// CHAR(20) / Scalar / NULL handling
TEST_F(StringAggOpTest, Type_Char_Scalar_Null)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val char(20)")
      .select("STRING_AGG(val, ',')")
      .with_data({{"a"}, {TestValue::null()}, {"c"}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"a,c"}}));
}

// CHAR(20) / Merge / Vec 1.0
TEST_F(StringAggOpTest, Type_Char_Merge_Vec1)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val char(20)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data({{1, "foo"}, {1, "bar"}, {2, "baz"}}, "grp ASC")
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "foo,bar"}, {2, "baz"}}));
}

// CHAR(20) / Merge / Vec 2.0
TEST_F(StringAggOpTest, Type_Char_Merge_Vec2)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val char(20)")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data({{1, "foo"}, {1, "bar"}, {2, "baz"}}, "grp ASC")
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "foo,bar"}, {2, "baz"}}));
}

// CHAR(20) / Hash / Vec 1.0
TEST_F(StringAggOpTest, Type_Char_Hash_Vec1)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val char(20)")
      .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
      .group_by("grp")
      .with_data({{1, "b"}, {1, "a"}, {2, "y"}, {2, "x"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "a,b"}, {2, "x,y"}}));
}

// TEXT / Scalar / Vec 1.0
TEST_F(StringAggOpTest, Type_Text_Scalar_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val text")
      .select("STRING_AGG(val, ',')")
      .with_data({{"hello"}, {"world"}, {"text"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello,world,text"}}));
}

// TEXT / Scalar / Vec 2.0
TEST_F(StringAggOpTest, Type_Text_Scalar_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val text")
      .select("STRING_AGG(val, ',')")
      .with_data({{"hello"}, {"world"}, {"text"}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello,world,text"}}));
}

// TEXT / Scalar / Non-vec
TEST_F(StringAggOpTest, Type_Text_Scalar_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = ScalarAggTestSpec()
        .table("t", "val text")
        .select("STRING_AGG(val, ',')")
        .with_data({{"hello"}, {"world"}, {"text"}})
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"hello,world,text"}}));
  }
}

// TEXT / Merge / Vec 1.0
TEST_F(StringAggOpTest, Type_Text_Merge_Vec1)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val text")
      .select("grp, STRING_AGG(val, ',')")
      .group_by("grp")
      .with_sorted_data({{1, "long text value"}, {1, "another"}, {2, "third"}}, "grp ASC")
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "long text value,another"}, {2, "third"}}));
}

// TEXT / Hash / Vec 1.0
TEST_F(StringAggOpTest, Type_Text_Hash_Vec1)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val text")
      .select("grp, STRING_AGG(val, ',' ORDER BY val ASC)")
      .group_by("grp")
      .with_data({{1, "beta"}, {1, "alpha"}, {2, "delta"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "alpha,beta"}, {2, "delta"}}));
}

// TINYTEXT / Scalar / Vec 1.0
TEST_F(StringAggOpTest, Type_TinyText_Scalar_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val tinytext")
      .select("STRING_AGG(val, '-')")
      .with_data({{"tiny"}, {"text"}, {"type"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"tiny-text-type"}}));
}

// TINYTEXT / Scalar / Vec 2.0
TEST_F(StringAggOpTest, Type_TinyText_Scalar_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val tinytext")
      .select("STRING_AGG(val, '-')")
      .with_data({{"tiny"}, {"text"}, {"type"}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"tiny-text-type"}}));
}

// MEDIUMTEXT / Scalar / Vec 1.0
TEST_F(StringAggOpTest, Type_MediumText_Scalar_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val mediumtext")
      .select("STRING_AGG(val, ',')")
      .with_data({{"medium"}, {"text"}, {"column"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"medium,text,column"}}));
}

// MEDIUMTEXT / Scalar / Vec 2.0
TEST_F(StringAggOpTest, Type_MediumText_Scalar_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val mediumtext")
      .select("STRING_AGG(val, ',')")
      .with_data({{"medium"}, {"text"}, {"column"}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"medium,text,column"}}));
}

// LONGTEXT / Scalar / Vec 1.0
TEST_F(StringAggOpTest, Type_LongText_Scalar_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val longtext")
      .select("STRING_AGG(val, ',')")
      .with_data({{"long"}, {"text"}, {"column"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"long,text,column"}}));
}

// LONGTEXT / Scalar / Vec 2.0
TEST_F(StringAggOpTest, Type_LongText_Scalar_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val longtext")
      .select("STRING_AGG(val, ',')")
      .with_data({{"long"}, {"text"}, {"column"}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"long,text,column"}}));
}

// ============================================================================
// Orthogonal Matrix Tests
// Cross-product of (operator × data_type × charset × separator × NULL × format)
// for combinations not already covered in earlier sections.
// ============================================================================

// Merge + CHAR + CJK values + CJK separator + Vec 2.0
TEST_F(StringAggOpTest, Ortho_Merge_Char_CJK_CJKSep_Vec2)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val char(30)")
      .select("grp, STRING_AGG(val, '、')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "甲"}, {1, "乙"}, {1, "丙"},
           {2, "春"}, {2, "夏"}},
          "grp ASC")
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "甲、乙、丙"}, {2, "春、夏"}}));
}

// Merge + CHAR + CJK values + CJK separator + Non-vec
TEST_F(StringAggOpTest, Ortho_Merge_Char_CJK_CJKSep_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = MergeGroupByTestSpec()
        .table("t", "grp int, val char(30)")
        .select("grp, STRING_AGG(val, '、')")
        .group_by("grp")
        .with_sorted_data(
            {{1, "甲"}, {1, "乙"}, {1, "丙"},
             {2, "春"}, {2, "夏"}},
            "grp ASC")
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, "甲、乙、丙"}, {2, "春、夏"}}));
  }
}

// Scalar + TEXT + emoji values + emoji separator + Vec 1.0
TEST_F(StringAggOpTest, Ortho_Scalar_Text_Emoji_EmojiSep_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val text")
      .select("STRING_AGG(val, '🔥')")
      .with_data({{"😀"}, {"🎉"}, {"🌟"}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"😀🔥🎉🔥🌟"}}));
}

// Scalar + TEXT + emoji values + emoji separator + Vec 2.0
TEST_F(StringAggOpTest, Ortho_Scalar_Text_Emoji_EmojiSep_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val text")
      .select("STRING_AGG(val, '🔥')")
      .with_data({{"😀"}, {"🎉"}, {"🌟"}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"😀🔥🎉🔥🌟"}}));
}

// Hash + CHAR + Latin-extended + empty separator + Vec 1.0 + NULL rows
TEST_F(StringAggOpTest, Ortho_Hash_Char_Latin_EmptySep_Vec1_Null)
{
  OpTestResult result = HashGroupByTestSpec()
      .table("t", "grp int, val char(30)")
      .select("grp, STRING_AGG(val, '' ORDER BY val ASC)")
      .group_by("grp")
      .with_data(
          {{1, "café"}, {1, TestValue::null()}, {1, "naïve"},
           {2, "résumé"}, {2, TestValue::null()}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_unordered({{1, "cafénaïve"}, {2, "résumé"}}));
}

// Scalar + LONGTEXT + mixed ASCII/CJK + emoji separator + ORDER BY + Vec 1.0
TEST_F(StringAggOpTest, Ortho_Scalar_LongText_Mixed_EmojiSep_OrderBy_Vec1)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val longtext, num int")
      .select("STRING_AGG(val, '🔥' ORDER BY num ASC)")
      .with_data({{"世界", 3}, {"hello", 1}, {"你好", 2}})
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello🔥你好🔥世界"}}));
}

// Scalar + LONGTEXT + mixed ASCII/CJK + emoji separator + ORDER BY + Vec 2.0
TEST_F(StringAggOpTest, Ortho_Scalar_LongText_Mixed_EmojiSep_OrderBy_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val longtext, num int")
      .select("STRING_AGG(val, '🔥' ORDER BY num ASC)")
      .with_data({{"世界", 3}, {"hello", 1}, {"你好", 2}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello🔥你好🔥世界"}}));
}

// Merge + TEXT + CJK all-null group + CJK separator + Vec 1.0
TEST_F(StringAggOpTest, Ortho_Merge_Text_CJK_NullGroup_CJKSep_Vec1)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val text")
      .select("grp, STRING_AGG(val, '、')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "北京"}, {1, "上海"},
           {2, TestValue::null()}, {2, TestValue::null()}},
          "grp ASC")
      .with_rich_format(false).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "北京、上海"}, {2, "NULL"}}));
}

// Merge + TEXT + CJK all-null group + CJK separator + Vec 2.0
TEST_F(StringAggOpTest, Ortho_Merge_Text_CJK_NullGroup_CJKSep_Vec2)
{
  OpTestResult result = MergeGroupByTestSpec()
      .table("t", "grp int, val text")
      .select("grp, STRING_AGG(val, '、')")
      .group_by("grp")
      .with_sorted_data(
          {{1, "北京"}, {1, "上海"},
           {2, TestValue::null()}, {2, TestValue::null()}},
          "grp ASC")
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, "北京、上海"}, {2, "NULL"}}));
}

// Hash + TEXT + emoji values + ORDER BY num + Vec 1.0 + Non-vec
TEST_F(StringAggOpTest, Ortho_Hash_Text_Emoji_OrderBy_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = HashGroupByTestSpec()
        .table("t", "grp int, val text, num int")
        .select("grp, STRING_AGG(val, ',' ORDER BY num ASC)")
        .group_by("grp")
        .with_data(
            {{1, "🎉", 2}, {1, "😀", 1},
             {2, "🌟", 2}, {2, "🔥", 1}})
        .with_rich_format(false).run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_unordered({{1, "😀,🎉"}, {2, "🔥,🌟"}}));
  }
}

// Scalar + CHAR + Latin-extended + ORDER BY DESC + Vec 2.0
TEST_F(StringAggOpTest, Ortho_Scalar_Char_Latin_OrderByDesc_Vec2)
{
  OpTestResult result = ScalarAggTestSpec()
      .table("t", "val char(30), num int")
      .select("STRING_AGG(val, ', ' ORDER BY num DESC)")
      .with_data({{"café", 1}, {"résumé", 3}, {"naïve", 2}})
      .with_rich_format(true).run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"résumé, naïve, café"}}));
}

// Merge + CHAR + ASCII + pipe separator + mixed nulls + Non-vec
TEST_F(StringAggOpTest, Ortho_Merge_Char_ASCII_PipeSep_Null_NonVec)
{
  WITH_TENANT_CONFS({"_rowsets_enabled", "False"}) {
    OpTestResult result = MergeGroupByTestSpec()
        .table("t", "grp int, val char(20)")
        .select("grp, STRING_AGG(val, '|')")
        .group_by("grp")
        .with_sorted_data(
            {{1, "a"}, {1, TestValue::null()}, {1, "b"},
             {2, "x"}, {2, "y"}},
            "grp ASC")
        .enable_dual_format_check().run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, "a|b"}, {2, "x|y"}}));
  }
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_string_agg_op.log*");
  // OB_LOGGER.set_file_name("test_string_agg_op.log", true, true);
  // OB_LOGGER.set_log_level("INFO");
  // common::ObPLogWriterCfg log_cfg;
  // OB_LOGGER.init(log_cfg, false);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

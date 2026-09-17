/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <cfloat>
#include <cmath>
#include <gtest/gtest.h>

#include "observer/table/ob_table_filter.h"
#include "lib/allocator/page_arena.h"
#include "lib/worker.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::table::hfilter;

class TestTableFilter : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    lib::set_compat_mode(lib::Worker::CompatMode::MYSQL);
  }

  virtual void TearDown() override {}

protected:
  ObArenaAllocator allocator_;
  ObObj make_real(ObObjType type, double value, ObScale scale = SCALE_UNKNOWN_YET);
  int compare(ObTableComparator &comparator,
              const ObObj &cell,
              CompareOperator compare_op,
              int &cmp_ret);
  int filter(const ObString &comparator_text,
             const ObObj &cell,
             CompareOperator compare_op,
             bool &filtered);
  int filter(const char *comparator_text,
             const ObObj &cell,
             CompareOperator compare_op,
             bool &filtered);
};

ObObj TestTableFilter::make_real(const ObObjType type, const double value, const ObScale scale)
{
  ObObj obj;
  if (ObFloatType == type || ObUFloatType == type) {
    obj.set_float(type, static_cast<float>(value));
  } else {
    obj.set_double(type, value);
  }
  obj.set_scale(scale);
  return obj;
}

int TestTableFilter::compare(ObTableComparator &comparator,
                             const ObObj &cell,
                             const CompareOperator compare_op,
                             int &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 1> select_columns;
  ObObj row_cell = cell;
  ObNewRow row;
  row.cells_ = &row_cell;
  row.count_ = 1;
  if (OB_FAIL(select_columns.push_back(ObString::make_string("c1")))) {
  } else {
    ret = comparator.compare_to(select_columns, row, compare_op, cmp_ret);
  }
  return ret;
}

int TestTableFilter::filter(const ObString &comparator_text,
                            const ObObj &cell,
                            const CompareOperator compare_op,
                            bool &filtered)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 1> select_columns;
  ObObj row_cell = cell;
  ObNewRow row;
  ObTableComparator comparator(ObString::make_string("c1"), comparator_text, &allocator_);
  ObTableCompareFilter compare_filter(compare_op, &comparator);
  row.cells_ = &row_cell;
  row.count_ = 1;
  if (OB_FAIL(select_columns.push_back(ObString::make_string("c1")))) {
  } else {
    ret = compare_filter.filter_row(select_columns, row, filtered);
  }
  return ret;
}

int TestTableFilter::filter(const char *comparator_text,
                            const ObObj &cell,
                            const CompareOperator compare_op,
                            bool &filtered)
{
  return filter(ObString::make_string(comparator_text), cell, compare_op, filtered);
}

TEST_F(TestTableFilter, real_types_and_operators)
{
  const ObObjType real_types[] = {ObFloatType, ObUFloatType, ObDoubleType, ObUDoubleType};
  const CompareOperator operators[] = {
    CompareOperator::EQUAL,
    CompareOperator::NOT_EQUAL,
    CompareOperator::GREATER,
    CompareOperator::GREATER_OR_EQUAL,
    CompareOperator::LESS,
    CompareOperator::LESS_OR_EQUAL
  };
  const double matching_values[] = {5.0, 4.0, 6.0, 5.0, 4.0, 5.0};
  const double filtered_values[] = {4.0, 5.0, 5.0, 4.0, 5.0, 6.0};

  for (int64_t type_idx = 0;
       type_idx < static_cast<int64_t>(sizeof(real_types) / sizeof(real_types[0]));
       ++type_idx) {
    for (int64_t op_idx = 0;
         op_idx < static_cast<int64_t>(sizeof(operators) / sizeof(operators[0]));
         ++op_idx) {
      bool filtered = true;
      ObObj matching_cell = make_real(real_types[type_idx], matching_values[op_idx]);
      ASSERT_EQ(OB_SUCCESS, filter("5", matching_cell, operators[op_idx], filtered));
      EXPECT_FALSE(filtered);

      filtered = false;
      ObObj nonmatching_cell = make_real(real_types[type_idx], filtered_values[op_idx]);
      ASSERT_EQ(OB_SUCCESS, filter("5", nonmatching_cell, operators[op_idx], filtered));
      EXPECT_TRUE(filtered);
    }
  }
}

TEST_F(TestTableFilter, target_type_precision_and_adjacent_values)
{
  bool filtered = true;
  ObObj float_cell = make_real(ObFloatType, 16777216.0);
  ASSERT_EQ(OB_SUCCESS, filter("16777217", float_cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);

  filtered = false;
  ObObj double_cell = make_real(ObDoubleType, 16777216.0);
  ASSERT_EQ(OB_SUCCESS, filter("16777217", double_cell, CompareOperator::EQUAL, filtered));
  EXPECT_TRUE(filtered);
  double_cell = make_real(ObDoubleType, 16777217.0);
  ASSERT_EQ(OB_SUCCESS, filter("16777217", double_cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);

  filtered = false;
  float_cell = make_real(ObFloatType,
                         static_cast<double>(std::nextafter(1.0f, 2.0f)));
  ASSERT_EQ(OB_SUCCESS, filter("1", float_cell, CompareOperator::EQUAL, filtered));
  EXPECT_TRUE(filtered);
  ASSERT_EQ(OB_SUCCESS, filter("1", float_cell, CompareOperator::GREATER, filtered));
  EXPECT_FALSE(filtered);

  filtered = false;
  double_cell = make_real(ObDoubleType, std::nextafter(1.0, 2.0));
  ASSERT_EQ(OB_SUCCESS, filter("1", double_cell, CompareOperator::EQUAL, filtered));
  EXPECT_TRUE(filtered);
  ASSERT_EQ(OB_SUCCESS, filter("1", double_cell, CompareOperator::GREATER, filtered));
  EXPECT_FALSE(filtered);
}

TEST_F(TestTableFilter, fixed_double_scale)
{
  bool filtered = true;
  ObObj cell = make_real(ObDoubleType, 1.234, 2);
  ASSERT_EQ(OB_SUCCESS, filter("1.231", cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);
  ASSERT_EQ(OB_SUCCESS, filter("1.231", cell, CompareOperator::GREATER, filtered));
  EXPECT_TRUE(filtered);

  cell = make_real(ObDoubleType, 1.24, 2);
  ASSERT_EQ(OB_SUCCESS, filter("1.231", cell, CompareOperator::EQUAL, filtered));
  EXPECT_TRUE(filtered);
  ASSERT_EQ(OB_SUCCESS, filter("1.231", cell, CompareOperator::GREATER, filtered));
  EXPECT_FALSE(filtered);

  cell = make_real(ObDoubleType, 1.234, SCALE_UNKNOWN_YET);
  ASSERT_EQ(OB_SUCCESS, filter("1.231", cell, CompareOperator::EQUAL, filtered));
  EXPECT_TRUE(filtered);
}

TEST_F(TestTableFilter, valid_real_text)
{
  struct ValidTextCase
  {
    const char *text_;
    double value_;
    ObObjType type_;
  };
  const ValidTextCase cases[] = {
    {"1.25", 1.25, ObFloatType},
    {"+1.25", 1.25, ObDoubleType},
    {"-.5", -0.5, ObFloatType},
    {"2.", 2.0, ObDoubleType},
    {"1.25e2", 125.0, ObFloatType},
    {"-2.5E-1", -0.25, ObDoubleType},
    {" \t+1.25e1\r\n", 12.5, ObDoubleType}
  };

  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(cases) / sizeof(cases[0])); ++i) {
    bool filtered = true;
    ObObj cell = make_real(cases[i].type_, cases[i].value_);
    ASSERT_EQ(OB_SUCCESS, filter(cases[i].text_, cell, CompareOperator::EQUAL, filtered));
    EXPECT_FALSE(filtered);
  }

  char length_aware_text[] = {'1', '.', '5', 'x'};
  bool filtered = true;
  ObObj cell = make_real(ObDoubleType, 1.5);
  ASSERT_EQ(OB_SUCCESS,
            filter(ObString(3, length_aware_text), cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);
  EXPECT_NE(OB_SUCCESS,
            filter(ObString(4, length_aware_text), cell, CompareOperator::EQUAL, filtered));
}

TEST_F(TestTableFilter, invalid_real_text)
{
  const char *invalid_texts[] = {
    "", "   ", "abc", ".", "+", "1x", "1 2", "1e", "1e+", "--1",
    "NaN", "-nan", "+NAN", "Inf", "-inf", "Infinity", "+INFINITY", "0x1p2"
  };
  const ObObjType real_types[] = {ObFloatType, ObDoubleType};

  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(invalid_texts) / sizeof(invalid_texts[0]));
       ++i) {
    bool filtered = false;
    ObObj cell = make_real(real_types[i % 2], 0.0);
    EXPECT_NE(OB_SUCCESS,
              filter(invalid_texts[i], cell, CompareOperator::EQUAL, filtered));
  }
}

TEST_F(TestTableFilter, range_and_unsigned)
{
  const char *float_max_text = "3.40282346638528859811704183484516925440e38";
  const char *double_max_text = "1.7976931348623157e308";
  bool filtered = true;

  ObObj cell = make_real(ObFloatType, FLT_MAX);
  ASSERT_EQ(OB_SUCCESS, filter(float_max_text, cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);
  cell = make_real(ObFloatType, -FLT_MAX);
  ASSERT_EQ(OB_SUCCESS,
            filter("-3.40282346638528859811704183484516925440e38",
                   cell,
                   CompareOperator::EQUAL,
                   filtered));
  EXPECT_FALSE(filtered);
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("3.5e38", cell, CompareOperator::EQUAL, filtered));
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("-3.5e38", cell, CompareOperator::EQUAL, filtered));

  cell = make_real(ObUFloatType, FLT_MAX);
  ASSERT_EQ(OB_SUCCESS, filter(float_max_text, cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("3.5e38", cell, CompareOperator::EQUAL, filtered));
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("-1", cell, CompareOperator::EQUAL, filtered));

  cell = make_real(ObDoubleType, DBL_MAX);
  ASSERT_EQ(OB_SUCCESS, filter(double_max_text, cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);
  cell = make_real(ObDoubleType, -DBL_MAX);
  ASSERT_EQ(OB_SUCCESS,
            filter("-1.7976931348623157e308", cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("1e309", cell, CompareOperator::EQUAL, filtered));
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("-1e309", cell, CompareOperator::EQUAL, filtered));

  cell = make_real(ObUDoubleType, DBL_MAX);
  ASSERT_EQ(OB_SUCCESS, filter(double_max_text, cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("1e309", cell, CompareOperator::EQUAL, filtered));
  EXPECT_EQ(OB_DATA_OUT_OF_RANGE,
            filter("-0.0001", cell, CompareOperator::EQUAL, filtered));

  const ObObjType unsigned_types[] = {ObUFloatType, ObUDoubleType};
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(unsigned_types) / sizeof(unsigned_types[0]));
       ++i) {
    cell = make_real(unsigned_types[i], 0.0);
    ASSERT_EQ(OB_SUCCESS, filter("-0", cell, CompareOperator::EQUAL, filtered));
    EXPECT_FALSE(filtered);
    ASSERT_EQ(OB_SUCCESS, filter("-0.0", cell, CompareOperator::EQUAL, filtered));
    EXPECT_FALSE(filtered);
    ASSERT_EQ(OB_SUCCESS, filter("0", cell, CompareOperator::EQUAL, filtered));
    EXPECT_FALSE(filtered);
  }
}

TEST_F(TestTableFilter, null_semantics)
{
  ObObj null_cell;
  bool filtered = true;
  ASSERT_EQ(OB_SUCCESS,
            filter("not-a-real", null_cell, CompareOperator::IS, filtered));
  EXPECT_FALSE(filtered);
  ASSERT_EQ(OB_SUCCESS,
            filter("not-a-real", null_cell, CompareOperator::IS_NOT, filtered));
  EXPECT_TRUE(filtered);
  ASSERT_EQ(OB_SUCCESS,
            filter("not-a-real", null_cell, CompareOperator::EQUAL, filtered));
  EXPECT_TRUE(filtered);

  ObObj nonnull_cell = make_real(ObDoubleType, 1.0);
  ASSERT_EQ(OB_SUCCESS,
            filter("not-a-real", nonnull_cell, CompareOperator::IS, filtered));
  EXPECT_TRUE(filtered);
  ASSERT_EQ(OB_SUCCESS,
            filter("not-a-real", nonnull_cell, CompareOperator::IS_NOT, filtered));
  EXPECT_FALSE(filtered);
}

TEST_F(TestTableFilter, request_local_cache_invalidation)
{
  char comparator_text[] = {'1', '.', '5', '\0'};
  ObTableComparator comparator(ObString::make_string("c1"),
                               ObString(3, comparator_text),
                               &allocator_);
  int cmp_ret = 1;

  ObObj cell = make_real(ObFloatType, 1.5);
  ASSERT_EQ(OB_SUCCESS, compare(comparator, cell, CompareOperator::EQUAL, cmp_ret));
  EXPECT_EQ(0, cmp_ret);

  comparator_text[0] = '2';
  ASSERT_EQ(OB_SUCCESS, compare(comparator, cell, CompareOperator::EQUAL, cmp_ret));
  EXPECT_GT(cmp_ret, 0);

  cell = make_real(ObFloatType, 2.5, 2);
  ASSERT_EQ(OB_SUCCESS, compare(comparator, cell, CompareOperator::EQUAL, cmp_ret));
  EXPECT_EQ(0, cmp_ret);

  comparator_text[0] = '3';
  cell = make_real(ObDoubleType, 2.5, 2);
  ASSERT_EQ(OB_SUCCESS, compare(comparator, cell, CompareOperator::EQUAL, cmp_ret));
  EXPECT_GT(cmp_ret, 0);

  ObTableComparator another_request(ObString::make_string("c1"),
                                     ObString(3, comparator_text),
                                     &allocator_);
  cell = make_real(ObDoubleType, 3.5, 2);
  ASSERT_EQ(OB_SUCCESS, compare(another_request, cell, CompareOperator::EQUAL, cmp_ret));
  EXPECT_EQ(0, cmp_ret);
}

TEST_F(TestTableFilter, existing_and_unsupported_types)
{
  bool filtered = true;
  ObObj cell;

  cell.set_int(5);
  ASSERT_EQ(OB_SUCCESS, filter("5", cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);

  cell.set_uint64(5);
  ASSERT_EQ(OB_SUCCESS, filter("5", cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);

  cell.set_varchar("value");
  cell.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ASSERT_EQ(OB_SUCCESS, filter("value", cell, CompareOperator::EQUAL, filtered));
  EXPECT_FALSE(filtered);

  ObArenaAllocator allocator;
  number::ObNumber number;
  ASSERT_EQ(OB_SUCCESS, number.from(static_cast<int64_t>(1), allocator));
  cell.set_number(number);
  EXPECT_EQ(OB_NOT_SUPPORTED, filter("1", cell, CompareOperator::EQUAL, filtered));

  int64_t decimal_value = 100;
  cell.set_decimal_int(sizeof(decimal_value),
                       2,
                       reinterpret_cast<ObDecimalInt *>(&decimal_value));
  EXPECT_EQ(OB_NOT_SUPPORTED, filter("1", cell, CompareOperator::EQUAL, filtered));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_filter.log", true);
  lib::set_compat_mode(lib::Worker::CompatMode::MYSQL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

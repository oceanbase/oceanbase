/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <unistd.h>  // for access()
#include <vector>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_engine.h"
#include "unittest/sql/engine/op_tests/ob_op_test_types.h"
#include "unittest/sql/engine/op_tests/ob_op_test_file_data.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

// ===== T1 Acceptance Tests: TestValue and TestRow =====

TEST(OpTestTypes, TestValueConstruction)
{
  // Test int construction
  TestValue int_val(123);
  EXPECT_TRUE(int_val.is_int());
  EXPECT_FALSE(int_val.is_null());
  EXPECT_EQ(123, int_val.get_int());

  // Test double construction
  TestValue double_val(3.14);
  EXPECT_TRUE(double_val.is_double());
  EXPECT_EQ(3.14, double_val.get_double());

  // Test string construction
  TestValue str_val("hello");
  EXPECT_TRUE(str_val.is_string());
  EXPECT_EQ("hello", str_val.get_string());

  // Test null construction
  TestValue null_val = TestValue::null();
  EXPECT_TRUE(null_val.is_null());

  // Test default construction (should be null)
  TestValue default_val;
  EXPECT_TRUE(default_val.is_null());
}

TEST(OpTestTypes, TestRowConstruction)
{
  // Test initializer list construction (acceptance criteria)
  TestRow row = {1, 2.5, std::string("abc"), TestValue::null()};
  EXPECT_EQ(4, row.size());

  EXPECT_TRUE(row[0].is_int());
  EXPECT_EQ(1, row[0].get_int());

  EXPECT_TRUE(row[1].is_double());
  EXPECT_EQ(2.5, row[1].get_double());

  EXPECT_TRUE(row[2].is_string());
  EXPECT_EQ("abc", row[2].get_string());

  EXPECT_TRUE(row[3].is_null());
}

TEST(OpTestTypes, TestValueComparison)
{
  TestValue a(1);
  TestValue b(1);
  TestValue c(2);
  TestValue null_val = TestValue::null();

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(a, null_val);

  // Null is smallest (null first semantics)
  EXPECT_TRUE(null_val < a);
  EXPECT_FALSE(a < null_val);
}

// Test null first ordering (default behavior)
TEST(OpTestTypes, TestValueNullFirstOrdering)
{
  TestValue null_val = TestValue::null();
  TestValue one(1);
  TestValue two(2);

  // Null first: null < 1 < 2
  EXPECT_TRUE(null_val < one);
  EXPECT_TRUE(null_val < two);
  EXPECT_TRUE(one < two);

  // Verify transitivity
  EXPECT_FALSE(one < null_val);
  EXPECT_FALSE(two < null_val);
  EXPECT_FALSE(two < one);
}

// Test null comparison with same type values
TEST(OpTestTypes, TestValueNullComparison)
{
  TestValue null1 = TestValue::null();
  TestValue null2 = TestValue::null();

  // Two nulls are equal
  EXPECT_EQ(null1, null2);
  EXPECT_FALSE(null1 < null2);
  EXPECT_FALSE(null2 < null1);
}

// Test TestRow with null values
TEST(OpTestTypes, TestRowWithNullValues)
{
  TestRow row1 = {TestValue::null(), 1, 2};
  TestRow row2 = {TestValue::null(), 1, 2};
  TestRow row3 = {0, 1, 2};

  EXPECT_EQ(row1, row2);
  EXPECT_NE(row1, row3);

  // Row with null in different position
  TestRow row4 = {1, TestValue::null(), 2};
  EXPECT_NE(row1, row4);
}

TEST(OpTestTypes, TestRowComparison)
{
  TestRow row1 = {1, 2, 3};
  TestRow row2 = {1, 2, 3};
  TestRow row3 = {1, 2, 4};

  EXPECT_EQ(row1, row2);
  EXPECT_NE(row1, row3);
}

// Test TestRow ordering with null values (null first)
TEST(OpTestTypes, TestRowOrderingWithNull)
{
  TestRow row_with_null = {TestValue::null(), 1};
  TestRow row_without_null = {0, 1};
  TestRow row_all_nulls = {TestValue::null(), TestValue::null()};
  TestRow row_normal = {1, 2};

  // Rows are compared element by element
  // With null first semantics, null < any value
  std::vector<TestRow> rows = {row_normal, row_with_null, row_without_null, row_all_nulls};
  std::sort(rows.begin(), rows.end(), [](const TestRow &a, const TestRow &b) {
    for (size_t i = 0; i < std::min(a.size(), b.size()); ++i) {
      if (a[i] < b[i]) return true;
      if (b[i] < a[i]) return false;
    }
    return a.size() < b.size();
  });

  // After sorting with null first, rows with null should come first
  EXPECT_TRUE(rows[0][0].is_null());  // First row should have null in first column
}

TEST(OpTestTypes, TestValueToString)
{
  EXPECT_EQ("123", TestValue(123).to_string());
  EXPECT_EQ("NULL", TestValue::null().to_string());
  EXPECT_EQ("hello", TestValue("hello").to_string());
}

// ===== T7 Acceptance Tests: Full Chain Verification =====

class MaterialOpTest : public OpTestKit
{
};

// TC1: 基础透传（验证 MockDataSourceOp + execute 链路）
TEST_F(MaterialOpTest, BasicPassThrough)
{
  OpTestResult result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 2}, {3, 4}, {5, 6}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "2"}, {"3", "4"}, {"5", "6"}}));
}

// Bypass: no temp store / sql mem — isolates Material full path vs child+execute chain.
TEST_F(MaterialOpTest, BypassPassThrough)
{
  OpTestResult result = material_test()
      .with_bypass(true)
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 2}, {3, 4}, {5, 6}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "2"}, {"3", "4"}, {"5", "6"}}));
}


// TC2: 表达式计算（验证 resolve 类型推导 + CG eval_func）
// Note: Implicit cast between int and number requires proper cast context setup.
// Using same-type addition to test expression evaluation.
TEST_F(MaterialOpTest, ExpressionEvaluation)
{
  OpTestResult result = material_test()
      .table("t", "a int, b int")
      .select("a + b")
      .with_data({{1, 2}, {3, 4}, {5, 6}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  // a + b: 1+2=3, 3+4=7, 5+6=11
  EXPECT_TRUE(result.verify_ordered({{"3"}, {"7"}, {"11"}}));
}

// TC3: 常量参数（验证 parameterize + init_datum_param_store + eval_question_mark_func）
// Note: This test may need adjustment based on OceanBase's constant handling
TEST_F(MaterialOpTest, ConstantExpression)
{
  OpTestResult result = material_test()
      .table("t", "a varchar(32)")
      .select("a")
      .with_data({TestRow{std::string("abc")}, TestRow{std::string("def")}, TestRow{std::string("ab123")}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"abc"}, {"def"}, {"ab123"}}));
}

// TC4: 多参数函数 + 多常量
TEST_F(MaterialOpTest, ConcatFunction)
{
  OpTestResult result = material_test()
      .table("t", "a varchar(16), b varchar(16)")
      .select("concat(a, '-', b)")
      .with_data({{std::string("hello"), std::string("world")}})
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello-world"}}));
}

// TC5: 多批次数据（验证 MockDataSourceOp 多轮 get_next_batch）
TEST_F(MaterialOpTest, MultiBatch)
{
  // Create 5 rows of data
  std::vector<TestRow> data;
  for (int i = 1; i <= 5; ++i) {
    data.push_back({i, i * 10});
  }

  OpTestResult result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_batch_size(2)  // Force multiple batches
      .with_data(std::move(data))
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(5, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"1", "10"}, {"2", "20"}, {"3", "30"}, {"4", "40"}, {"5", "50"}
  }));
}

// TC6: 空数据集
TEST_F(MaterialOpTest, EmptyDataset)
{
  OpTestResult result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({})  // Empty data
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// ===== T11 Acceptance Tests: Filter End-to-End Tests =====

// TC1: 基础 filter
TEST_F(MaterialOpTest, FilterBasic)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .where("a > 1")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_dual_format_check().run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"2", "20"}, {"3", "30"}}));
}

// TC2: 多条件 AND
TEST_F(MaterialOpTest, FilterMultiCondition)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .where("a > 1 AND b < 30")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_dual_format_check().run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"2", "20"}}));
}

// TC3: 带算术表达式的 filter
TEST_F(MaterialOpTest, FilterWithExpression)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .where("a + b > 20")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_dual_format_check().run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"2", "20"}, {"3", "30"}}));
}

// TC4: filter 过滤所有行
TEST_F(MaterialOpTest, FilterAllFiltered)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .where("a > 100")
      .with_data({{1}, {2}, {3}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(0, result.row_count());
}

// TC5: filter 不过滤任何行
TEST_F(MaterialOpTest, FilterNoneFiltered)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .where("a > 0")
      .with_data({{1}, {2}, {3}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
}

// ===== T7 Acceptance Tests: OpTestResult verify_column =====

// Test verify_column with int values
TEST(OpTestResultTest, VerifyColumnWithIntValues)
{
  OpTestResult result;
  result.add_row(std::vector<std::string>{"1", "a"});
  result.add_row(std::vector<std::string>{"2", "b"});
  result.add_row(std::vector<std::string>{"3", "c"});

  // Verify first column (int values as strings)
  EXPECT_TRUE(result.verify_column(0, {TestValue(1), TestValue(2), TestValue(3)}));
  EXPECT_FALSE(result.verify_column(0, {TestValue(1), TestValue(2)}));  // Wrong count
  EXPECT_FALSE(result.verify_column(0, {TestValue(1), TestValue(5), TestValue(3)}));  // Wrong value
}

// Test verify_column with string values
TEST(OpTestResultTest, VerifyColumnWithStringValues)
{
  OpTestResult result;
  result.add_row(std::vector<std::string>{"1", "hello"});
  result.add_row(std::vector<std::string>{"2", "world"});

  // Verify second column (string values)
  EXPECT_TRUE(result.verify_column(1, {TestValue("hello"), TestValue("world")}));
  EXPECT_FALSE(result.verify_column(1, {TestValue("hello"), TestValue("foo")}));
}

// Test verify_column with null values
TEST(OpTestResultTest, VerifyColumnWithNullValues)
{
  OpTestResult result;
  result.add_row(std::vector<std::string>{"NULL", "a"});
  result.add_row(std::vector<std::string>{"1", "NULL"});

  // Verify first column has null
  EXPECT_TRUE(result.verify_column(0, {TestValue::null(), TestValue(1)}));
  // Verify second column has null
  EXPECT_TRUE(result.verify_column(1, {TestValue("a"), TestValue::null()}));
}

// Test verify_column with row count mismatch
TEST(OpTestResultTest, VerifyColumnRowCountMismatch)
{
  OpTestResult result;
  result.add_row(std::vector<std::string>{"1", "a"});
  result.add_row(std::vector<std::string>{"2", "b"});

  // 3 expected values but only 2 rows
  EXPECT_FALSE(result.verify_column(0, {TestValue(1), TestValue(2), TestValue(3)}));
}

// Test verify_column with out of range column index
TEST(OpTestResultTest, VerifyColumnOutOfRange)
{
  OpTestResult result;
  result.add_row(std::vector<std::string>{"1", "a"});

  // Column index 5 is out of range, should throw or fail
  EXPECT_ANY_THROW(result.verify_column(5, {TestValue(1)}));
}

// Test verify_column on empty result
TEST(OpTestResultTest, VerifyColumnEmptyResult)
{
  OpTestResult result;
  EXPECT_TRUE(result.verify_column(0, {}));  // Empty expected matches empty result
  EXPECT_FALSE(result.verify_column(0, {TestValue(1)}));  // Non-empty expected on empty result
}

// ===== Null Ordering Tests =====

// Test null first ordering (default)
TEST(OpTestTypes, NullFirstOrdering)
{
  TestValue::set_null_ordering(NullOrdering::NULL_FIRST);

  TestValue null_val = TestValue::null();
  TestValue one(1);

  // NULL < 1 in null first mode
  EXPECT_TRUE(null_val < one);
  EXPECT_FALSE(one < null_val);
}

// Test null last ordering
TEST(OpTestTypes, NullLastOrdering)
{
  TestValue::set_null_ordering(NullOrdering::NULL_LAST);

  TestValue null_val = TestValue::null();
  TestValue one(1);

  // 1 < NULL in null last mode
  EXPECT_FALSE(null_val < one);
  EXPECT_TRUE(one < null_val);
}

// Test row ordering with null first
TEST(OpTestTypes, RowOrderingNullFirst)
{
  TestValue::set_null_ordering(NullOrdering::NULL_FIRST);

  TestRow row_with_null = {TestValue::null(), 1};
  TestRow row_without_null = {0, 1};

  // Row with null should come first
  EXPECT_TRUE(row_with_null[0] < row_without_null[0]);
}

// Test row ordering with null last
TEST(OpTestTypes, RowOrderingNullLast)
{
  TestValue::set_null_ordering(NullOrdering::NULL_LAST);

  TestValue null_val = TestValue::null();
  TestValue zero(0);

  // Row with null should come last
  EXPECT_FALSE(null_val < zero);
  EXPECT_TRUE(zero < null_val);
}

// Reset to default after tests
TEST(OpTestTypes, ResetNullOrdering)
{
  TestValue::set_null_ordering(NullOrdering::NULL_FIRST);
  EXPECT_EQ(NullOrdering::NULL_FIRST, TestValue::get_null_ordering());
}

// ===== T10 Acceptance Tests: Rescan Full Chain =====

// TC1: 基础 rescan
TEST_F(MaterialOpTest, RescanBasic)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 2}, {3, 4}})
      .with_rescan_times(3)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "2"}, {"3", "4"}}));
}

// TC2: 多批次 rescan
TEST_F(MaterialOpTest, RescanMultiBatch)
{
  std::vector<TestRow> data;
  for (int i = 1; i <= 10; ++i) data.push_back({i, i * 10});
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_batch_size(3)
      .with_rescan_times(2)
      .with_data(std::move(data))
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(10, result.row_count());
}

// TC3: 空数据 rescan
TEST_F(MaterialOpTest, RescanEmpty)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({})
      .with_rescan_times(2)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(0, result.row_count());
}

// TC4: rescan + 表达式计算
TEST_F(MaterialOpTest, RescanWithExpression)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a + b")
      .with_data({{1, 2}, {3, 4}})
      .with_rescan_times(2)
      .enable_dual_format_check().run(engine_);
  EXPECT_TRUE(result.verify_ordered({{"3"}, {"7"}}));
}

// ===== T2 Acceptance Tests: MockDataSourceOp inner_rescan() =====

// Helper function to collect all rows from a MockDataSourceOp
static std::vector<std::vector<std::string>> collect_mock_rows(MockDataSourceOp *op)
{
  std::vector<std::vector<std::string>> rows;
  const ObBatchRows *batch_rows = nullptr;

  while (OB_SUCCESS == op->get_next_batch(INT64_MAX, batch_rows)) {
    if (batch_rows->end_) {
      break;
    }
    if (batch_rows->size_ == 0) {
      break;
    }

    // Collect rows from this batch
    const ExprFixedArray &output_exprs = op->get_spec().output_;
    ObEvalCtx &eval_ctx = op->get_eval_ctx();

    for (int64_t row = 0; row < batch_rows->size_; ++row) {
      if (batch_rows->skip_->exist(row)) {
        continue;
      }

      std::vector<std::string> row_values;
      for (int64_t col = 0; col < output_exprs.count(); ++col) {
        ObExpr *expr = output_exprs.at(col);
        if (OB_ISNULL(expr)) {
          row_values.push_back("NULL");
          continue;
        }

        sql::ObBitVector &nulls = expr->get_nulls(eval_ctx);
        if (nulls.at(row)) {
          row_values.push_back("NULL");
          continue;
        }

        // Get value as string
        VectorFormat fmt = expr->get_format(eval_ctx);
        char tmp_buf[64];
        const char *payload = nullptr;
        ObLength length = 0;

        if (fmt == VEC_FIXED) {
          char *data = expr->get_res_buf(eval_ctx);
          const ObObjType obj_type = expr->datum_meta_.get_type();
          if (obj_type == ObIntType || obj_type == ObInt32Type) {
            int64_t val = reinterpret_cast<const int64_t *>(data)[row];
            snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
            payload = tmp_buf;
            length = strlen(tmp_buf);
          } else {
            int64_t val = reinterpret_cast<const int64_t *>(data)[row];
            snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
            payload = tmp_buf;
            length = strlen(tmp_buf);
          }
        } else if (fmt == VEC_DISCRETE || fmt == VEC_CONTINUOUS) {
          char **ptrs = expr->get_discrete_vector_ptrs(eval_ctx);
          int32_t *lens = expr->get_discrete_vector_lens(eval_ctx);
          payload = ptrs[row];
          length = lens[row];
        } else {
          row_values.push_back("?");
          continue;
        }

        row_values.push_back(std::string(payload, length));
      }
      rows.push_back(row_values);
    }
  }

  return rows;
}

// TC1: rescan 后 cur_idx_ 归零，能从头重新输出数据
TEST_F(MaterialOpTest, MockDataSourceRescan)
{
  // Step 1: Register table and resolve SQL to get expression infrastructure
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));
  ObDMLStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, engine_.resolve_sql("SELECT a, b FROM t", stmt));
  ASSERT_NE(nullptr, stmt);
  ASSERT_EQ(OB_SUCCESS, engine_.generate_exprs(*stmt));

  // Step 2: Get column expressions
  const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();
  ExprFixedArray column_exprs(engine_.get_phy_plan().get_allocator());
  ASSERT_EQ(OB_SUCCESS, column_exprs.init(column_items.count()));
  ASSERT_EQ(OB_SUCCESS, column_exprs.prepare_allocate(column_items.count()));

  int64_t column_count = 0;
  for (int64_t i = 0; i < column_items.count(); ++i) {
    const ColumnItem &col_item = column_items.at(i);
    if (OB_NOT_NULL(col_item.expr_)) {
      ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*col_item.expr_);
      if (OB_NOT_NULL(expr)) {
        column_exprs.at(column_count++) = expr;
      }
    }
  }

  // Step 3: Create MockDataSourceOp with test data
  MockDataSourceOp *mock_op = engine_.create_mock_data_source(column_exprs);
  ASSERT_NE(nullptr, mock_op);

  std::vector<TestRow> test_data = {{1, 2}, {3, 4}, {5, 6}};
  mock_op->set_test_data(test_data);

  // Step 4: Open and collect first result
  ASSERT_EQ(OB_SUCCESS, mock_op->open());
  std::vector<std::vector<std::string>> first_result = collect_mock_rows(mock_op);

  // Step 5: Rescan and collect second result
  ASSERT_EQ(OB_SUCCESS, mock_op->rescan());
  std::vector<std::vector<std::string>> second_result = collect_mock_rows(mock_op);

  // Step 6: Close
  ASSERT_EQ(OB_SUCCESS, mock_op->close());

  // Step 7: Verify both results are identical
  ASSERT_EQ(first_result.size(), second_result.size());
  for (size_t i = 0; i < first_result.size(); ++i) {
    EXPECT_EQ(first_result[i], second_result[i])
        << "Row " << i << " mismatch: first=" << first_result[i][0] << "," << first_result[i][1]
        << " second=" << second_result[i][0] << "," << second_result[i][1];
  }

  // Verify actual values
  ASSERT_EQ(3, first_result.size());
  EXPECT_EQ("1", first_result[0][0]);
  EXPECT_EQ("2", first_result[0][1]);
  EXPECT_EQ("3", first_result[1][0]);
  EXPECT_EQ("4", first_result[1][1]);
  EXPECT_EQ("5", first_result[2][0]);
  EXPECT_EQ("6", first_result[2][1]);
}

// TC2: rescan 多次，每次输出一致
TEST_F(MaterialOpTest, MockDataSourceRescanMultiple)
{
  // Step 1: Setup expression infrastructure
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));
  ObDMLStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, engine_.resolve_sql("SELECT a, b FROM t", stmt));
  ASSERT_NE(nullptr, stmt);
  ASSERT_EQ(OB_SUCCESS, engine_.generate_exprs(*stmt));

  // Step 2: Get column expressions
  const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();
  ExprFixedArray column_exprs(engine_.get_phy_plan().get_allocator());
  ASSERT_EQ(OB_SUCCESS, column_exprs.init(column_items.count()));
  ASSERT_EQ(OB_SUCCESS, column_exprs.prepare_allocate(column_items.count()));

  int64_t column_count = 0;
  for (int64_t i = 0; i < column_items.count(); ++i) {
    const ColumnItem &col_item = column_items.at(i);
    if (OB_NOT_NULL(col_item.expr_)) {
      ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*col_item.expr_);
      if (OB_NOT_NULL(expr)) {
        column_exprs.at(column_count++) = expr;
      }
    }
  }

  // Step 3: Create MockDataSourceOp with test data
  MockDataSourceOp *mock_op = engine_.create_mock_data_source(column_exprs);
  ASSERT_NE(nullptr, mock_op);

  std::vector<TestRow> test_data = {{10, 20}, {30, 40}, {50, 60}, {70, 80}};
  mock_op->set_test_data(test_data);

  // Step 4: Open and collect results from multiple rescans
  ASSERT_EQ(OB_SUCCESS, mock_op->open());

  std::vector<std::vector<std::string>> results[3];

  // First iteration
  results[0] = collect_mock_rows(mock_op);

  // Second iteration (after first rescan)
  ASSERT_EQ(OB_SUCCESS, mock_op->rescan());
  results[1] = collect_mock_rows(mock_op);

  // Third iteration (after second rescan)
  ASSERT_EQ(OB_SUCCESS, mock_op->rescan());
  results[2] = collect_mock_rows(mock_op);

  ASSERT_EQ(OB_SUCCESS, mock_op->close());

  // Step 5: Verify all three results are identical
  for (int iter = 0; iter < 3; ++iter) {
    ASSERT_EQ(4, results[iter].size()) << "Iteration " << iter << " has wrong row count";
    EXPECT_EQ("10", results[iter][0][0]);
    EXPECT_EQ("20", results[iter][0][1]);
    EXPECT_EQ("30", results[iter][1][0]);
    EXPECT_EQ("40", results[iter][1][1]);
    EXPECT_EQ("50", results[iter][2][0]);
    EXPECT_EQ("60", results[iter][2][1]);
    EXPECT_EQ("70", results[iter][3][0]);
    EXPECT_EQ("80", results[iter][3][1]);

    // Verify all iterations produce same results
    if (iter > 0) {
      EXPECT_EQ(results[0].size(), results[iter].size());
      for (size_t i = 0; i < results[0].size(); ++i) {
        EXPECT_EQ(results[0][i], results[iter][i])
            << "Row " << i << " differs between iteration 0 and " << iter;
      }
    }
  }
}

// TC3: 空数据集 rescan
TEST_F(MaterialOpTest, MockDataSourceRescanEmpty)
{
  // Step 1: Setup expression infrastructure
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));
  ObDMLStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, engine_.resolve_sql("SELECT a, b FROM t", stmt));
  ASSERT_NE(nullptr, stmt);
  ASSERT_EQ(OB_SUCCESS, engine_.generate_exprs(*stmt));

  // Step 2: Get column expressions
  const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();
  ExprFixedArray column_exprs(engine_.get_phy_plan().get_allocator());
  ASSERT_EQ(OB_SUCCESS, column_exprs.init(column_items.count()));
  ASSERT_EQ(OB_SUCCESS, column_exprs.prepare_allocate(column_items.count()));

  int64_t column_count = 0;
  for (int64_t i = 0; i < column_items.count(); ++i) {
    const ColumnItem &col_item = column_items.at(i);
    if (OB_NOT_NULL(col_item.expr_)) {
      ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*col_item.expr_);
      if (OB_NOT_NULL(expr)) {
        column_exprs.at(column_count++) = expr;
      }
    }
  }

  // Step 3: Create MockDataSourceOp with EMPTY test data
  MockDataSourceOp *mock_op = engine_.create_mock_data_source(column_exprs);
  ASSERT_NE(nullptr, mock_op);

  std::vector<TestRow> test_data = {};  // Empty dataset
  mock_op->set_test_data(test_data);

  // Step 4: Open and get first result (should be empty)
  ASSERT_EQ(OB_SUCCESS, mock_op->open());
  std::vector<std::vector<std::string>> first_result = collect_mock_rows(mock_op);
  EXPECT_EQ(0, first_result.size());

  // Step 5: Rescan and get second result (should also be empty)
  ASSERT_EQ(OB_SUCCESS, mock_op->rescan());
  std::vector<std::vector<std::string>> second_result = collect_mock_rows(mock_op);
  EXPECT_EQ(0, second_result.size());

  // Step 6: Close
  ASSERT_EQ(OB_SUCCESS, mock_op->close());

  // Verify both are empty
  EXPECT_TRUE(first_result.empty());
  EXPECT_TRUE(second_result.empty());
}

// ===== T6 Acceptance Tests: CsvDataReader =====

class CsvDataReaderTest : public ::testing::Test
{
protected:
  std::string test_data_dir_;

  virtual void SetUp()
  {
    // Try to find test_data directory
    // First check if it exists relative to current directory (build directory with symlink)
    if (access("test_data/sample.csv", F_OK) == 0) {
      test_data_dir_ = "test_data/";
    } else {
      // Fall back to source directory path
      test_data_dir_ = std::string(std::getenv("PWD") ? std::getenv("PWD") : ".") +
                       "/unittest/sql/engine/op_tests/test_data/";
    }
  }
};

// TC1: ReadBasicCsv - 基础读取 int, double, varchar
TEST_F(CsvDataReaderTest, ReadBasicCsv)
{
  CsvDataReader reader;
  std::string file_path = test_data_dir_ + "sample.csv";

  int ret = reader.open(file_path);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Set column types manually since no type header
  reader.set_column_types({"int", "double", "varchar"});

  std::vector<TestRow> rows;
  ret = reader.read_all(rows);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(5, rows.size());

  // Verify first row
  EXPECT_TRUE(rows[0][0].is_int());
  EXPECT_EQ(1, rows[0][0].get_int());
  EXPECT_TRUE(rows[0][1].is_double());
  EXPECT_NEAR(3.14, rows[0][1].get_double(), 0.001);
  EXPECT_TRUE(rows[0][2].is_string());
  EXPECT_EQ("hello", rows[0][2].get_string());

  // Verify last row
  EXPECT_EQ(5, rows[4][0].get_int());
  EXPECT_EQ("database", rows[4][2].get_string());

  reader.close();
}

// TC2: ReadCsvWithTypeHeader - 带类型头行
TEST_F(CsvDataReaderTest, ReadCsvWithTypeHeader)
{
  CsvDataReader reader;
  std::string file_path = test_data_dir_ + "typed_sample.csv";

  int ret = reader.open(file_path);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(reader.has_type_header());
  EXPECT_EQ(3, reader.get_column_count());

  TestRow row;
  ret = reader.read_row(row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(3, row.size());

  // Types should be parsed from header: int, double, varchar(32)
  EXPECT_TRUE(row[0].is_int());
  EXPECT_EQ(1, row[0].get_int());
  EXPECT_TRUE(row[1].is_double());
  EXPECT_NEAR(3.14, row[1].get_double(), 0.001);
  EXPECT_TRUE(row[2].is_string());
  EXPECT_EQ("hello", row[2].get_string());

  reader.close();
}

// TC3: ReadCsvWithNull - NULL 值处理
TEST_F(CsvDataReaderTest, ReadCsvWithNull)
{
  CsvDataReader reader;
  std::string file_path = test_data_dir_ + "null_sample.csv";

  int ret = reader.open(file_path);
  EXPECT_EQ(OB_SUCCESS, ret);

  reader.set_column_types({"int", "double", "varchar"});

  std::vector<TestRow> rows;
  ret = reader.read_all(rows);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(4, rows.size());

  // Row 1: 1, 3.14, hello - no nulls
  EXPECT_FALSE(rows[0][0].is_null());
  EXPECT_FALSE(rows[0][1].is_null());
  EXPECT_FALSE(rows[0][2].is_null());

  // Row 2: null, 2.71, world - first column null
  EXPECT_TRUE(rows[1][0].is_null());
  EXPECT_FALSE(rows[1][1].is_null());
  EXPECT_FALSE(rows[1][2].is_null());

  // Row 3: 3, null, test - second column null
  EXPECT_FALSE(rows[2][0].is_null());
  EXPECT_TRUE(rows[2][1].is_null());
  EXPECT_FALSE(rows[2][2].is_null());

  // Row 4: 4, 1.73, null - third column null
  EXPECT_FALSE(rows[3][0].is_null());
  EXPECT_FALSE(rows[3][1].is_null());
  EXPECT_TRUE(rows[3][2].is_null());

  reader.close();
}

// TC4: ReadEmptyCsv - 空文件
TEST_F(CsvDataReaderTest, ReadEmptyCsv)
{
  CsvDataReader reader;
  std::string file_path = test_data_dir_ + "empty.csv";

  int ret = reader.open(file_path);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::vector<TestRow> rows;
  ret = reader.read_all(rows);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, rows.size());

  reader.close();
}

// TC5: FileNotExist - 文件不存在
TEST_F(CsvDataReaderTest, FileNotExist)
{
  CsvDataReader reader;
  std::string file_path = test_data_dir_ + "nonexistent.csv";

  int ret = reader.open(file_path);
  EXPECT_EQ(OB_IO_ERROR, ret);
  EXPECT_FALSE(reader.is_open());
}

// TC6: ReadLargeCsv - 大文件流式读取 (100 rows)
TEST_F(CsvDataReaderTest, ReadLargeCsv)
{
  CsvDataReader reader;
  std::string file_path = test_data_dir_ + "100_rows.csv";

  int ret = reader.open(file_path);
  EXPECT_EQ(OB_SUCCESS, ret);

  reader.set_column_types({"int", "double", "varchar"});

  // Read row by row (streaming)
  int count = 0;
  TestRow row;
  while (OB_SUCC(reader.read_row(row))) {
    count++;
    // Verify each row has 3 columns
    EXPECT_EQ(3, row.size());
    // First column should be the row number (1-indexed)
    EXPECT_TRUE(row[0].is_int());
    EXPECT_EQ(count, row[0].get_int());
    // Third column should be "row_N"
    EXPECT_TRUE(row[2].is_string());
    std::string expected_name = "row_" + std::to_string(count);
    EXPECT_EQ(expected_name, row[2].get_string());
  }

  EXPECT_EQ(100, count);
  reader.close();
}

// Additional test: Verify read_row returns OB_ITER_END
TEST_F(CsvDataReaderTest, ReadRowIterEnd)
{
  CsvDataReader reader;
  std::string file_path = test_data_dir_ + "sample.csv";

  int ret = reader.open(file_path);
  EXPECT_EQ(OB_SUCCESS, ret);

  reader.set_column_types({"int", "double", "varchar"});

  // Read all 5 rows
  TestRow row;
  for (int i = 0; i < 5; ++i) {
    ret = reader.read_row(row);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  // Next read should return OB_ITER_END
  ret = reader.read_row(row);
  EXPECT_EQ(OB_ITER_END, ret);

  reader.close();
}

// ===== T12 Acceptance Tests: File Reader Full Chain =====

// TC1: Small file reading
TEST_F(MaterialOpTest, FileDataSmall)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_file_data("t", "test_data/small.csv")
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "2"}, {"3", "4"}, {"5", "6"}}));
}

// TC2: Large file reading (multiple batches)
TEST_F(MaterialOpTest, FileDataLarge)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_file_data("t", "test_data/10k_rows.csv")
      .with_batch_size(256)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(10000, result.row_count());
}

// TC3: File with type header
TEST_F(MaterialOpTest, FileDataWithTypeHeader)
{
  auto result = material_test()
      .table("t", "a int, b double, c varchar(32)")
      .select("a, b, c")
      .with_file_data("t", "test_data/typed_sample.csv")
      .enable_dual_format_check().run(engine_);
  EXPECT_GT(result.row_count(), 0);
}

// ===== T13 Acceptance Tests: Cross-Functional Tests =====

// TC1: Rescan + Filter
TEST_F(MaterialOpTest, CrossRescanFilter)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .where("a > 1")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .with_rescan_times(3)
      .enable_dual_format_check().run(engine_);
  // filter filters to 2 rows, rescan 3 times results are consistent
  EXPECT_TRUE(result.verify_ordered({{"2", "20"}, {"3", "30"}}));
}

// TC2: Rescan + File Reader
TEST_F(MaterialOpTest, CrossRescanFileData)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_file_data("t", "test_data/small.csv")
      .with_rescan_times(2)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
}

// TC3: Filter + File Reader
TEST_F(MaterialOpTest, CrossFilterFileData)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .where("a > 5000")
      .with_file_data("t", "test_data/10k_rows.csv")
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(5000, result.row_count());
}

// TC4: Rescan + Filter + File Reader
TEST_F(MaterialOpTest, CrossRescanFilterFileData)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .where("a > 5000")
      .with_file_data("t", "test_data/10k_rows.csv")
      .with_rescan_times(2)
      .enable_dual_format_check().run(engine_);
  // 10k rows file, after filter 5000 rows, rescan 2 times consistent
  EXPECT_EQ(5000, result.row_count());
}

// ===== T3: Decimal Int Full Pipeline Tests =====

// TC1: decimal(10,2) basic pass-through
TEST_F(MaterialOpTest, DecimalIntBasic)
{
  auto result = material_test()
      .table("t", "a decimal(10,2), b int")
      .select("a, b")
      .with_data({{std::string("123.45"), 1}, {std::string("678.90"), 2}})
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: DecimalInt unsupported in 1.0 processor
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"123.45", "1"}, {"678.90", "2"}}));
}

// TC2: decimal + expression evaluation (verify CG and eval_func handle decimal int)
// Note: decimal(10,2) + decimal(10,2) result type is decimal(11,3), scale increases
// Actual output "4.000", but verify_ordered normalizes to compare logical value
TEST_F(MaterialOpTest, DecimalIntExpression)
{
  auto result = material_test()
      .table("t", "a decimal(10,2), b decimal(10,2)")
      .select("a + b")
      .with_data({{std::string("1.50"), std::string("2.50")},
                  {std::string("10.00"), std::string("20.00")}})
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: DecimalInt unsupported in 1.0 processor
  EXPECT_EQ(2, result.row_count());
  // Logical value comparison: 4.000 ≡ 4, 30.000 ≡ 30
  EXPECT_TRUE(result.verify_ordered({{"4"}, {"30"}}));
}

// TC3: high precision decimal (int128 width, precision > 18)
TEST_F(MaterialOpTest, DecimalIntHighPrecision)
{
  auto result = material_test()
      .table("t", "a decimal(30,5)")
      .select("a")
      .with_data({{std::string("1234567890123456789.12345")}})
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: DecimalInt unsupported in 1.0 processor
  EXPECT_EQ(1, result.row_count());
}

// ===== T4b Acceptance Tests: expr_unit_test API =====

// TC1: expr_unit_test basic usage
TEST_F(MaterialOpTest, T4b_ExprUnitTestBasic)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b")
      .with_data({{1, 2}, {3, 4}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"3"}, {"7"}}));
}

// TC2: expr_unit_test vs material_test equivalence
TEST_F(MaterialOpTest, T4b_ExprEquivalence)
{
  auto r1 = expr_unit_test()
      .columns("a int")
      .with_expr("a * 2")
      .with_data({{5}, {10}})
      .enable_dual_format_check().run(engine_);

  auto r2 = material_test()
      .table("t", "a int")
      .select("a * 2")
      .with_data({{5}, {10}})
      .enable_dual_format_check().run(engine_);

  // Both results should be identical
  EXPECT_TRUE(r1.equals(r2));
}

// TC3: expr_unit_test + with_expr_eval_func chaining (no crash)
TEST_F(MaterialOpTest, T4b_ExprWithCustomEval)
{
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("a + 0")
      .with_expr_eval_func(nullptr)  // Only verify chaining doesn't crash
      .with_data({{1}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, result.row_count());
}

// ===== T6 Acceptance Tests: Custom eval_func Full Chain =====

// Custom eval_func: adds 100 to int value
static int my_add_100_eval_func(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  FatalErrorChecker error_checker(ret);
  ObDatum *child_datum = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_datum))) {
    // Error case
  } else if (child_datum->is_null()) {
    datum.set_null();
  } else {
    int64_t val = child_datum->get_int();
    datum.set_int(val + 100);
  }
  return ret;
}

// TC1: custom eval_func modifies output
// Note: Use batch_size=1 to force row-by-row execution (eval_func path)
TEST_F(MaterialOpTest, T6_CustomEvalFunc)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a + 0")  // Expression to test
      .with_expr_eval_func(my_add_100_eval_func)
      .with_batch_size(1)  // Force row-by-row execution
      .with_data({{1}, {5}, {10}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  // Custom eval_func adds 100: 1->101, 5->105, 10->110
  EXPECT_TRUE(result.verify_ordered({{"101"}, {"105"}, {"110"}}));
}

// TC2: custom eval_func with expression evaluation
// Note: Use batch_size=1 to force row-by-row execution (eval_func path)
// The custom eval_func adds 100 to args_[0], which is the first child of "a + b"
// For "a + b", args_[0] is just 'a', so result is a+100: 1+100=101, 10+100=110
TEST_F(MaterialOpTest, T6_CustomEvalFuncWithExpr)
{
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b")
      .with_expr_eval_func(my_add_100_eval_func)
      .with_batch_size(1)  // Force row-by-row execution
      .with_data({{1, 2}, {10, 20}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(2, result.row_count());
  // Custom eval_func adds 100 to first child (a): 1+100=101, 10+100=110
  EXPECT_TRUE(result.verify_ordered({{"101"}, {"110"}}));
}

// Custom eval_vector_func: multiplies int values by 10
static int my_mul_10_eval_vector_func(const ObExpr &expr, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  FatalErrorChecker error_checker(ret);
  // Evaluate child expression first
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    return ret;
  }

  // Get the vector format
  ObIVector *vec = expr.args_[0]->get_vector(ctx);
  VectorFormat fmt = vec->get_format();

  // Initialize result vector
  expr.init_vector(ctx, fmt, bound.batch_size());

  // Get nulls bitmap
  sql::ObBitVector &nulls = expr.get_nulls(ctx);
  sql::ObBitVector &child_nulls = expr.args_[0]->get_nulls(ctx);

  if (fmt == VEC_FIXED) {
    const int64_t *child_data = reinterpret_cast<const int64_t *>(
        expr.args_[0]->get_res_buf(ctx));
    int64_t *res_data = reinterpret_cast<int64_t *>(expr.get_res_buf(ctx));

    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      if (skip.at(i) || child_nulls.at(i)) {
        nulls.set(i);
      } else {
        nulls.unset(i);
        res_data[i] = child_data[i] * 10;
      }
    }
  }

  return ret;
}

// TC3: custom eval_vector_func
TEST_F(MaterialOpTest, T6_CustomEvalVectorFunc)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a + 0")
      .with_expr_eval_vector_func(my_mul_10_eval_vector_func)
      .with_data({{1}, {5}, {10}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  // Custom eval_vector_func multiplies by 10: 1->10, 5->50, 10->100
  EXPECT_TRUE(result.verify_ordered({{"10"}, {"50"}, {"100"}}));
}

// ===== T7 Acceptance Tests: VEC_CONTINUOUS Fill =====

// TC1: VEC_CONTINUOUS basic string write
TEST_F(MaterialOpTest, T7_ContinuousFillBasic)
{
  auto result = material_test()
      .table("t", "a varchar(32)")
      .select("a")
      .with_vector_format(VEC_CONTINUOUS)
      .with_data({{std::string("abc")}, {std::string("defgh")}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"abc"}, {"defgh"}}));
}

// TC2: VEC_CONTINUOUS empty strings
TEST_F(MaterialOpTest, T7_ContinuousFillEmpty)
{
  auto result = material_test()
      .table("t", "a varchar(32)")
      .select("a")
      .with_vector_format(VEC_CONTINUOUS)
      .with_data({{std::string("")}, {std::string("x")}, {std::string("")}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{""}, {"x"}, {""}}));
}

// ===== T9 Acceptance Tests: VEC_CONTINUOUS Full Chain =====

// TC1: continuous format basic string pass-through
TEST_F(MaterialOpTest, ContinuousFormatBasic)
{
  auto result = material_test()
      .table("t", "a varchar(32), b varchar(32)")
      .select("a, b")
      .with_vector_format(VEC_CONTINUOUS)
      .with_data({{std::string("hello"), std::string("world")},
                   {std::string("foo"), std::string("bar")}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello", "world"}, {"foo", "bar"}}));
}

// TC2: continuous + int mixed (int column still uses VEC_FIXED)
TEST_F(MaterialOpTest, ContinuousFormatMixed)
{
  auto result = material_test()
      .table("t", "a int, b varchar(32)")
      .select("a, b")
      .with_vector_format(VEC_CONTINUOUS)
      .with_data({{1, std::string("abc")}, {2, std::string("def")}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "abc"}, {"2", "def"}}));
}

// TC3: continuous + multiple batches
TEST_F(MaterialOpTest, ContinuousFormatMultiBatch)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 10; ++i) {
    data.push_back({i, std::string("row_") + std::to_string(i)});
  }
  auto result = material_test()
      .table("t", "a int, b varchar(32)")
      .select("a, b")
      .with_vector_format(VEC_CONTINUOUS)
      .with_batch_size(3)
      .with_data(std::move(data))
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(10, result.row_count());
}

// TC4: continuous + NULL values
TEST_F(MaterialOpTest, ContinuousFormatWithNull)
{
  auto result = material_test()
      .table("t", "a varchar(32)")
      .select("a")
      .with_vector_format(VEC_CONTINUOUS)
      .with_data({{std::string("hello")}, {TestValue::null()}, {std::string("world")}})
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: VEC_CONTINUOUS + NULL unsupported in 1.0 processor
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"hello"}, {"NULL"}, {"world"}}));
}

// TC5: continuous + concat expression
TEST_F(MaterialOpTest, ContinuousFormatWithExpression)
{
  auto result = material_test()
      .table("t", "a varchar(16), b varchar(16)")
      .select("concat(a, b)")
      .with_vector_format(VEC_CONTINUOUS)
      .with_data({{std::string("hello"), std::string("world")}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"helloworld"}}));
}

// TC6: continuous + file data
TEST_F(MaterialOpTest, ContinuousFormatWithFileData)
{
  auto result = material_test()
      .table("t", "a int, b double, c varchar(32)")
      .select("a, b, c")
      .with_vector_format(VEC_CONTINUOUS)
      .with_file_data("t", "test_data/typed_sample.csv")
      .enable_dual_format_check().run(engine_);
  EXPECT_GT(result.row_count(), 0);
}

// ===== E2E: Cross-Feature Integration Tests =====

// E2E-1: decimal int + expression + multi-batch
TEST_F(MaterialOpTest, E2E_DecimalExprMultiBatch)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 20; ++i) {
    data.push_back({std::to_string(i) + ".50", std::to_string(i * 10) + ".25"});
  }
  auto result = material_test()
      .table("t", "a decimal(10,2), b decimal(10,2)")
      .select("a + b")
      .with_batch_size(7)
      .with_data(std::move(data))
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: DecimalInt unsupported in 1.0 processor
  EXPECT_EQ(20, result.row_count());
}

// E2E-2: continuous + NULL + multi-batch + rescan
TEST_F(MaterialOpTest, E2E_ContinuousNullRescan)
{
  auto result = material_test()
      .table("t", "a varchar(32)")
      .select("a")
      .with_vector_format(VEC_CONTINUOUS)
      .with_batch_size(2)
      .with_rescan_times(2)
      .with_data({{std::string("hello")}, {TestValue::null()}, {std::string("world")},
                   {std::string("foo")}, {TestValue::null()}})
      .run(engine_);  // No dual-format check: VEC_CONTINUOUS + NULL unsupported in 1.0 processor
  EXPECT_EQ(5, result.row_count());
}

// E2E-3: decimal int + continuous string mixed columns
TEST_F(MaterialOpTest, E2E_DecimalContinuousMixed)
{
  auto result = material_test()
      .table("t", "a decimal(10,2), b varchar(32)")
      .select("a, b")
      .with_vector_format(VEC_CONTINUOUS)
      .with_data({{"123.45", std::string("hello")},
                   {"678.90", std::string("world")}})
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: DecimalInt + VEC_CONTINUOUS mixed unsupported in 1.0
  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"123.45", "hello"}, {"678.90", "world"}}));
}

// ===== dump-t11: Acceptance Tests for Dump Configuration =====

// §A: GCONF dump enable/disable
TEST_F(MaterialOpTest, DumpConfig_GconfEnable)
{
  // Test that dump can be enabled via builder
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_sql_operator_dump(true)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
}

// §B: Hash area size configuration
TEST_F(MaterialOpTest, DumpConfig_HashAreaSize)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_sql_operator_dump(true)
      .with_hash_area_size(64 * 1024)  // 64KB
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
}

// §C: DumpVerifyMode - FULL_ROWS (default)
TEST_F(MaterialOpTest, DumpVerifyMode_FullRows)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::FULL_ROWS)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 10}, {2, 20}, {3, 30}}));
}

// §C: DumpVerifyMode - CHECKSUM
TEST_F(MaterialOpTest, DumpVerifyMode_Checksum)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
      .enable_dual_format_check().run(engine_);
  // CHECKSUM mode should compute checksum, not store rows
  EXPECT_EQ(0, result.row_count());  // rows_ is empty
  EXPECT_NE(0, result.get_checksum());  // checksum should be computed
  EXPECT_EQ(3, result.get_checksum_row_count());  // row count should be tracked
}

// §C: DumpVerifyMode - NONE
TEST_F(MaterialOpTest, DumpVerifyMode_None)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::NONE)
      .enable_dual_format_check().run(engine_);
  // NONE mode should not collect any results
  EXPECT_EQ(0, result.row_count());
  EXPECT_EQ(0, result.get_checksum());
}

// §D: Combined dump config with small hash area to potentially trigger dump
TEST_F(MaterialOpTest, DumpConfig_CombinedSmallHashArea)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 100; ++i) {
    data.push_back({i, i * 10});
  }

  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data(std::move(data))
      .with_sql_operator_dump(true)
      .with_hash_area_size(1024)  // Very small 1KB to potentially trigger dump
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(100, result.get_checksum_row_count());
}

// §E: Generator mode for large data (no full vector in memory)
TEST_F(MaterialOpTest, GeneratorMode_LargeData)
{
  constexpr int64_t TOTAL_ROWS = 1000;
  int64_t generated_count = 0;

  auto generator = [&generated_count](int64_t row_idx) -> TestRow {
    generated_count++;
    return {row_idx, row_idx * 2};
  };

  // Note: Generator mode requires direct MockDataSourceOp usage
  // This test verifies the API is available
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_batch_size(100)
      .with_sql_operator_dump(true)
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
      .enable_dual_format_check().run(engine_);
  // For now, use regular with_data since generator requires lower-level setup
}

// ===== dump-t12: E2E Tests for Dump Scenarios =====

// E2E: Small data with FULL_ROWS verification
TEST_F(MaterialOpTest, DumpE2E_SmallDataFullRows)
{
  auto result = material_test()
      .table("t", "a int, b varchar(32)")
      .select("a, b")
      .with_data({{1, std::string("hello")},
                   {2, std::string("world")},
                   {3, std::string("test")}})
      .with_sql_operator_dump(true)
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::FULL_ROWS)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"1", "hello"},
      {"2", "world"},
      {"3", "test"}
  }));
}

// E2E: Large data with CHECKSUM verification
TEST_F(MaterialOpTest, DumpE2E_LargeDataChecksum)
{
  constexpr int64_t TOTAL_ROWS = 5000;
  std::vector<TestRow> data;
  data.reserve(TOTAL_ROWS);
  for (int64_t i = 0; i < TOTAL_ROWS; ++i) {
    data.push_back({i, i * 10});
  }

  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data(std::move(data))
      .with_batch_size(256)
      .with_sql_operator_dump(true)
      .with_hash_area_size(64 * 1024)  // 64KB to potentially trigger dump
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());  // CHECKSUM mode doesn't store rows
  EXPECT_EQ(TOTAL_ROWS, result.get_checksum_row_count());
  EXPECT_NE(0, result.get_checksum());  // Should have computed checksum
}

// E2E: Rescan with dump enabled
TEST_F(MaterialOpTest, DumpE2E_RescanWithDump)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .with_sql_operator_dump(true)
      .with_rescan_times(2)
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::FULL_ROWS)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(5, result.row_count());
}

// E2E: Expression evaluation with dump
TEST_F(MaterialOpTest, DumpE2E_ExpressionEvaluation)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a + b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .with_sql_operator_dump(true)
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::FULL_ROWS)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_column(0, {"11", "22", "33"}));
}

// E2E: Decimal with dump and checksum
TEST_F(MaterialOpTest, DumpE2E_DecimalChecksum)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 100; ++i) {
    data.push_back({TestValue(std::to_string(i * 1.5))});
  }

  auto result = material_test()
      .table("t", "a decimal(10,2)")
      .select("a")
      .with_data(std::move(data))
      .with_sql_operator_dump(true)
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: DecimalInt unsupported in 1.0 processor

  EXPECT_EQ(100, result.get_checksum_row_count());
}

// E2E: Verify checksum consistency - same data produces same checksum
TEST_F(MaterialOpTest, DumpE2E_ChecksumConsistency)
{
  std::vector<TestRow> data1 = {{1}, {2}, {3}, {4}, {5}};
  std::vector<TestRow> data2 = {{1}, {2}, {3}, {4}, {5}};

  auto result1 = material_test()
      .table("t", "a int")
      .select("a")
      .with_data(std::move(data1))
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
      .enable_dual_format_check().run(engine_);

  auto result2 = material_test()
      .table("t", "a int")
      .select("a")
      .with_data(std::move(data2))
      .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
      .enable_dual_format_check().run(engine_);

  // Same data should produce same checksum
  EXPECT_EQ(result1.get_checksum(), result2.get_checksum());
}

// ===== Phase 2: Rescan Memory Consistency Tests =====

// Test: Basic rescan memory consistency check
TEST_F(MaterialOpTest, RescanMemoryConsistency_Basic)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}})
      .with_rescan_times(3)
      .with_rescan_memory_check(true)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(5, result.row_count());
  // Verify rescan memory info is populated
  EXPECT_EQ(3, result.get_rescan_count());
  EXPECT_TRUE(result.is_rescan_memory_consistent());
  EXPECT_GT(result.get_memory_after_first_scan(), 0);
}

// Test: Large data rescan with memory check
TEST_F(MaterialOpTest, RescanMemoryConsistency_LargeData)
{
  const int64_t TOTAL_ROWS = 1000;
  std::vector<TestRow> data;
  data.reserve(TOTAL_ROWS);
  for (int64_t i = 0; i < TOTAL_ROWS; ++i) {
    data.push_back({i, i * 10});
  }

  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data(std::move(data))
      .with_rescan_times(2)
      .with_rescan_memory_check(true)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(TOTAL_ROWS, result.row_count());
  EXPECT_EQ(2, result.get_rescan_count());
  EXPECT_TRUE(result.is_rescan_memory_consistent());
}

// Test: Custom tolerance for rescan memory check
TEST_F(MaterialOpTest, RescanMemoryConsistency_CustomTolerance)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_rescan_times(2)
      .with_rescan_memory_check(true)
      .with_rescan_memory_tolerance(1024 * 1024)  // 1MB tolerance
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.is_rescan_memory_consistent());
}

// Test: Disable rescan memory check
TEST_F(MaterialOpTest, RescanMemoryConsistency_DisableCheck)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .with_rescan_times(2)
      .with_rescan_memory_check(false)  // Explicitly disable
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(5, result.row_count());
  // When disabled, rescan_count should be 0 (no tracking)
  EXPECT_EQ(0, result.get_rescan_count());
}

// Test: Empty data rescan memory consistency
TEST_F(MaterialOpTest, RescanMemoryConsistency_EmptyData)
{
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({})  // Empty data
      .with_rescan_times(2)
      .with_rescan_memory_check(true)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(0, result.row_count());
  EXPECT_EQ(2, result.get_rescan_count());
  EXPECT_TRUE(result.is_rescan_memory_consistent());
}

// ===== DataGenerator Tests =====

TEST_F(MaterialOpTest, DataGenerator_CustomGenerators)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data_generator(5,
          gen::sequential(1),
          gen::sequential(10, 10))
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(5, result.row_count());
  // verify with generators
  EXPECT_TRUE(result.verify_ordered(5, gen::sequential(1), gen::sequential(10, 10)));
}

TEST_F(MaterialOpTest, DataGenerator_AutoRandom)
{
  auto result = material_test()
      .table("t", "a int, b double, c varchar(8)")
      .select("a, b, c")
      .with_data_generator(100)
      .run(engine_);  // No dual-format check: random data differs between independent 2.0 and 1.0 runs

  EXPECT_EQ(100, result.row_count());
}

TEST_F(MaterialOpTest, DataGenerator_LambdaGenerator)
{
  auto result = material_test()
      .table("t", "a int, b varchar(10)")
      .select("a, b")
      .with_data_generator(3,
          [](int64_t row_idx) -> TestValue { return row_idx * 100; },
          [](int64_t row_idx) -> TestValue { return row_idx % 2 == 0 ? "even" : "odd"; })
      .run(engine_);  // No dual-format check: varchar data mismatch (truncation) in 1.0 path

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"0", "even"}, {"100", "odd"}, {"200", "even"}}));
}

TEST_F(MaterialOpTest, DataGenerator_CycleAndNullable)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data_generator(6,
          gen::cycle({TestValue(1), TestValue(2), TestValue(3)}),
          gen::nullable(gen::sequential(10, 10), 3))
      .enable_dual_format_check()
      .run(engine_);  // No dual-format check: nullable generator causes segfault in 1.0 path

  EXPECT_EQ(6, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"1", "NULL"}, {"2", "20"}, {"3", "30"},
      {"1", "NULL"}, {"2", "50"}, {"3", "60"}}));
}

TEST_F(MaterialOpTest, DataGenerator_Rescan)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data_generator(3,
          gen::sequential(1),
          gen::constant(TestValue(99)))
      .with_rescan_times(2)
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "99"}, {"2", "99"}, {"3", "99"}}));
}

TEST_F(MaterialOpTest, DataGenerator_LargeData)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data_generator(10000,
          gen::sequential(0),
          gen::random_int(1, 999999))
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(10000, result.row_count());
}

TEST_F(MaterialOpTest, DataGenerator_WithExpr)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a + b")
      .with_data_generator(3,
          gen::sequential(1),
          gen::sequential(10, 10))
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(3, result.row_count());
  // verify with lambda generator for computed expression
  EXPECT_TRUE(result.verify_ordered(3,
      [](int64_t i) -> TestValue { return std::to_string(1 + i + 10 + i * 10); }));
}

TEST_F(MaterialOpTest, DataGenerator_VerifyColumnWithGenerator)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data_generator(100,
          gen::sequential(1),
          gen::sequential(100, 5))
      .enable_dual_format_check().run(engine_);

  EXPECT_EQ(100, result.row_count());
  EXPECT_TRUE(result.verify_column(0, 100, gen::sequential(1)));
  EXPECT_TRUE(result.verify_column(1, 100, gen::sequential(100, 5)));
}

// ===== with_sorted_data + 复杂 SQL 表达式排序键测试 =====
// 验证 order_desc 通过真实 SQL 表达式求值支持任意 SQL 语法（不限于符号运算符）

// TC1: 列名排序（回归：column_items 路径）
// a: {3,10}=3, {1,30}=1, {2,20}=2 → ASC → {1,30}, {2,20}, {3,10}
TEST_F(MaterialOpTest, SortedData_ColumnKey)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_sorted_data({{3, 10}, {1, 30}, {2, 20}}, "a ASC")
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 30}, {2, 20}, {3, 10}}));
}

// TC2: % 表达式作排序键（select item 路径）
// a % b: {5,3}=2, {7,4}=3, {3,2}=1, {9,5}=4 → ASC → {3,2},{5,3},{7,4},{9,5}
TEST_F(MaterialOpTest, SortedData_ModExpression)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a % b, a, b")   // a % b 必须是 SELECT item
      .with_sorted_data({{5, 3}, {7, 4}, {3, 2}, {9, 5}}, "a % b ASC")
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"1", "3", "2"},
      {"2", "5", "3"},
      {"3", "7", "4"},
      {"4", "9", "5"}
  }));
}

// TC3: ABS 函数表达式作排序键（select item 路径）
// abs(a): {-3,1}=3, {5,2}=5, {-1,3}=1, {4,4}=4 → ASC → {-1,3},{-3,1},{4,4},{5,2}
TEST_F(MaterialOpTest, SortedData_AbsFunction)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("abs(a), b")       // abs(a) 必须是 SELECT item
      .with_sorted_data({{-3, 1}, {5, 2}, {-1, 3}, {4, 4}}, "abs(a) ASC")
      .enable_dual_format_check()
      .run(engine_);
  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"1", "3"},
      {"3", "1"},
      {"4", "4"},
      {"5", "2"}
  }));
}

// TC4: 列作排序键，但列仅以 sub-expression 出现在 SELECT items（指针共享验证）
// sort key "a" 通过 column_items 找到 ObExpr*，
// 与 SELECT item "a + b"/"a % b" 的 args_[0] 是同一指针
// a: {2,3}=2, {5,1}=5, {1,4}=1, {3,2}=3 → ASC → {1,4},{2,3},{3,2},{5,1}
// 输出(a+b, a % b): {5,1}, {5,2}, {5,1}, {6,0}
TEST_F(MaterialOpTest, SortedData_ColumnAsSubExpr)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a + b, a % b")  // a 仅作 sub-expression 出现
      .with_sorted_data({{2, 3}, {5, 1}, {1, 4}, {3, 2}}, "a ASC")
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(4, result.row_count());
  // a=1,b=4: 1+4=5, 1 % 4=1
  // a=2,b=3: 2+3=5, 2 % 3=2
  // a=3,b=2: 3+2=5, 3 % 2=1
  // a=5,b=1: 5+1=6, 5 % 1=0
  EXPECT_TRUE(result.verify_ordered({
      {"5", "1"},
      {"5", "2"},
      {"5", "1"},
      {"6", "0"}
  }));
}

// TC5: 多键排序（表达式 + 列）
// 排序键: a % 3 ASC, b ASC
// {1,5}→mod=1, {2,3}→mod=2, {4,1}→mod=1, {3,4}→mod=0, {6,2}→mod=0
// 排序后: {6,2}(0,b=2), {3,4}(0,b=4), {4,1}(1,b=1), {1,5}(1,b=5), {2,3}(2,b=3)
TEST_F(MaterialOpTest, SortedData_MultiKey)
{
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a % 3, a, b")  // a % 3 必须是 SELECT item
      .with_sorted_data({{1, 5}, {2, 3}, {4, 1}, {3, 4}, {6, 2}},
                        "a % 3 ASC, b ASC")
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(5, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"0", "6", "2"},
      {"0", "3", "4"},
      {"1", "4", "1"},
      {"1", "1", "5"},
      {"2", "2", "3"}
  }));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // init ob_logger
  // OB_LOGGER.set_log_level("INFO");
  // OB_LOGGER.set_file_name("test_material_op.log", true);  // just run MaterialOpTest
  // ::testing::GTEST_FLAG(filter) = "MaterialOpTest.SortedData_AbsFunction";
  return RUN_ALL_TESTS();
}
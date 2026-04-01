/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_test/ob_op_test_engine.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/worker.h"  // for lib::is_oracle_mode, lib::is_mysql_mode

namespace oceanbase
{
namespace sql
{

// ===== Test Fixture =====

class OpTestEngineTest : public ::testing::Test
{
protected:
  OpTestEngineTest() {}
  virtual ~OpTestEngineTest() {}

  virtual void SetUp() override
  {
    engine_.init();
  }

  virtual void TearDown() override
  {
    engine_.destroy();
  }

protected:
  OpTestEngine engine_;
};

// ===== register_table Tests =====

// Test basic table registration with int columns
TEST_F(OpTestEngineTest, RegisterTableBasicInt)
{
  int ret = engine_.register_table("t1", "a int, b int");
  EXPECT_EQ(OB_SUCCESS, ret);

  // Verify table was registered by resolving a simple SELECT
  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a, b FROM t1", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);
}

// Test table registration with varchar columns
TEST_F(OpTestEngineTest, RegisterTableVarchar)
{
  int ret = engine_.register_table("t2", "name varchar(32), addr varchar(128)");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT name FROM t2", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test table registration with multiple types
TEST_F(OpTestEngineTest, RegisterTableMultipleTypes)
{
  int ret = engine_.register_table("t3", "a int, b varchar(32), c double, d number");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a, b, c, d FROM t3", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test registering same table twice (should succeed, idempotent)
TEST_F(OpTestEngineTest, RegisterTableIdempotent)
{
  int ret = engine_.register_table("t4", "a int");
  EXPECT_EQ(OB_SUCCESS, ret);

  // Register again with same name
  ret = engine_.register_table("t4", "a int");
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test register_table with null arguments (should fail)
TEST_F(OpTestEngineTest, RegisterTableNullArgs)
{
  int ret = engine_.register_table(nullptr, "a int");
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = engine_.register_table("t5", nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

// Test register_table with empty column defs
TEST_F(OpTestEngineTest, RegisterTableEmptyColDefs)
{
  // Empty column defs should still work (creates table with no columns)
  int ret = engine_.register_table("t6", "");
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ===== resolve_sql Tests =====

// Test resolving simple SELECT
TEST_F(OpTestEngineTest, ResolveSimpleSelect)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));

  ObDMLStmt *stmt = nullptr;
  int ret = engine_.resolve_sql("SELECT a, b FROM t", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);

  if (nullptr != stmt && stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    EXPECT_EQ(2, select_stmt->get_select_items().count());
  }
}

// Test resolving SELECT with expression
TEST_F(OpTestEngineTest, ResolveSelectWithExpression)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));

  ObDMLStmt *stmt = nullptr;
  int ret = engine_.resolve_sql("SELECT a + b FROM t", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);
}

// Test resolving SELECT with WHERE clause
TEST_F(OpTestEngineTest, ResolveSelectWithWhere)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));

  ObDMLStmt *stmt = nullptr;
  int ret = engine_.resolve_sql("SELECT a FROM t WHERE a > 10", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);
}

// Test resolving SELECT with GROUP BY
TEST_F(OpTestEngineTest, ResolveSelectWithGroupBy)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));

  ObDMLStmt *stmt = nullptr;
  int ret = engine_.resolve_sql("SELECT a, COUNT(*) FROM t GROUP BY a", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);

  if (nullptr != stmt && stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    EXPECT_EQ(1, select_stmt->get_group_exprs().count());
  }
}

// Test resolving SELECT with ORDER BY
TEST_F(OpTestEngineTest, ResolveSelectWithOrderBy)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));

  ObDMLStmt *stmt = nullptr;
  int ret = engine_.resolve_sql("SELECT a FROM t ORDER BY a DESC", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);

  if (nullptr != stmt && stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    EXPECT_EQ(1, select_stmt->get_order_items().count());
  }
}

// Test resolving SELECT non-existent table
// Note: Pass expected error to do_resolve to avoid assertion failure
TEST_F(OpTestEngineTest, ResolveSelectNonExistentTable)
{
  ObDMLStmt *stmt = nullptr;
  // For non-existent table, expect OB_TABLE_NOT_EXIST (-5019)
  // The resolver will fail when trying to resolve table reference
  // Pass the expected error so do_resolve doesn't assert
  int ret = engine_.resolve_sql("SELECT a FROM nonexistent_table", stmt, -OB_TABLE_NOT_EXIST);
  // Note: stmt may not be null even on error - the stmt object is created during resolve
  // The key is that we can pass expected error to avoid assertion failure
  // This test verifies that error handling works correctly with expect_error parameter
}

// Test resolving invalid SQL
// Note: do_resolve expects parsing to succeed before checking expect_error
// So we test with a valid SQL that will fail during resolve (not during parse)
TEST_F(OpTestEngineTest, ResolveInvalidSQL)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int"));

  ObDMLStmt *stmt = nullptr;
  // Select a column that doesn't exist - this will fail during resolve
  // For non-existent column, expect OB_ERR_BAD_FIELD_ERROR (-5217)
  int ret = engine_.resolve_sql("SELECT nonexistent_column FROM t", stmt, -OB_ERR_BAD_FIELD_ERROR);
  // This test verifies that error handling works correctly with expect_error parameter
}

// Test resolving SELECT with string literals (parameterized=false)
TEST_F(OpTestEngineTest, ResolveSelectWithStringLiterals)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a varchar(32)"));

  ObDMLStmt *stmt = nullptr;
  // String literals should remain as T_VARCHAR, not converted to T_QUESTIONMARK
  int ret = engine_.resolve_sql("SELECT a FROM t WHERE a = 'hello'", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);
}

// Test resolving SELECT with aggregate functions
TEST_F(OpTestEngineTest, ResolveSelectWithAggregate)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));

  ObDMLStmt *stmt = nullptr;
  int ret = engine_.resolve_sql("SELECT SUM(a), AVG(b), COUNT(*) FROM t", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nullptr, stmt);

  if (nullptr != stmt && stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    EXPECT_EQ(3, select_stmt->get_aggr_items().count());
  }
}

// Test statement type checking
TEST_F(OpTestEngineTest, StmtTypeCheck)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int"));

  ObDMLStmt *stmt = nullptr;
  int ret = engine_.resolve_sql("SELECT a FROM t", stmt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, stmt);

  EXPECT_TRUE(stmt->is_select_stmt());
  EXPECT_FALSE(stmt->is_insert_stmt());
  EXPECT_FALSE(stmt->is_update_stmt());
  EXPECT_FALSE(stmt->is_delete_stmt());
}

// Test column items after resolve
TEST_F(OpTestEngineTest, ColumnItemsAfterResolve)
{
  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b varchar(32), c double"));

  ObDMLStmt *stmt = nullptr;
  // Simple SELECT without WHERE to avoid implicit cast issues
  int ret = engine_.resolve_sql("SELECT a, b FROM t", stmt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, stmt);

  // Should have 2 column items (a and b in SELECT)
  const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();
  EXPECT_EQ(2, column_items.count());

  // Check that column expressions are properly typed
  for (int64_t i = 0; i < column_items.count(); ++i) {
    const ColumnItem &col_item = column_items.at(i);
    if (OB_NOT_NULL(col_item.expr_)) {
      EXPECT_TRUE(col_item.expr_->is_column_ref_expr());
    }
  }
}

// ===== SQL Mode Tests =====

// Test default SQL mode is MYSQL
TEST_F(OpTestEngineTest, DefaultSqlModeIsMysql)
{
  EXPECT_EQ(SqlMode::MYSQL, engine_.get_sql_mode());
}

// Test setting SQL mode to ORACLE
TEST_F(OpTestEngineTest, SetSqlModeOracle)
{
  engine_.destroy();  // Tear down first
  engine_.set_sql_mode(SqlMode::ORACLE);
  engine_.init();

  EXPECT_EQ(SqlMode::ORACLE, engine_.get_sql_mode());
  // Verify lib::is_oracle_mode() returns true after init
  EXPECT_TRUE(lib::is_oracle_mode());
  EXPECT_FALSE(lib::is_mysql_mode());
}

// Test setting SQL mode to MYSQL (explicit)
TEST_F(OpTestEngineTest, SetSqlModeMysql)
{
  engine_.destroy();  // Tear down first
  engine_.set_sql_mode(SqlMode::MYSQL);
  engine_.init();

  EXPECT_EQ(SqlMode::MYSQL, engine_.get_sql_mode());
  // Verify lib::is_mysql_mode() returns true after init
  EXPECT_TRUE(lib::is_mysql_mode());
  EXPECT_FALSE(lib::is_oracle_mode());
}

// Test mode switching between MYSQL and ORACLE
TEST_F(OpTestEngineTest, ModeSwitching)
{
  // Start in MYSQL mode (default)
  EXPECT_EQ(SqlMode::MYSQL, engine_.get_sql_mode());
  EXPECT_TRUE(lib::is_mysql_mode());

  // Switch to ORACLE mode
  engine_.destroy();
  engine_.set_sql_mode(SqlMode::ORACLE);
  engine_.init();
  EXPECT_TRUE(lib::is_oracle_mode());

  // Switch back to MYSQL mode
  engine_.destroy();
  engine_.set_sql_mode(SqlMode::MYSQL);
  engine_.init();
  EXPECT_TRUE(lib::is_mysql_mode());
}

// ===== Batch Size Tests =====

// Test default batch size is 256
TEST_F(OpTestEngineTest, DefaultBatchSize)
{
  EXPECT_EQ(256, engine_.get_batch_size());
}

// Test setting custom batch size
TEST_F(OpTestEngineTest, SetCustomBatchSize)
{
  engine_.set_batch_size(128);
  EXPECT_EQ(128, engine_.get_batch_size());

  engine_.set_batch_size(512);
  EXPECT_EQ(512, engine_.get_batch_size());
}

// Test batch size affects physical plan
TEST_F(OpTestEngineTest, BatchSizeAffectsPhyPlan)
{
  engine_.set_batch_size(64);

  ASSERT_EQ(OB_SUCCESS, engine_.register_table("t", "a int, b int"));
  ObDMLStmt *stmt = nullptr;
  ASSERT_EQ(OB_SUCCESS, engine_.resolve_sql("SELECT a, b FROM t", stmt));
  ASSERT_NE(nullptr, stmt);

  // generate_exprs should use the configured batch size
  int ret = engine_.generate_exprs(*stmt);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Verify physical plan batch size was set
  EXPECT_EQ(64, engine_.get_phy_plan().get_batch_size());
}

// ===== Number Type Tests =====

// Test number type in MySQL mode (default)
TEST_F(OpTestEngineTest, NumberTypeMySQLMode)
{
  // Default is MySQL mode
  int ret = engine_.register_table("t_num", "a number");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a FROM t_num", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test number(P, S) type with precision and scale
TEST_F(OpTestEngineTest, NumberWithPrecisionScale)
{
  int ret = engine_.register_table("t_num_ps", "a number(10, 2)");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a FROM t_num_ps", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test number(P) type with precision only
TEST_F(OpTestEngineTest, NumberWithPrecisionOnly)
{
  int ret = engine_.register_table("t_num_p", "a number(20)");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a FROM t_num_p", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test decimal type (alias for number)
TEST_F(OpTestEngineTest, DecimalType)
{
  int ret = engine_.register_table("t_dec", "a decimal(15, 4)");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a FROM t_dec", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test decimal without precision
TEST_F(OpTestEngineTest, DecimalWithoutPrecision)
{
  int ret = engine_.register_table("t_dec2", "a decimal");
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ===== Double Type Tests =====

// Test double without precision/scale
TEST_F(OpTestEngineTest, DoubleBasic)
{
  int ret = engine_.register_table("t_dbl", "a double");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a FROM t_dbl", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test double(M, S) with scale
TEST_F(OpTestEngineTest, DoubleWithScale)
{
  int ret = engine_.register_table("t_dbl_s", "a double(10, 2)");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a FROM t_dbl_s", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// Test double(M) without scale
TEST_F(OpTestEngineTest, DoubleWithPrecisionOnly)
{
  int ret = engine_.register_table("t_dbl_p", "a double(15)");
  EXPECT_EQ(OB_SUCCESS, ret);

  ObDMLStmt *stmt = nullptr;
  ret = engine_.resolve_sql("SELECT a FROM t_dbl_p", stmt);
  EXPECT_EQ(OB_SUCCESS, ret);
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
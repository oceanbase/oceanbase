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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/outline/ob_outline_template_matcher.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/parser/parse_node.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

// ==========================================================================
// Test: has_explicit_db_prefix
//
// This function determines whether the user wrote "db.table" or just "table"
// in the original SQL. It's used to decide:
//   db.table  -> *.*   (wildcard both)
//   table     -> *     (wildcard table only)
// ==========================================================================

class TestHasExplicitDbPrefix : public ::testing::Test
{
protected:
  ObArenaAllocator allocator_;

  ParseNode *make_node(ObItemType type, int num_child)
  {
    ParseNode *node = static_cast<ParseNode *>(allocator_.alloc(sizeof(ParseNode)));
    memset(node, 0, sizeof(ParseNode));
    node->type_ = type;
    node->num_child_ = num_child;
    if (num_child > 0) {
      node->children_ = static_cast<ParseNode **>(
          allocator_.alloc(sizeof(ParseNode *) * num_child));
      memset(node->children_, 0, sizeof(ParseNode *) * num_child);
    }
    return node;
  }

  // Build parse tree for: table_name (no db)
  //   T_RELATION_FACTOR
  //     children_[0] = NULL (no db node)
  ParseNode *make_table_node(const char *db_name = NULL)
  {
    ParseNode *rel = make_node(T_RELATION_FACTOR, 2);
    if (db_name != NULL) {
      ParseNode *db = make_node(T_IDENT, 0);
      db->str_value_ = db_name;
      db->str_len_ = strlen(db_name);
      rel->children_[0] = db;
    }
    return rel;
  }

  // Build parse tree for: table_name AS alias
  //   T_ALIAS
  //     children_[0] = T_RELATION_FACTOR (with optional db)
  ParseNode *make_alias_node(const char *db_name = NULL)
  {
    ParseNode *alias = make_node(T_ALIAS, 2);
    alias->children_[0] = make_table_node(db_name);
    return alias;
  }

  TableItem *make_table_item(const char *db_name, ParseNode *node)
  {
    TableItem *ti = static_cast<TableItem *>(allocator_.alloc(sizeof(TableItem)));
    new (ti) TableItem(allocator_);
    ti->database_name_ = (db_name != NULL) ? ObString::make_string(db_name) : ObString();
    ti->node_ = node;
    return ti;
  }
};

//
// Rule: NULL input -> false
//
TEST_F(TestHasExplicitDbPrefix, null_input_returns_false)
{
  EXPECT_FALSE(ObOutlineTemplateMatcher::has_explicit_db_prefix(NULL));
}

//
// Rule: database_name_ non-empty -> true (fast path, no need to check node)
// Scenario: resolver 已经填充了 database_name_ (比如 CREATE OUTLINE 路径)
//
TEST_F(TestHasExplicitDbPrefix, database_name_filled_returns_true)
{
  TableItem *ti = make_table_item("testdb", NULL);
  EXPECT_TRUE(ObOutlineTemplateMatcher::has_explicit_db_prefix(ti));
}

//
// Rule: database_name_ empty + no node -> false
// Scenario: 只有表名，ParseNode 也为空
//
TEST_F(TestHasExplicitDbPrefix, empty_db_null_node_returns_false)
{
  TableItem *ti = make_table_item(NULL, NULL);
  EXPECT_FALSE(ObOutlineTemplateMatcher::has_explicit_db_prefix(ti));
}

//
// ParseNode path: "SELECT * FROM db.tbl"
// T_RELATION_FACTOR -> children_[0] has db name
//
TEST_F(TestHasExplicitDbPrefix, parse_node_with_db_returns_true)
{
  ParseNode *node = make_table_node("mydb");        // db.tbl
  TableItem *ti = make_table_item(NULL, node);
  EXPECT_TRUE(ObOutlineTemplateMatcher::has_explicit_db_prefix(ti));
}

//
// ParseNode path: "SELECT * FROM tbl"
// T_RELATION_FACTOR -> children_[0] = NULL
//
TEST_F(TestHasExplicitDbPrefix, parse_node_without_db_returns_false)
{
  ParseNode *node = make_table_node(NULL);           // tbl (no db)
  TableItem *ti = make_table_item(NULL, node);
  EXPECT_FALSE(ObOutlineTemplateMatcher::has_explicit_db_prefix(ti));
}

//
// ParseNode path: "SELECT * FROM db.tbl AS t"
// T_ALIAS -> T_RELATION_FACTOR -> children_[0] has db name
//
TEST_F(TestHasExplicitDbPrefix, alias_node_with_db_returns_true)
{
  ParseNode *node = make_alias_node("mydb");         // db.tbl AS t
  TableItem *ti = make_table_item(NULL, node);
  EXPECT_TRUE(ObOutlineTemplateMatcher::has_explicit_db_prefix(ti));
}

//
// ParseNode path: "SELECT * FROM tbl AS t"
// T_ALIAS -> T_RELATION_FACTOR -> children_[0] = NULL
//
TEST_F(TestHasExplicitDbPrefix, alias_node_without_db_returns_false)
{
  ParseNode *node = make_alias_node(NULL);           // tbl AS t (no db)
  TableItem *ti = make_table_item(NULL, node);
  EXPECT_FALSE(ObOutlineTemplateMatcher::has_explicit_db_prefix(ti));
}

//
// Edge: unknown node type -> false
//
TEST_F(TestHasExplicitDbPrefix, unknown_node_type_returns_false)
{
  ParseNode *node = make_node(T_SELECT, 0);
  TableItem *ti = make_table_item(NULL, node);
  EXPECT_FALSE(ObOutlineTemplateMatcher::has_explicit_db_prefix(ti));
}

// ==========================================================================
// Test: Column ref wildcard printing
//
// When print_dbtbname_as_wildcard_ = 1, ObRawExprPrinter should:
//   - tbl.col        ->  *.col          (table without db prefix)
//   - db.tbl.col     ->  *.*.col        (table with db prefix)
//   - alias.col      ->  alias.col      (alias preserved, no wildcard)
//   - (flag off)     ->  db.tbl.col     (original output)
// ==========================================================================

class TestColumnRefWildcardPrint : public ::testing::Test
{
protected:
  ObArenaAllocator allocator_;

  // Helper: print a column ref and return the result string
  std::string do_print(ObColumnRefRawExpr &expr, ObDMLStmt *stmt, bool wildcard)
  {
    char buf[1024] = {0};
    int64_t pos = 0;
    ObObjPrintParams params;
    params.print_dbtbname_as_wildcard_ = wildcard ? 1 : 0;

    ObRawExprPrinter printer(buf, sizeof(buf), &pos, NULL, params, NULL);
    printer.set_stmt(stmt);
    EXPECT_EQ(OB_SUCCESS, printer.print(&expr));
    return std::string(buf, pos);
  }

  // Helper: set up a stmt with one table item and return the stmt
  void setup_stmt_with_table(ObSelectStmt &stmt, TableItem &ti,
                             uint64_t table_id,
                             const char *db_name,
                             const char *tbl_name)
  {
    new (&ti) TableItem(allocator_);
    ti.table_id_ = table_id;
    ti.table_name_ = ObString::make_string(tbl_name);
    ti.database_name_ = db_name ? ObString::make_string(db_name) : ObString();
    ti.type_ = TableItem::BASE_TABLE;
    ti.node_ = NULL;
    stmt.table_items_.push_back(&ti);
  }

  // Helper: set up a column ref expr
  void setup_column_ref(ObColumnRefRawExpr &expr,
                        uint64_t table_id,
                        const char *db_name,
                        const char *tbl_name,
                        const char *col_name,
                        bool from_alias)
  {
    expr.set_table_id(table_id);
    expr.set_table_name(ObString::make_string(tbl_name));
    expr.set_database_name(db_name ? ObString::make_string(db_name) : ObString());
    expr.set_column_name(ObString::make_string(col_name));
    expr.set_from_alias_table(from_alias);
  }
};

//
// SQL: SELECT tbl.col FROM tbl
// table has no explicit db prefix -> signature column ref: *.col
//
TEST_F(TestColumnRefWildcardPrint, table_without_db_becomes_star_dot_col)
{
  ObSelectStmt stmt(allocator_);
  TableItem ti(allocator_);
  setup_stmt_with_table(stmt, ti, 1001, NULL/*no db*/, "tbl");

  ObColumnRefRawExpr expr;
  setup_column_ref(expr, 1001, NULL, "tbl", "col", false);

  std::string result = do_print(expr, &stmt, true/*wildcard ON*/);

  // Expect: `*`.`col`  (one *, no db wildcard)
  EXPECT_NE(std::string::npos, result.find("*"))   << "output: " << result;
  EXPECT_NE(std::string::npos, result.find("col")) << "output: " << result;
  // Only one * (table), not two (no db prefix)
  size_t pos1 = result.find('*');
  size_t pos2 = result.find('*', pos1 + 1);
  EXPECT_EQ(std::string::npos, pos2)
      << "Expected single *, got: " << result;
}

//
// SQL: SELECT db.tbl.col FROM db.tbl
// table has explicit db prefix -> signature column ref: *.*.col
//
TEST_F(TestColumnRefWildcardPrint, table_with_db_becomes_star_star_dot_col)
{
  ObSelectStmt stmt(allocator_);
  TableItem ti(allocator_);
  setup_stmt_with_table(stmt, ti, 1002, "mydb", "tbl");

  ObColumnRefRawExpr expr;
  setup_column_ref(expr, 1002, "mydb", "tbl", "col", false);

  std::string result = do_print(expr, &stmt, true/*wildcard ON*/);

  // Expect: `*`.`*`.`col`  (two *'s + col)
  size_t pos1 = result.find('*');
  EXPECT_NE(std::string::npos, pos1)               << "output: " << result;
  size_t pos2 = result.find('*', pos1 + 1);
  EXPECT_NE(std::string::npos, pos2)
      << "Expected two *'s, got: " << result;
  EXPECT_NE(std::string::npos, result.find("col")) << "output: " << result;
}

//
// SQL: SELECT t.col FROM tbl t
// alias table -> column ref preserved as alias.col (no wildcard)
//
TEST_F(TestColumnRefWildcardPrint, alias_column_ref_preserved)
{
  ObSelectStmt stmt(allocator_);
  TableItem ti(allocator_);
  setup_stmt_with_table(stmt, ti, 1003, "mydb", "tbl");
  ti.alias_name_ = ObString::make_string("t");

  ObColumnRefRawExpr expr;
  setup_column_ref(expr, 1003, NULL, "t"/*alias*/, "col", true/*from_alias*/);

  std::string result = do_print(expr, &stmt, true/*wildcard ON*/);

  // Expect: `t`.`col`  (alias preserved, no *)
  EXPECT_EQ(std::string::npos, result.find("*"))
      << "Alias ref should have no *, got: " << result;
  EXPECT_NE(std::string::npos, result.find("t"))   << "output: " << result;
  EXPECT_NE(std::string::npos, result.find("col")) << "output: " << result;
}

//
// SQL: SELECT db.tbl.col FROM db.tbl  (wildcard mode OFF)
// -> should print original: db.tbl.col
//
TEST_F(TestColumnRefWildcardPrint, wildcard_off_prints_original)
{
  ObSelectStmt stmt(allocator_);
  TableItem ti(allocator_);
  setup_stmt_with_table(stmt, ti, 1004, "shopdb", "orders");

  ObColumnRefRawExpr expr;
  setup_column_ref(expr, 1004, "shopdb", "orders", "id", false);

  std::string result = do_print(expr, &stmt, false/*wildcard OFF*/);

  // Expect: `shopdb`.`orders`.`id`  (original names, no *)
  EXPECT_EQ(std::string::npos, result.find("*"))
      << "Wildcard off should have no *, got: " << result;
  EXPECT_NE(std::string::npos, result.find("shopdb"))  << "output: " << result;
  EXPECT_NE(std::string::npos, result.find("orders"))  << "output: " << result;
  EXPECT_NE(std::string::npos, result.find("id"))      << "output: " << result;
}

// ==========================================================================
// Test: print_dbtbname_as_wildcard_ flag in ObObjPrintParams
// ==========================================================================

TEST(TestPrintParams, wildcard_flag_defaults_to_zero)
{
  ObObjPrintParams params;
  EXPECT_EQ(0u, params.print_dbtbname_as_wildcard_);
}

TEST(TestPrintParams, wildcard_flag_independent_of_other_flags)
{
  ObObjPrintParams params;
  params.print_dbtbname_as_wildcard_ = 1;
  EXPECT_EQ(1u, params.print_dbtbname_as_wildcard_);
  EXPECT_EQ(0u, params.not_print_internal_catalog_);
  EXPECT_EQ(0u, params.for_dblink_);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

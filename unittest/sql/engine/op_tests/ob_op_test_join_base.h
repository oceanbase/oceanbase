/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_JOIN_BASE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_JOIN_BASE_H_

#include "lib/ob_errno.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/ob_sql_define.h"
#include "share/datum/ob_datum_funcs.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/random/ob_random.h"
#include "unittest/sql/engine/op_tests/ob_op_test_engine.h"
#include "unittest/sql/engine/op_tests/ob_op_test_result.h"
#include "unittest/sql/engine/op_tests/ob_op_test_types.h"
#include "unittest/sql/engine/op_tests/ob_op_test_file_data.h"
#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include <string>
#include <vector>
#include <map>
#include <set>
#include <functional>

namespace oceanbase
{
namespace sql
{

/**
 * @brief JoinSpecBuilder - CRTP base class for JOIN operator test specifications.
 *
 * Supports two-table JOIN with left/right children:
 * - Fluent API for left/right table definitions
 * - Automatic column expression splitting by table_id
 * - Smart output_exprs prefix matching (omit table prefix when unambiguous)
 *
 * Usage:
 *   MergeJoinTestSpec()
 *     .left_table("t1", "a int, b int")
 *     .right_table("t2", "c int, d int")
 *     .select("t1.a, t1.b, t2.c, t2.d")
 *     .with_left_sorted_data({{1, 2}, {3, 4}}, "a ASC")
 *     .with_right_sorted_data({{1, 10}, {3, 30}}, "c ASC")
 *     .join_type(INNER_JOIN)
 *     .on("t1.a = t2.c")
 *     .run(engine_);
 */
template <typename Derived>
class JoinSpecBuilder
{
public:
  JoinSpecBuilder()
    : resolved_stmt_(nullptr),
      phy_plan_(nullptr),
      left_resolved_table_id_(0),
      right_resolved_table_id_(0),
      left_table_name_(),
      left_col_defs_(),
      right_table_name_(),
      right_col_defs_(),
      select_exprs_(),
      join_condition_(),
      output_expr_strs_(),
      join_type_(UNKNOWN_JOIN),
      left_table_id_(0),
      right_table_id_(0),
      left_test_data_(),
      left_file_data_path_(),
      left_is_sorted_data_(false),
      left_sorted_desc_(),
      left_generator_row_num_(0),
      left_column_generators_(),
      left_use_generator_mode_(false),
      right_test_data_(),
      right_file_data_path_(),
      right_is_sorted_data_(false),
      right_sorted_desc_(),
      right_generator_row_num_(0),
      right_column_generators_(),
      right_use_generator_mode_(false),
      batch_size_(256),
      rescan_times_(0),
      rich_format_(true),
      dual_format_check_(false),
      prepared_left_col_exprs_vec_(),
      prepared_right_col_exprs_vec_(),
      prepared_out_exprs_vec_(),
      prepared_cond_exprs_vec_()
  {}
  virtual ~JoinSpecBuilder() = default;

  // ===== Virtual Interface for Derived Classes =====

  /**
   * @brief Create JOIN operator spec (e.g., ObMergeJoinVecSpec).
   * Override in derived classes.
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc,
                        MockDataSourceSpec *left_mock_spec,
                        MockDataSourceSpec *right_mock_spec,
                        const ExprFixedArray &output_exprs,
                        bool use_rich_format)
  {
    return nullptr;  // Default: no parent spec
  }

  /**
   * @brief Create operator input (ObOpInput) for operators that need it.
   */
  ObOpInput *create_input(ObExecContext &ctx, ObOpSpec &spec)
  {
    return nullptr;  // Default: no OpInput needed
  }

  /**
   * @brief Create JOIN operator (e.g., ObMergeJoinVecOp).
   * Override in derived classes.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec,
                        ObOperator *left_op, ObOperator *right_op)
  {
    return nullptr;  // Default: no parent op
  }

  /**
   * @brief Template method to create JOIN operator with OpInput support.
   */
  template <typename OpType>
  ObOperator *default_create_join_op(ObExecContext &ctx, ObOpSpec &spec,
                                      ObOperator *left_op, ObOperator *right_op)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    Derived *derived = static_cast<Derived *>(this);

    // Step 1: Get OpInput
    ObOpInput *input = derived->create_input(ctx, spec);

    // Step 2: Allocate operator
    void *mem = ctx.get_allocator().alloc(sizeof(OpType));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc operator failed", K(ret));
      return nullptr;
    }
    OpType *op = new (mem) OpType(ctx, spec, input);

    // Step 3: Set children pointer (two children)
    void *children_mem = ctx.get_allocator().alloc(sizeof(ObOperator *) * 2);
    if (OB_ISNULL(children_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOperator **children = reinterpret_cast<ObOperator **>(children_mem);
    children[0] = left_op;
    children[1] = right_op;
    if (OB_FAIL(op->set_children_pointer(children, 2))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    return op;
  }

  // ===== Fluent API for Two Tables =====

  /**
   * @brief Set left table name and column definitions.
   */
  Derived& left_table(const char *table_name, const char *col_defs)
  {
    left_table_name_ = table_name;
    left_col_defs_ = col_defs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set right table name and column definitions.
   */
  Derived& right_table(const char *table_name, const char *col_defs)
  {
    right_table_name_ = table_name;
    right_col_defs_ = col_defs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set SELECT expressions.
   */
  Derived& select(const char *exprs)
  {
    select_exprs_ = exprs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set JOIN type (INNER_JOIN, LEFT_OUTER_JOIN, etc.).
   */
  Derived& join_type(ObJoinType type)
  {
    join_type_ = type;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set ON condition.
   */
  Derived& on(const char *condition)
  {
    join_condition_ = condition;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set explicit output expressions with smart prefix matching.
   */
  Derived& with_output_exprs(std::initializer_list<const char *> exprs)
  {
    output_expr_strs_.clear();
    for (const char *expr : exprs) {
      output_expr_strs_.push_back(std::string(expr));
    }
    return static_cast<Derived&>(*this);
  }

  // ===== Data Injection for Left Table =====

  Derived& with_left_data(const std::vector<TestRow> &rows)
  {
    left_test_data_ = rows;
    return static_cast<Derived&>(*this);
  }

  Derived& with_left_data(std::vector<TestRow> &&rows)
  {
    left_test_data_ = std::move(rows);
    return static_cast<Derived&>(*this);
  }

  Derived& with_left_sorted_data(const std::vector<TestRow> &rows, const char *order_desc)
  {
    left_test_data_ = rows;
    left_is_sorted_data_ = true;
    if (order_desc) {
      left_sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  Derived& with_left_sorted_data(std::vector<TestRow> &&rows, const char *order_desc)
  {
    left_test_data_ = std::move(rows);
    left_is_sorted_data_ = true;
    if (order_desc) {
      left_sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  Derived& with_left_file_data(const std::string &file_path)
  {
    left_file_data_path_ = file_path;
    return static_cast<Derived&>(*this);
  }

  template <typename... Generators>
  Derived& with_left_data_generator(int64_t row_num, Generators&&... generators)
  {
    left_generator_row_num_ = row_num;
    left_column_generators_ = {ColumnGenerator(std::forward<Generators>(generators))...};
    left_use_generator_mode_ = true;
    return static_cast<Derived&>(*this);
  }

  // ===== Data Injection for Right Table =====

  Derived& with_right_data(const std::vector<TestRow> &rows)
  {
    right_test_data_ = rows;
    return static_cast<Derived&>(*this);
  }

  Derived& with_right_data(std::vector<TestRow> &&rows)
  {
    right_test_data_ = std::move(rows);
    return static_cast<Derived&>(*this);
  }

  Derived& with_right_sorted_data(const std::vector<TestRow> &rows, const char *order_desc)
  {
    right_test_data_ = rows;
    right_is_sorted_data_ = true;
    if (order_desc) {
      right_sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  Derived& with_right_sorted_data(std::vector<TestRow> &&rows, const char *order_desc)
  {
    right_test_data_ = std::move(rows);
    right_is_sorted_data_ = true;
    if (order_desc) {
      right_sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  Derived& with_right_file_data(const std::string &file_path)
  {
    right_file_data_path_ = file_path;
    return static_cast<Derived&>(*this);
  }

  template <typename... Generators>
  Derived& with_right_data_generator(int64_t row_num, Generators&&... generators)
  {
    right_generator_row_num_ = row_num;
    right_column_generators_ = {ColumnGenerator(std::forward<Generators>(generators))...};
    right_use_generator_mode_ = true;
    return static_cast<Derived&>(*this);
  }

  // ===== Configuration =====

  Derived& with_batch_size(int64_t batch_size)
  {
    batch_size_ = batch_size;
    return static_cast<Derived&>(*this);
  }

  Derived& with_rescan_times(int64_t times)
  {
    rescan_times_ = times;
    return static_cast<Derived&>(*this);
  }

  Derived& enable_dual_format_check(bool enable = true)
  {
    dual_format_check_ = enable;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Enable dual format check with unordered (set-based) comparison.
   * Use for operators whose output order is non-deterministic (Hash Join, etc.)
   * where 1.0 and 2.0 may return the same rows in different order.
   */
  Derived& enable_dual_format_unordered_check()
  {
    dual_format_check_ = true;
    dual_format_unordered_ = true;
    return static_cast<Derived&>(*this);
  }

  Derived& with_rich_format(bool enable)
  {
    rich_format_ = enable;
    return static_cast<Derived&>(*this);
  }

  // ===== Execution =====

  /**
   * @brief Prepare: register tables, resolve SQL, generate expressions.
   */
  int prepare(OpTestEngine &engine)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    // Step 0: Configure engine
    engine.set_batch_size(batch_size_);

    // Step 1: Register left table
    ret = engine.register_table(left_table_name_.c_str(), left_col_defs_.c_str());
    if (OB_FAIL(ret)) {
      LOG_WARN("register left table failed", K(ret), K(left_table_name_.c_str()));
      return ret;
    }

    // Step 2: Register right table
    ret = engine.register_table(right_table_name_.c_str(), right_col_defs_.c_str());
    if (OB_FAIL(ret)) {
      LOG_WARN("register right table failed", K(ret), K(right_table_name_.c_str()));
      return ret;
    }

    // Step 3: Build JOIN SQL
    std::string sql = build_join_sql();

    // Step 4: Resolve SQL
    ObDMLStmt *stmt = nullptr;
    ret = engine.resolve_sql(sql, stmt);
    if (OB_FAIL(ret) || OB_ISNULL(stmt)) {
      LOG_WARN("resolve join sql failed", K(ret), K(sql.c_str()));
      return OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
    }

    // Step 5: Generate expressions
    // Register join conditions as pending raw exprs so they get code generated
    const common::ObIArray<JoinedTable *> &joined_tables_for_pend = stmt->get_joined_tables();
    for (int64_t i = 0; i < joined_tables_for_pend.count(); ++i) {
      JoinedTable *jt = joined_tables_for_pend.at(i);
      if (OB_NOT_NULL(jt)) {
        const common::ObIArray<ObRawExpr *> &join_conds = jt->get_join_conditions();
        for (int64_t j = 0; j < join_conds.count(); ++j) {
          if (OB_NOT_NULL(join_conds.at(j))) {
            engine.add_pending_raw_expr(join_conds.at(j));
          }
        }
      }
    }
    ret = engine.generate_exprs(*stmt);
    if (OB_FAIL(ret)) {
      LOG_WARN("generate exprs failed", K(ret));
      return ret;
    }

    ObSelectStmt *select_stmt = stmt->is_select_stmt() ?
                                 static_cast<ObSelectStmt *>(stmt) : nullptr;
    if (OB_ISNULL(select_stmt)) {
      LOG_WARN("stmt is not select stmt");
      return OB_ERR_UNEXPECTED;
    }

    resolved_stmt_ = select_stmt;
    phy_plan_ = &engine.get_phy_plan();

    // Step 6: Split column_exprs by table_id
    const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();
    const common::ObIArray<TableItem *> &tables = select_stmt->get_table_items();

    if (tables.count() < 2) {
      LOG_WARN("expected at least 2 tables", K(tables.count()));
      return OB_ERR_UNEXPECTED;
    }

    // Find table_id for left and right tables
    uint64_t left_resolved_table_id = 0;
    uint64_t right_resolved_table_id = 0;
    for (int64_t i = 0; i < tables.count(); ++i) {
      TableItem *table = tables.at(i);
      if (OB_NOT_NULL(table)) {
        std::string table_name(table->table_name_.ptr(), table->table_name_.length());
        if (table_name == left_table_name_) {
          left_resolved_table_id = table->table_id_;
        } else if (table_name == right_table_name_) {
          right_resolved_table_id = table->table_id_;
        }
      }
    }

    // Split column expressions
    prepared_left_col_exprs_vec_.clear();
    prepared_right_col_exprs_vec_.clear();
    for (int64_t i = 0; i < column_items.count(); ++i) {
      const ColumnItem &col_item = column_items.at(i);
      if (OB_NOT_NULL(col_item.expr_)) {
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*col_item.expr_);
        if (OB_NOT_NULL(expr)) {
          if (col_item.table_id_ == left_resolved_table_id) {
            prepared_left_col_exprs_vec_.push_back(expr);
          } else if (col_item.table_id_ == right_resolved_table_id) {
            prepared_right_col_exprs_vec_.push_back(expr);
          }
        }
      }
    }

    // Step 7: Collect output_exprs with smart prefix matching
    const common::ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    prepared_out_exprs_vec_.clear();
    if (!output_expr_strs_.empty()) {
      ret = collect_output_exprs_with_smart_prefix(select_items, column_items,
                                                    left_resolved_table_id,
                                                    right_resolved_table_id);
      if (OB_FAIL(ret)) {
        LOG_WARN("collect output exprs failed", K(ret));
        return ret;
      }
    } else {
      for (int64_t i = 0; i < select_items.count(); ++i) {
        if (OB_NOT_NULL(select_items.at(i).expr_)) {
          ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*select_items.at(i).expr_);
          if (OB_NOT_NULL(expr)) {
            prepared_out_exprs_vec_.push_back(expr);
          }
        }
      }
    }

    // Step 8: Collect ON condition expressions
    const common::ObIArray<ObRawExpr *> &cond_exprs = select_stmt->get_condition_exprs();
    prepared_cond_exprs_vec_.clear();
    for (int64_t i = 0; i < cond_exprs.count(); ++i) {
      ObRawExpr *raw_expr = cond_exprs.at(i);
      if (OB_NOT_NULL(raw_expr)) {
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw_expr);
        if (OB_NOT_NULL(expr)) {
          prepared_cond_exprs_vec_.push_back(expr);
        }
      }
    }

    // Save resolved table IDs for derived classes
    left_resolved_table_id_ = left_resolved_table_id;
    right_resolved_table_id_ = right_resolved_table_id;

    return ret;
  }

  /**
   * @brief Build and execute: create two MockDataSourceOps, build JOIN op, run.
   */
  OpTestResult build_and_execute(OpTestEngine &engine, bool use_rich_format)
  {
    OpTestResult result;
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    // Build ExprFixedArrays
    ExprFixedArray left_col_exprs(engine.get_phy_plan().get_allocator());
    ExprFixedArray right_col_exprs(engine.get_phy_plan().get_allocator());
    ExprFixedArray output_exprs(engine.get_phy_plan().get_allocator());

    int expr_ret = left_col_exprs.init(static_cast<int64_t>(prepared_left_col_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) { return result; }
    expr_ret = left_col_exprs.prepare_allocate(static_cast<int64_t>(prepared_left_col_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) { return result; }
    for (size_t i = 0; i < prepared_left_col_exprs_vec_.size(); ++i) {
      left_col_exprs.at(i) = prepared_left_col_exprs_vec_[i];
    }

    expr_ret = right_col_exprs.init(static_cast<int64_t>(prepared_right_col_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) { return result; }
    expr_ret = right_col_exprs.prepare_allocate(static_cast<int64_t>(prepared_right_col_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) { return result; }
    for (size_t i = 0; i < prepared_right_col_exprs_vec_.size(); ++i) {
      right_col_exprs.at(i) = prepared_right_col_exprs_vec_[i];
    }

    expr_ret = output_exprs.init(static_cast<int64_t>(prepared_out_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) { return result; }
    expr_ret = output_exprs.prepare_allocate(static_cast<int64_t>(prepared_out_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) { return result; }
    for (size_t i = 0; i < prepared_out_exprs_vec_.size(); ++i) {
      output_exprs.at(i) = prepared_out_exprs_vec_[i];
    }

    // Create left MockDataSourceOp
    MockDataSourceOp *left_mock_op = engine.create_mock_data_source(left_col_exprs, nullptr);
    if (OB_ISNULL(left_mock_op)) {
      LOG_WARN("create left mock data source failed");
      return result;
    }

    // Set left data
    if (left_use_generator_mode_) {
      std::vector<ColumnGenerator> col_gens = left_column_generators_;
      RowGenerator row_gen = [col_gens](int64_t row_idx) -> TestRow {
        std::vector<TestValue> values;
        values.reserve(col_gens.size());
        for (const auto &g : col_gens) {
          values.push_back(g(row_idx));
        }
        return TestRow(std::move(values));
      };
      left_mock_op->set_row_generator(std::move(row_gen), left_generator_row_num_);
    } else {
      std::vector<TestRow> left_data = left_test_data_;
      if (!left_file_data_path_.empty()) {
        CsvDataReader reader;
        ret = reader.open(left_file_data_path_);
        if (OB_FAIL(ret)) { return result; }
        ret = reader.read_all(left_data);
        reader.close();
        if (OB_FAIL(ret)) { return result; }
      }
      left_mock_op->set_test_data(left_data);
    }

    // Create right MockDataSourceOp
    MockDataSourceOp *right_mock_op = engine.create_mock_data_source(right_col_exprs, nullptr);
    if (OB_ISNULL(right_mock_op)) {
      LOG_WARN("create right mock data source failed");
      return result;
    }

    // Set right data
    if (right_use_generator_mode_) {
      std::vector<ColumnGenerator> col_gens = right_column_generators_;
      RowGenerator row_gen = [col_gens](int64_t row_idx) -> TestRow {
        std::vector<TestValue> values;
        values.reserve(col_gens.size());
        for (const auto &g : col_gens) {
          values.push_back(g(row_idx));
        }
        return TestRow(std::move(values));
      };
      right_mock_op->set_row_generator(std::move(row_gen), right_generator_row_num_);
    } else {
      std::vector<TestRow> right_data = right_test_data_;
      if (!right_file_data_path_.empty()) {
        CsvDataReader reader;
        ret = reader.open(right_file_data_path_);
        if (OB_FAIL(ret)) { return result; }
        ret = reader.read_all(right_data);
        reader.close();
        if (OB_FAIL(ret)) { return result; }
      }
      right_mock_op->set_test_data(right_data);
    }

    // Get specs from mock ops
    MockDataSourceSpec *left_mock_spec = static_cast<MockDataSourceSpec *>(
        const_cast<ObOpSpec *>(&left_mock_op->get_spec()));
    MockDataSourceSpec *right_mock_spec = static_cast<MockDataSourceSpec *>(
        const_cast<ObOpSpec *>(&right_mock_op->get_spec()));

    // Call derived class to create JOIN spec
    Derived *derived = static_cast<Derived *>(this);
    ObOpSpec *join_spec = derived->create_spec(engine.get_allocator(),
                                                left_mock_spec, right_mock_spec,
                                                output_exprs, use_rich_format);

    // Link children
    if (OB_NOT_NULL(join_spec)) {
      void *children_mem = engine.get_allocator().alloc(sizeof(ObOpSpec *) * 2);
      if (OB_ISNULL(children_mem)) {
        LOG_WARN("alloc children array failed");
        return result;
      }
      ObOpSpec **children = reinterpret_cast<ObOpSpec **>(children_mem);
      children[0] = left_mock_spec;
      children[1] = right_mock_spec;
      if (OB_FAIL(join_spec->set_children_pointer(children, 2))) {
        LOG_WARN("set children pointer failed", K(ret));
        return result;
      }
    }

    // Create JOIN operator
    ObOperator *join_op = nullptr;
    if (OB_NOT_NULL(join_spec)) {
      join_op = derived->create_op(engine.get_exec_ctx(), *join_spec, left_mock_op, right_mock_op);
    }

    if (OB_ISNULL(join_op)) {
      LOG_WARN("create join operator failed");
      return result;
    }

    // Execute
    if (datahub_setup_fn_ && OB_NOT_NULL(join_spec)) {
      if (OB_SUCC(ret)) {
        ret = datahub_setup_fn_(engine.get_exec_ctx(), join_spec->id_);
      }
      if (OB_SUCC(ret)) {
        result = engine.execute(join_op, &output_exprs, rescan_times_);
      }
    } else {
      result = engine.execute(join_op, &output_exprs, rescan_times_);
    }

    return result;
  }

  /**
   * @brief Main entry: prepare + build_and_execute.
   */
  OpTestResult run(OpTestEngine &engine)
  {
    OpTestResult result;
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    ret = prepare(engine);
    if (OB_FAIL(ret)) {
      LOG_WARN("prepare failed", K(ret));
      return result;
    }

    if (dual_format_check_) {
      engine.enable_rich_format(true);
      OpTestResult res_2_0 = build_and_execute(engine, true);

      engine.enable_rich_format(false);
      OpTestResult res_1_0 = build_and_execute(engine, false);

      engine.enable_rich_format(true);

      EXPECT_EQ(res_2_0.row_count(), res_1_0.row_count());
      if (res_2_0.row_count() == res_1_0.row_count()) {
        if (dual_format_unordered_) {
          auto rows_2_0 = res_2_0.get_rows();
          auto rows_1_0 = res_1_0.get_rows();
          std::sort(rows_2_0.begin(), rows_2_0.end());
          std::sort(rows_1_0.begin(), rows_1_0.end());
          for (int64_t i = 0; i < res_2_0.row_count(); ++i) {
            EXPECT_EQ(rows_2_0[i], rows_1_0[i])
                << "dual_format_check: row " << i << " mismatch between 2.0 and 1.0 (unordered)";
          }
        } else {
          for (int64_t i = 0; i < res_2_0.row_count(); ++i) {
            EXPECT_EQ(res_2_0.get_row(i), res_1_0.get_row(i));
          }
        }
      }
      return res_2_0;
    } else {
      engine.enable_rich_format(rich_format_);
      result = build_and_execute(engine, rich_format_);
      if (!rich_format_) {
        engine.enable_rich_format(true);
      }
      return result;
    }
  }

protected:
  /**
   * @brief Build JOIN SQL statement.
   */
  std::string build_join_sql() const
  {
    std::string sql = "SELECT ";

    if (!select_exprs_.empty()) {
      sql += select_exprs_;
    } else {
      sql += "*";
    }

    sql += " FROM ";
    sql += left_table_name_;

    switch (join_type_) {
      case INNER_JOIN:
        sql += " INNER JOIN ";
        break;
      case LEFT_OUTER_JOIN:
        sql += " LEFT JOIN ";
        break;
      case RIGHT_OUTER_JOIN:
        sql += " RIGHT JOIN ";
        break;
      case FULL_OUTER_JOIN:
        sql += " FULL JOIN ";
        break;
      default:
        sql += " JOIN ";
        break;
    }

    sql += right_table_name_;

    if (!join_condition_.empty()) {
      sql += " ON ";
      sql += join_condition_;
    }

    return sql;
  }

  /**
   * @brief Collect output expressions with smart prefix matching.
   */
  int collect_output_exprs_with_smart_prefix(
      const common::ObIArray<SelectItem> &select_items,
      const common::ObIArray<ColumnItem> &column_items,
      uint64_t left_table_id,
      uint64_t right_table_id)
  {
    int ret = OB_SUCCESS;
    char name_buf[512];

    // Build column name -> table_id mapping for ambiguity detection
    std::map<std::string, std::vector<uint64_t>> col_name_to_tables;
    for (int64_t i = 0; i < column_items.count(); ++i) {
      const ColumnItem &col = column_items.at(i);
      std::string col_name(col.column_name_.ptr(), col.column_name_.length());
      col_name_to_tables[col_name].push_back(col.table_id_);
    }

    for (const std::string &target : output_expr_strs_) {
      bool found = false;

      // Try exact match first
      for (int64_t i = 0; i < select_items.count() && !found; ++i) {
        ObRawExpr *raw = select_items.at(i).expr_;
        if (OB_ISNULL(raw)) continue;

        int64_t pos = 0;
        if (OB_SUCCESS == raw->get_name(name_buf, sizeof(name_buf) - 1, pos)) {
          std::string expr_str(name_buf, pos);

          if (target == expr_str) {
            ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw);
            if (OB_NOT_NULL(expr)) {
              prepared_out_exprs_vec_.push_back(expr);
              found = true;
              break;
            }
          }
        }
      }

      if (found) continue;

      // Try prefix omission (target is just column name without table prefix)
      if (target.find('.') == std::string::npos) {
        auto it = col_name_to_tables.find(target);
        if (it != col_name_to_tables.end()) {
          if (it->second.size() > 1) {
            LOG_WARN("ambiguous column name, table prefix required", K(target.c_str()));
            ret = OB_ERR_UNEXPECTED;
            return ret;
          }
        }

        for (int64_t i = 0; i < select_items.count() && !found; ++i) {
          ObRawExpr *raw = select_items.at(i).expr_;
          if (OB_ISNULL(raw)) continue;

          int64_t pos = 0;
          if (OB_SUCCESS == raw->get_name(name_buf, sizeof(name_buf) - 1, pos)) {
            std::string expr_str(name_buf, pos);
            std::string suffix = "." + target;
            if (expr_str.length() > suffix.length() &&
                expr_str.substr(expr_str.length() - suffix.length()) == suffix) {
              ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw);
              if (OB_NOT_NULL(expr)) {
                prepared_out_exprs_vec_.push_back(expr);
                found = true;
                break;
              }
            }
          }
        }
      }

      if (!found) {
        LOG_WARN("output expression not found", K(target.c_str()));
      }
    }

    return ret;
  }

protected:
  // Context for derived classes
  ObSelectStmt *resolved_stmt_;
  ObPhysicalPlan *phy_plan_;
  uint64_t left_resolved_table_id_;
  uint64_t right_resolved_table_id_;

  // Table definitions
  std::string left_table_name_;
  std::string left_col_defs_;
  std::string right_table_name_;
  std::string right_col_defs_;

  // SQL clauses
  std::string select_exprs_;
  std::string join_condition_;
  std::vector<std::string> output_expr_strs_;
  ObJoinType join_type_;

  // Table IDs (from registration)
  uint64_t left_table_id_;
  uint64_t right_table_id_;

  // Left table data
  std::vector<TestRow> left_test_data_;
  std::string left_file_data_path_;
  bool left_is_sorted_data_;
  std::string left_sorted_desc_;
  int64_t left_generator_row_num_;
  std::vector<ColumnGenerator> left_column_generators_;
  bool left_use_generator_mode_;

  // Right table data
  std::vector<TestRow> right_test_data_;
  std::string right_file_data_path_;
  bool right_is_sorted_data_;
  std::string right_sorted_desc_;
  int64_t right_generator_row_num_;
  std::vector<ColumnGenerator> right_column_generators_;
  bool right_use_generator_mode_;

  // Configuration
  int64_t batch_size_;
  int64_t rescan_times_;
  bool rich_format_;
  bool dual_format_check_;
  bool dual_format_unordered_ = false;   // When true, sort rows before comparing 1.0 vs 2.0

  // Intermediate state
  std::vector<ObExpr *> prepared_left_col_exprs_vec_;
  std::vector<ObExpr *> prepared_right_col_exprs_vec_;
  std::vector<ObExpr *> prepared_out_exprs_vec_;
  std::vector<ObExpr *> prepared_cond_exprs_vec_;

  std::function<int(ObExecContext&, uint64_t)> datahub_setup_fn_;

  Derived& set_datahub_fn(std::function<int(ObExecContext&, uint64_t)> fn)
  {
    datahub_setup_fn_ = std::move(fn);
    return static_cast<Derived&>(*this);
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TESTS_OB_OP_TEST_JOIN_BASE_H_
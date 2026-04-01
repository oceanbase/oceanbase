/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_BASE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_BASE_H_

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_test/ob_op_test_engine.h"
#include "unittest/sql/engine/op_test/ob_op_test_result.h"
#include "unittest/sql/engine/op_test/ob_op_test_types.h"
#include "unittest/sql/engine/op_test/ob_op_test_file_data.h"
#include "sql/engine/basic/ob_material_vec_op.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "objit/common/ob_item_type.h"
#include <string>
#include <vector>
#include <map>
#include <set>

namespace oceanbase
{
namespace sql
{

namespace
{
// Helper: check if expr is in child_output (by pointer)
inline bool expr_in_child_output(ObExpr *expr, const ExprFixedArray &child_output)
{
  for (int64_t i = 0; i < child_output.count(); ++i) {
    ObExpr *e = child_output.at(i);
    if (OB_NOT_NULL(e) && e == expr) {
      return true;
    }
  }
  return false;
}

// Recursively collect calc exprs: expressions used by operator but not in child output.
// Excludes T_REF_COLUMN (column refs) and expressions already in child_output.
int collect_calc_exprs(ObExpr *expr,
                      const ExprFixedArray &child_output,
                      std::set<ObExpr *> &visited,
                      ExprFixedArray &calc_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    return ret;
  }
  if (visited.count(expr) > 0) {
    return ret;
  }
  visited.insert(expr);

  // Skip if in child output (produced by child)
  if (expr_in_child_output(expr, child_output)) {
    return ret;
  }
  // Skip column references - they come from child
  if (T_REF_COLUMN == expr->type_) {
    return ret;
  }

  // Recurse into children first (dependency order)
  for (uint32_t i = 0; OB_SUCC(ret) && i < expr->arg_cnt_; ++i) {
    if (OB_NOT_NULL(expr->args_) && OB_NOT_NULL(expr->args_[i])) {
      if (OB_FAIL(collect_calc_exprs(expr->args_[i], child_output, visited, calc_exprs))) {
        LOG_WARN("collect calc exprs from child failed", K(ret), K(i));
      }
    }
  }

  // Add this expr to calc_exprs (including constants so their evaluated_ flag gets cleared)
  if (OB_SUCC(ret) && OB_FAIL(calc_exprs.push_back(expr))) {
    LOG_WARN("push calc expr failed", K(ret));
  }
  return ret;
}

// Helper: Fill ObAggrInfo for a simple aggregate function (COUNT, SUM, AVG, MIN, MAX)
// This is a simplified version for basic aggregate testing.
int fill_simple_aggr_info(common::ObIAllocator &alloc,
                          ObAggFunRawExpr &raw_expr,
                          ObExpr &rt_expr,
                          ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;

  // Set the allocator first (this initializes param_exprs_ and other arrays)
  aggr_info.set_allocator(&alloc);

  // Set the expression
  aggr_info.expr_ = &rt_expr;

  // Set aggregate type
  aggr_info.real_aggr_type_ = raw_expr.get_expr_type();

  // Set distinct flag
  aggr_info.has_distinct_ = raw_expr.is_param_distinct();

  // Initialize param_exprs with allocator already set
  if (OB_FAIL(aggr_info.param_exprs_.init(raw_expr.get_real_param_count()))) {
    LOG_WARN("init param_exprs failed", K(ret));
  } else {
    // Add parameter expressions
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr.get_real_param_count(); ++i) {
      ObRawExpr *param_raw = raw_expr.get_real_param_exprs().at(i);
      if (OB_ISNULL(param_raw)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret), K(i));
      } else {
        ObExpr *param_expr = ObStaticEngineExprCG::get_rt_expr(*param_raw);
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get_rt_expr returned null", K(ret), K(i));
        } else {
          if (OB_FAIL(aggr_info.param_exprs_.push_back(param_expr))) {
            LOG_WARN("push param expr failed", K(ret), K(i));
          }
        }
      }
    }
  }

  // Set group_concat_param_count (for GROUP_CONCAT, otherwise same as param count)
  aggr_info.group_concat_param_count_ = raw_expr.get_real_param_count();

  return ret;
}
}  // anonymous namespace

/**
 * @brief OpSpecBuilder - CRTP base class for operator test specifications.
 *
 * Usage pattern:
 *   MaterialTestSpec builder;
 *   builder.table("t", "a int, b int")
 *          .select("a, b")
 *          .with_data({{1, 2}, {3, 4}})
 *          .run();
 *
 * Derived classes can implement:
 *   - create_spec(ObIAllocator &alloc, MockDataSourceSpec *child_spec) - create parent spec
 *   - create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op) - create parent op
 *
 * If create_spec() returns nullptr, MockDataSourceSpec is used as root spec.
 * If create_op() returns nullptr, MockDataSourceOp is used as root op.
 */
template <typename Derived>
class OpSpecBuilder
{
public:
  OpSpecBuilder() : batch_size_(256), rescan_times_(0) {}
  virtual ~OpSpecBuilder() = default;

  // ===== Virtual Interface for Derived Classes =====

  /**
   * @brief Create parent operator spec (e.g., ObLimitVecSpec, ObScalarAggregateVecSpec).
   * Override in derived classes to return the actual operator spec.
   * Default implementation returns nullptr (use MockDataSourceSpec as root).
   * @param alloc Allocator for spec creation
   * @param child_spec The child spec (MockDataSourceSpec)
   * @param output_exprs The real output expressions (SELECT expressions)
   * @param limit_expr Optional LIMIT expression from stmt
   * @param offset_expr Optional OFFSET expression from stmt
   * @param use_rich_format Whether to use rich format (from session setting)
   * @return Pointer to created spec, or nullptr to use child_spec
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    // Default: no parent spec, use MockDataSourceSpec as root
    return nullptr;
  }

  // ===== SQL Description Interface =====

  /**
   * @brief Set table name and column definitions.
   * @param table_name Name of the table
   * @param col_defs Column definitions (e.g., "a int, b varchar(32)")
   */
  Derived& table(const char *table_name, const char *col_defs)
  {
    table_name_ = table_name;
    col_defs_ = col_defs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set SELECT expressions.
   * @param exprs Comma-separated expression list
   */
  Derived& select(const char *exprs)
  {
    select_exprs_ = exprs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set GROUP BY columns.
   * @param exprs Comma-separated column list
   */
  Derived& group_by(const char *exprs)
  {
    group_by_exprs_ = exprs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set ORDER BY columns.
   * @param exprs Comma-separated column list with optional ASC/DESC
   */
  Derived& order_by(const char *exprs)
  {
    order_by_exprs_ = exprs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set WHERE clause condition.
   * @param condition The WHERE condition (e.g., "a > 0 AND b < 10")
   */
  Derived& where(const char *condition)
  {
    where_clause_ = condition;
    return static_cast<Derived&>(*this);
  }

  // ===== Data Injection =====

  /**
   * @brief Set test data for the mock table.
   * @param rows Vector of TestRow data
   */
  Derived& with_data(const std::vector<TestRow> &rows)
  {
    test_data_ = rows;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set test data with move semantics.
   */
  Derived& with_data(std::vector<TestRow> &&rows)
  {
    test_data_ = std::move(rows);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set test data from a CSV file.
   * @param table Table name (must match table() call)
   * @param file_path Path to the CSV file
   * @return Reference to derived class for chaining
   *
   * Note: with_file_data() takes priority over with_data().
   * If both are called, with_file_data() data will be used.
   */
  Derived& with_file_data(const char *table, const std::string &file_path)
  {
    file_data_map_[table] = file_path;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set test data with sorting specification.
   * @param rows Test data (will be sorted in place)
   * @param order_desc Sort order description (e.g., "b DESC", "a ASC, b DESC")
   *                   Uses column names from table definition
   * @return Reference to derived class for chaining
   *
   * Use this when testing operators that expect sorted input (e.g., LIMIT with ORDER BY).
   * The data will be sorted according to order_desc before being output.
   *
   * Example:
   *   .table("t", "a int, b int")
   *   .with_sorted_data({{1, 10}, {2, 30}}, "b DESC")
   */
  Derived& with_sorted_data(const std::vector<TestRow> &rows, const char *order_desc)
  {
    test_data_ = rows;
    is_sorted_data_ = true;
    if (order_desc) {
      sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  Derived& with_sorted_data(std::vector<TestRow> &&rows, const char *order_desc)
  {
    test_data_ = std::move(rows);
    is_sorted_data_ = true;
    if (order_desc) {
      sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  // ===== Configuration =====

  /**
   * @brief Set batch size for vectorization.
   */
  Derived& with_batch_size(int64_t batch_size)
  {
    batch_size_ = batch_size;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set rescan times for operator rescan testing.
   * @param times Number of times to rescan the operator (0 = no rescan)
   */
  Derived& with_rescan_times(int64_t times)
  {
    rescan_times_ = times;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set vector format for variable-length columns.
   * Only applies to variable-length types (varchar, text, etc.).
   * Fixed-length types always use VEC_FIXED.
   * @param fmt Vector format (VEC_DISCRETE or VEC_CONTINUOUS)
   */
  Derived& with_vector_format(VectorFormat fmt)
  {
    vector_format_ = fmt;
    return static_cast<Derived&>(*this);
  }

  // ===== SQL Operator Dump Configuration =====

  /**
   * @brief Enable/disable SQL operator dump for large data scenarios.
   * When enabled, Material operator can spill to disk when memory limit is exceeded.
   * @param enable True to enable dump (default: false)
   */
  Derived& with_sql_operator_dump(bool enable = true)
  {
    enable_sql_operator_dump_ = enable;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set hash area size for work area memory limit (in bytes).
   * This controls the memory threshold before Material operator spills to disk.
   * @param size Hash area size in bytes (default: 1MB, use small values to force dump)
   */
  Derived& with_hash_area_size(int64_t size)
  {
    hash_area_size_ = size;
    return static_cast<Derived&>(*this);
  }

  // ===== Dump Verify Mode Support =====

  /**
   * @brief Set dump verify mode for large data scenarios.
   * - FULL_ROWS: Collect all rows in memory (default, for small data)
   * - CHECKSUM: Calculate checksum instead of storing rows (for large data)
   * - NONE: No result collection (for performance testing)
   * @param mode The verification mode to use
   */
  Derived& with_dump_verify_mode(OpTestEngine::DumpVerifyMode mode)
  {
    dump_verify_mode_ = mode;
    return static_cast<Derived&>(*this);
  }

  // ===== Rescan Memory Check Support =====

  /**
   * @brief Enable/disable rescan memory consistency check.
   * When enabled, memory usage is tracked during rescan and compared with first scan.
   * This helps detect memory leaks in operators that support rescan.
   * @param enable True to enable rescan memory check (default: true in engine)
   */
  Derived& with_rescan_memory_check(bool enable = true)
  {
    rescan_memory_check_ = enable;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set tolerance for rescan memory consistency check (in bytes).
   * Memory usage after rescan should be within first_scan + tolerance.
   * Default (0): auto-calculated as max(first_scan * 10%, 64KB)
   * @param tolerance_bytes Tolerance in bytes
   */
  Derived& with_rescan_memory_tolerance(int64_t tolerance_bytes)
  {
    rescan_memory_tolerance_bytes_ = tolerance_bytes;
    return static_cast<Derived&>(*this);
  }

  // ===== Limit/Offset Support =====

  /**
   * @brief Set LIMIT value.
   * @param n Number of rows to limit
   */
  Derived& limit(int64_t n)
  {
    limit_ = n;
    has_limit_ = true;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set OFFSET value.
   * @param n Number of rows to skip
   */
  Derived& offset(int64_t n)
  {
    offset_ = n;
    has_offset_ = true;
    return static_cast<Derived&>(*this);
  }

  // ===== SQL Mode Support =====

  /**
   * @brief Set SQL compatibility mode (MySQL or Oracle).
   * @param mode MYSQL or ORACLE mode
   */
  Derived& sql_mode(SqlMode mode)
  {
    sql_mode_ = mode;
    return static_cast<Derived&>(*this);
  }

  // ===== Custom Eval Function Override =====

  /**
   * @brief Replace eval_func_ on SELECT output expressions after CG.
   * @param func_ptr The custom eval function (row-by-row evaluation)
   * @return Reference to derived class for chaining
   *
   * Use this to inject custom evaluation logic for expression testing.
   * Applied to all output expressions after code generation.
   */
  Derived& with_expr_eval_func(ObExpr::EvalFunc func_ptr)
  {
    custom_eval_func_ = func_ptr;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Replace eval_vector_func_ on SELECT output expressions after CG.
   * @param func_ptr The custom eval vector function (vectorized evaluation)
   * @return Reference to derived class for chaining
   *
   * Use this to inject custom vectorized evaluation logic for expression testing.
   * Applied to all output expressions after code generation.
   */
  Derived& with_expr_eval_vector_func(ObExpr::EvalVectorFunc func_ptr)
  {
    custom_eval_vector_func_ = func_ptr;
    return static_cast<Derived&>(*this);
  }

  // ===== Execution =====

  /**
   * @brief Build and execute the test.
   * @param engine The OpTestEngine instance
   * @return OpTestResult containing output rows
   */
  OpTestResult run(OpTestEngine &engine)
  {
    OpTestResult result;

    // Step 0: Set SQL mode (must be set before init)
    engine.set_sql_mode(sql_mode_);

    // Step 0.1: Set batch size on engine for vectorized execution
    engine.set_batch_size(batch_size_);

    // Step 0.2: Enable row-by-row eval mode if custom eval_func is set
    // This is needed because eval_batch() uses eval_vector_func_, not eval_func_
    engine.set_use_row_by_row_eval(custom_eval_func_ != nullptr);

    // Step 0.3: Configure SQL operator dump for large data scenarios
    engine.set_enable_sql_operator_dump(enable_sql_operator_dump_);
    engine.set_hash_area_size(hash_area_size_);
    engine.set_dump_verify_mode(dump_verify_mode_);
    engine.set_rescan_memory_check(rescan_memory_check_);
    engine.set_rescan_memory_tolerance(rescan_memory_tolerance_bytes_);

    // Step 1: Register mock table schema
    int ret = engine.register_table(table_name_.c_str(), col_defs_.c_str());
    if (OB_FAIL(ret)) {
      LOG_WARN("register table failed", K(ret));
      return result;
    }

    // Step 2: Build SQL string
    std::string sql = build_sql();

    // Step 3: Resolve SQL
    ObDMLStmt *stmt = nullptr;
    ret = engine.resolve_sql(sql, stmt);
    if (OB_FAIL(ret) || OB_ISNULL(stmt)) {
      LOG_WARN("resolve sql failed", K(ret));
      return result;
    }

    // Step 4: Generate expressions via CG
    ret = engine.generate_exprs(*stmt);
    if (OB_FAIL(ret)) {
      LOG_WARN("generate exprs failed", K(ret));
      return result;
    }

    // Step 5: Create MockDataSourceOp
    // The mock data source should output TABLE COLUMNS, not SELECT expressions.
    // For "SELECT a + b FROM t", mock outputs columns a and b,
    // then SELECT expression "a + b" is evaluated on those columns.
    ObSelectStmt *select_stmt = stmt->is_select_stmt() ?
                                 static_cast<ObSelectStmt *>(stmt) : nullptr;
    if (OB_ISNULL(select_stmt)) {
      LOG_WARN("stmt is not select stmt");
      return result;
    }

    // Get column expressions from the resolved statement
    // These are the actual table columns that mock should output
    const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();

    ExprFixedArray column_exprs(engine.get_phy_plan().get_allocator());
    int expr_ret = column_exprs.init(column_items.count());
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("init column_exprs failed", K(expr_ret));
      return result;
    }
    expr_ret = column_exprs.prepare_allocate(column_items.count());
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("prepare_allocate column_exprs failed", K(expr_ret));
      return result;
    }

    int64_t column_count = 0;
    for (int64_t i = 0; i < column_items.count(); ++i) {
      const ColumnItem &col_item = column_items.at(i);
      if (OB_NOT_NULL(col_item.expr_)) {
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*col_item.expr_);
        if (OB_NOT_NULL(expr)) {
          column_exprs.at(column_count++) = expr;
        } else {
          LOG_WARN("get_rt_expr returned null", K(i));
        }
      }
    }

    // Also collect SELECT expressions for output evaluation
    const common::ObIArray<SelectItem> &select_items = select_stmt->get_select_items();

    ExprFixedArray output_exprs(engine.get_phy_plan().get_allocator());
    expr_ret = output_exprs.init(select_items.count());
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("init output_exprs failed", K(expr_ret));
      return result;
    }
    expr_ret = output_exprs.prepare_allocate(select_items.count());
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("prepare_allocate output_exprs failed", K(expr_ret));
      return result;
    }

    int64_t output_count = 0;
    for (int64_t i = 0; i < select_items.count(); ++i) {
      if (OB_NOT_NULL(select_items.at(i).expr_)) {
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*select_items.at(i).expr_);
        if (OB_NOT_NULL(expr)) {
          output_exprs.at(output_count++) = expr;
        }
      }
    }

    // Step 5.3: Override eval functions if custom ones are specified
    if (custom_eval_func_ != nullptr || custom_eval_vector_func_ != nullptr) {
      for (int64_t i = 0; i < output_count; ++i) {
        ObExpr *expr = output_exprs.at(i);
        if (OB_NOT_NULL(expr)) {
          if (custom_eval_func_ != nullptr) {
            expr->eval_func_ = custom_eval_func_;
            expr->eval_vector_func_ = expr_default_eval_vector_func;
          }
          if (custom_eval_vector_func_ != nullptr) {
            expr->eval_vector_func_ = custom_eval_vector_func_;
          }
        }
      }
    }

    // Collect WHERE condition expressions for filter injection
    // These will be set on the mock_op spec's filters_ to enable filter_rows()
    ExprFixedArray filter_exprs(engine.get_phy_plan().get_allocator());
    const int64_t condition_size = stmt->get_condition_size();
    if (condition_size > 0) {
      expr_ret = filter_exprs.init(condition_size);
      if (OB_FAIL(expr_ret)) {
        LOG_WARN("init filter_exprs failed", K(expr_ret));
        return result;
      }
      expr_ret = filter_exprs.prepare_allocate(condition_size);
      if (OB_FAIL(expr_ret)) {
        LOG_WARN("prepare_allocate filter_exprs failed", K(expr_ret));
        return result;
      }

      int64_t filter_count = 0;
      for (int64_t i = 0; i < condition_size; ++i) {
        ObRawExpr *raw_expr = stmt->get_condition_expr(i);
        if (OB_NOT_NULL(raw_expr)) {
          ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*raw_expr);
          if (OB_NOT_NULL(expr)) {
            filter_exprs.at(filter_count++) = expr;
          }
        }
      }
    }

    // Step 5.4: Create MockDataSourceOp
    // Filter expressions will be set on the tested operator's spec (root_spec) if exists,
    // otherwise on mock_spec. This ensures filter_rows() is called on the operator under test.
    MockDataSourceOp *mock_op = engine.create_mock_data_source(column_exprs, nullptr);
    if (OB_ISNULL(mock_op)) {
      LOG_WARN("create mock data source failed");
      return result;
    }

    // Step 5.5: Set test data
    // Priority: file_data_map_ > test_data_
    std::vector<TestRow> data_to_use;
    auto file_it = file_data_map_.find(table_name_);
    if (file_it != file_data_map_.end()) {
      // Load data from CSV file
      CsvDataReader reader;
      ret = reader.open(file_it->second);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to open csv file", K(file_it->second.c_str()), K(ret));
        return result;
      }
      ret = reader.read_all(data_to_use);
      reader.close();
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to read csv file", K(file_it->second.c_str()), K(ret));
        return result;
      }
    } else {
      // Use inline test data
      data_to_use = test_data_;
    }

    // Apply sorting if with_sorted_data was called
    if (is_sorted_data_ && !sorted_desc_.empty() && !data_to_use.empty()) {
      // Parse column names from col_defs_ (e.g., "a int, b int" -> ["a", "b"])
      std::vector<std::string> col_names;
      std::istringstream col_ss(col_defs_);
      std::string col_token;
      while (std::getline(col_ss, col_token, ',')) {
        // Extract column name (first word before type)
        size_t start = col_token.find_first_not_of(" \t");
        if (start == std::string::npos) continue;
        size_t end = col_token.find_first_of(" \t", start);
        if (end == std::string::npos) {
          end = col_token.length();
        }
        col_names.push_back(col_token.substr(start, end - start));
      }
      // Parse sort spec and apply sorting
      std::vector<SortSpec> specs = parse_sort_spec(sorted_desc_, col_names);
      if (!specs.empty()) {
        sort_rows(data_to_use, specs);
      }
    }

    mock_op->set_test_data(data_to_use);
    mock_op->set_vector_format(vector_format_);

    // Step 6: Get LIMIT/OFFSET expressions from stmt (for real Limit operator)
    ObExpr *limit_expr = nullptr;
    ObExpr *offset_expr = nullptr;
    if (select_stmt->has_limit()) {
      ObRawExpr *limit_raw_expr = stmt->get_limit_expr();
      ObRawExpr *offset_raw_expr = stmt->get_offset_expr();
      if (OB_NOT_NULL(limit_raw_expr)) {
        limit_expr = ObStaticEngineExprCG::get_rt_expr(*limit_raw_expr);
      }
      if (OB_NOT_NULL(offset_raw_expr)) {
        offset_expr = ObStaticEngineExprCG::get_rt_expr(*offset_raw_expr);
      }
    }

    // Step 6.5: Call derived class to create parent spec (e.g., ObLimitVecSpec)
    // The parent spec wraps the MockDataSourceSpec as child
    MockDataSourceSpec *mock_spec = static_cast<MockDataSourceSpec *>(
        const_cast<ObOpSpec *>(&mock_op->get_spec()));
    Derived *derived = static_cast<Derived *>(this);
    bool use_rich_format = engine.get_session_info().use_rich_format();
    ObOpSpec *root_spec = derived->create_spec(engine.get_allocator(), mock_spec,
                                                output_exprs, limit_expr, offset_expr, use_rich_format);

    // Step 6.6: Fill aggr_infos_ if this is a GroupBy spec (for ScalarAggregate)
    if (OB_NOT_NULL(root_spec) &&
        (root_spec->type_ == PHY_SCALAR_AGGREGATE ||
         root_spec->type_ == PHY_HASH_GROUP_BY ||
         root_spec->type_ == PHY_MERGE_GROUP_BY)) {
      ObGroupBySpec *groupby_spec = static_cast<ObGroupBySpec *>(root_spec);
      const common::ObIArray<ObAggFunRawExpr*> &aggr_items = select_stmt->get_aggr_items();

      if (aggr_items.count() > 0) {
        // Initialize aggr_infos_ array
        if (OB_FAIL(groupby_spec->aggr_infos_.prepare_allocate(aggr_items.count()))) {
          LOG_WARN("prepare_allocate aggr_infos failed", K(ret));
        } else {
          // Fill each ObAggrInfo
          for (int64_t i = 0; OB_SUCC(ret) && i < aggr_items.count(); ++i) {
            ObAggFunRawExpr *aggr_raw = aggr_items.at(i);
            if (OB_ISNULL(aggr_raw)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("aggr_raw is null", K(ret), K(i));
            } else {
              ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*aggr_raw);
              if (OB_ISNULL(rt_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get_rt_expr returned null for aggr", K(ret), K(i));
              } else {
                ObAggrInfo &aggr_info = groupby_spec->aggr_infos_.at(i);
                if (OB_FAIL(fill_simple_aggr_info(engine.get_allocator(),
                                                   *aggr_raw, *rt_expr, aggr_info))) {
                  LOG_WARN("fill_simple_aggr_info failed", K(ret), K(i));
                }
              }
            }
          }
        }
      }
    }

    // Step 6.7: Set filter expressions on the tested operator's spec (root_spec) if exists,
    // otherwise on mock_spec. This ensures filter_rows() is called on the operator under test.
    if (condition_size > 0 && filter_exprs.count() > 0) {
      ObOpSpec *target_spec = OB_NOT_NULL(root_spec) ? root_spec : mock_spec;
      if (OB_FAIL(target_spec->filters_.init(filter_exprs.count()))) {
        LOG_WARN("init filters failed", K(ret));
      } else if (OB_FAIL(target_spec->filters_.prepare_allocate(filter_exprs.count()))) {
        LOG_WARN("prepare_allocate filters failed", K(ret));
      } else {
        for (int64_t i = 0; i < filter_exprs.count(); ++i) {
          target_spec->filters_.at(i) = filter_exprs.at(i);
        }
      }
    }

    /// Step 7: Call derived class to create operator
    // If create_spec returned a spec, create_op should use its spec
    // Otherwise, use MockDataSourceOp directly
    ObOperator *op = nullptr;
    if (OB_NOT_NULL(root_spec)) {
      // Parent spec exists, set child and create parent operator
      root_spec->set_child(0, mock_spec);
      mock_spec->set_parent(root_spec);
      op = derived->create_op(engine.get_exec_ctx(), *root_spec, mock_op);
    } else {
      // No parent spec, use mock_op directly
      op = derived->create_op(engine.get_exec_ctx(), mock_op);
    }

    if (OB_ISNULL(op)) {
      LOG_WARN("create operator failed");
      return result;
    }

    // Step 7.5: Compute and set calc_exprs_ on the tested operator's spec.
    // calc_exprs_ are expressions that depend on child output (e.g. a+b in SELECT a+b)
    // and must have their eval_info cleared after each get_next_batch().
    constexpr int64_t MAX_CALC_EXPRS = 128;
    ExprFixedArray calc_exprs(engine.get_phy_plan().get_allocator());
    if (OB_SUCC(ret) && OB_FAIL(calc_exprs.init(MAX_CALC_EXPRS))) {
      LOG_WARN("init calc_exprs failed", K(ret));
    } else if (OB_SUCC(ret)) {
      std::set<ObExpr *> visited;
      for (int64_t i = 0; i < output_count && OB_SUCC(ret); ++i) {
        if (OB_NOT_NULL(output_exprs.at(i))) {
          ret = collect_calc_exprs(output_exprs.at(i), column_exprs, visited, calc_exprs);
        }
      }
      for (int64_t i = 0; i < condition_size && OB_SUCC(ret); ++i) {
        if (OB_NOT_NULL(filter_exprs.at(i))) {
          ret = collect_calc_exprs(filter_exprs.at(i), column_exprs, visited, calc_exprs);
        }
      }
      if (OB_SUCC(ret) && calc_exprs.count() > 0) {
        ObOpSpec &spec = const_cast<ObOpSpec &>(op->get_spec());
        if (OB_FAIL(spec.calc_exprs_.init(calc_exprs.count()))) {
          LOG_WARN("init spec calc_exprs failed", K(ret));
        } else if (OB_FAIL(spec.calc_exprs_.prepare_allocate(calc_exprs.count()))) {
          LOG_WARN("prepare_allocate spec calc_exprs failed", K(ret));
        } else {
          for (int64_t i = 0; i < calc_exprs.count(); ++i) {
            spec.calc_exprs_.at(i) = calc_exprs.at(i);
          }
        }
      }
    }

    // Step 7: Execute
    // Pass output_exprs for SELECT expression evaluation
    // If output_exprs is different from column_exprs (e.g., SELECT a+b FROM t),
    // execute() will evaluate those expressions on the column data output by mock
    result = engine.execute(op, &output_exprs, rescan_times_);

    return result;
  }

protected:
  /**
   * @brief Build the complete SQL SELECT statement.
   */
  std::string build_sql() const
  {
    std::string sql = "SELECT ";

    // SELECT clause
    if (!select_exprs_.empty()) {
      sql += select_exprs_;
    } else {
      sql += "*";
    }

    // FROM clause
    sql += " FROM ";
    sql += table_name_;

    // WHERE clause
    if (!where_clause_.empty()) {
      sql += " WHERE ";
      sql += where_clause_;
    }

    // GROUP BY clause
    if (!group_by_exprs_.empty()) {
      sql += " GROUP BY ";
      sql += group_by_exprs_;
    }

    // ORDER BY clause
    if (!order_by_exprs_.empty()) {
      sql += " ORDER BY ";
      sql += order_by_exprs_;
    }

    // LIMIT clause
    if (has_limit_) {
      sql += " LIMIT ";
      sql += std::to_string(limit_);
    }

    // OFFSET clause
    if (has_offset_) {
      sql += " OFFSET ";
      sql += std::to_string(offset_);
    }

    return sql;
  }

protected:
  // Table definition
  std::string table_name_;
  std::string col_defs_;

  // SQL clauses
  std::string select_exprs_;
  std::string where_clause_;
  std::string group_by_exprs_;
  std::string order_by_exprs_;

  // Test data
  std::vector<TestRow> test_data_;

  // Sorted data info (for documentation purposes)
  bool is_sorted_data_ = false;
  std::string sorted_desc_;

  // File-based test data (table_name -> file_path)
  // Takes priority over test_data_ if set
  std::map<std::string, std::string> file_data_map_;

  // Configuration
  int64_t batch_size_;
  int64_t rescan_times_;
  VectorFormat vector_format_ = VEC_DISCRETE;  // Vector format for variable-length columns

  // Limit/Offset support
  int64_t limit_ = 0;
  int64_t offset_ = 0;
  bool has_limit_ = false;
  bool has_offset_ = false;

  // SQL mode support
  SqlMode sql_mode_ = SqlMode::MYSQL;

  // SQL operator dump configuration
  bool enable_sql_operator_dump_ = false;
  int64_t hash_area_size_ = 1024 * 1024;  // Default 1MB
  OpTestEngine::DumpVerifyMode dump_verify_mode_ = OpTestEngine::DumpVerifyMode::FULL_ROWS;

  // Rescan memory check configuration
  bool rescan_memory_check_ = true;  // Enable rescan memory check by default
  int64_t rescan_memory_tolerance_bytes_ = 0;  // 0 means auto (max(first_scan * 10%, 64KB))

  // Custom eval function overrides (applied to output exprs after CG)
  ObExpr::EvalFunc custom_eval_func_ = nullptr;
  ObExpr::EvalVectorFunc custom_eval_vector_func_ = nullptr;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_BASE_H_
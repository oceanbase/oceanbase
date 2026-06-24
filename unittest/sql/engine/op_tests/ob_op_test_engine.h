/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_ENGINE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_ENGINE_H_

#include <atomic>
#include "unittest/sql/test_sql_utils.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "unittest/sql/engine/op_tests/ob_op_test_types.h"
#include "unittest/sql/engine/op_tests/ob_op_test_result.h"
#include "unittest/sql/engine/op_tests/ob_op_test_data_source.h"
#include "share/config/ob_server_config.h"  // for GCONF
#include "observer/omt/ob_tenant_config_mgr.h"  // for ObTenantConfigGuard
#include "lib/alloc/alloc_func.h"  // for get_tenant_memory_hold

namespace oceanbase
{
namespace sql
{

class ObDMLStmt;
class ObRawExpr;

/**
 * @brief SQL compatibility mode for testing
 */
enum class SqlMode {
  MYSQL,
  ORACLE
};

// Default batch size for vectorized execution
constexpr int64_t DEFAULT_BATCH_SIZE = 256;

/**
 * @brief OpTestEngine extends TestSqlUtils with expression CG and execution capabilities.
 *
 * Key features:
 * - Inherits schema registration and SQL resolve from TestSqlUtils
 * - Adds expression code generation (ObStaticEngineExprCG)
 * - Provides execute() to run operators and collect results
 */
class OpTestEngine : public test::TestSqlUtils
{
public:
  OpTestEngine();
  virtual ~OpTestEngine();

  // ===== Configuration =====
  /**
   * @brief Set SQL compatibility mode. Must be called before init().
   * @param mode MYSQL or ORACLE mode
   */
  void set_sql_mode(SqlMode mode);
  SqlMode get_sql_mode() const { return sql_mode_; }
  void apply_sql_mode();

  /**
   * @brief Set batch size for vectorized execution. Must be called before generate_exprs().
   * @param batch_size Number of rows per batch (default: 256)
   */
  void set_batch_size(int64_t batch_size) { batch_size_ = batch_size; }
  int64_t get_batch_size() const { return batch_size_; }

  /**
   * @brief Enable row-by-row evaluation mode for custom eval_func testing.
   * When true, collect_batch_results() uses eval() instead of eval_batch().
   * @param enable True to enable row-by-row evaluation
   */
  void set_use_row_by_row_eval(bool enable) { use_row_by_row_eval_ = enable; }
  bool get_use_row_by_row_eval() const { return use_row_by_row_eval_; }

  // ===== SQL Operator Dump Configuration =====
  /**
   * @brief Enable/disable SQL operator dump for large data scenarios.
   * When enabled, Material operator can spill to disk when memory limit is exceeded.
   * @param enable True to enable dump
   */
  void set_enable_sql_operator_dump(bool enable) { enable_sql_operator_dump_ = enable; }
  bool get_enable_sql_operator_dump() const { return enable_sql_operator_dump_; }

  // ===== Dump Verification Mode =====
  /**
   * @brief Enum for dump verification mode.
   * - FULL_ROWS: Collect all rows in memory (default, for small data)
   * - CHECKSUM: Calculate checksum instead of storing rows (for large data)
   * - NONE: No result collection, just run the operator (for performance testing)
   */
  enum class DumpVerifyMode {
    FULL_ROWS,   // Collect all rows
    CHECKSUM,    // Calculate checksum only
    NONE         // No result collection
  };

  void set_dump_verify_mode(DumpVerifyMode mode) { dump_verify_mode_ = mode; }
  DumpVerifyMode get_dump_verify_mode() const { return dump_verify_mode_; }

  // ===== Rescan Memory Check Configuration =====

  /**
   * @brief Enable/disable rescan memory consistency check.
   * When enabled, memory usage is tracked during rescan and compared with first scan.
   * @param enable True to enable rescan memory check (default: true)
   */
  void set_rescan_memory_check(bool enable) { rescan_memory_check_ = enable; }
  bool get_rescan_memory_check() const { return rescan_memory_check_; }

  /**
   * @brief Set tolerance for rescan memory consistency check (in bytes).
   * Memory usage after rescan should be within first_scan + tolerance.
   * Default: max(first_scan * 10%, 64KB)
   * @param tolerance_bytes Tolerance in bytes
   */
  void set_rescan_memory_tolerance(int64_t tolerance_bytes) { rescan_memory_tolerance_bytes_ = tolerance_bytes; }
  int64_t get_rescan_memory_tolerance() const { return rescan_memory_tolerance_bytes_; }

  /**
   * @brief Enable/disable rich format for vectorized execution.
   * Must be called AFTER prepare() (expression CG) to ensure CG runs in FORCE_ON mode.
   * @param enable True for FORCE_ON (2.0 vec), false for FORCE_OFF (1.0)
   */
  void enable_rich_format(bool enable)
  {
    session_info_.set_force_rich_format(
        enable ? ObBasicSessionInfo::ForceRichFormatStatus::FORCE_ON
               : ObBasicSessionInfo::ForceRichFormatStatus::FORCE_OFF);
  }

  // ===== Lifecycle =====
  virtual void init() override;
  virtual void destroy() override;

  // ===== Tenant Config Override =====
  /**
   * @brief Apply tenant config overrides from TestDefaultParameterConf TLS.
   * Called by OpSpecBuilder::prepare() after engine init and before register_table.
   */
  void apply_tenant_config_overrides();

  // ===== Session Variable Override =====
  /**
   * @brief Apply session variable overrides from TestSessionVarConf TLS.
   * Called by OpSpecBuilder::prepare() after engine init and before register_table.
   * Uses session_info_.update_sys_variable() to set each variable.
   */
  void apply_session_variable_overrides();

  // ===== Schema Registration =====
  /**
   * @brief Register a mock table for testing.
   * @param table_name Name of the table
   * @param col_defs Column definitions (e.g., "a int, b varchar(32)")
   * @return OB_SUCCESS on success
   */
  int register_table(const char *table_name, const char *col_defs);

  // ===== SQL Resolve =====
  /**
   * @brief Resolve SQL to get ObDMLStmt with typed expressions.
   * @param sql The SQL statement
   * @param stmt Output: resolved statement
   * @param expect_error Expected error code (OB_SUCCESS for success, or error code like -OB_ERR_TABLE_NOT_EXIST)
   * @return OB_SUCCESS on success
   */
  int resolve_sql(const std::string &sql, ObDMLStmt *&stmt, int expect_error = OB_SUCCESS);

  // ===== Expression CG =====
  /**
   * @brief Generate ObExpr from ObRawExpr.
   * @param stmt The resolved DML statement
   * @return OB_SUCCESS on success
   */
  int generate_exprs(ObDMLStmt &stmt);

  // ===== Execution =====
  /**
   * @brief Execute an operator and collect results.
   * @param op The operator to execute
   * @param output_exprs Optional SELECT expressions to evaluate on output.
   *                     If null, uses operator's output expressions.
   * @param rescan_times Number of times to rescan the operator (default: 0).
   *                     When > 0, verifies that each rescan produces the same results.
   * @return OpTestResult containing output rows
   */
  OpTestResult execute(ObOperator *op, const ExprFixedArray *output_exprs = nullptr,
                       int64_t rescan_times = 0);

  // ===== Accessors =====
  ObPhysicalPlan &get_phy_plan() { return phy_plan_; }
  ObExecContext &get_exec_ctx() { return exec_ctx_; }
  ObExprFrameInfo &get_expr_frame_info() { return phy_plan_.get_expr_frame_info(); }

  // ===== Mock Data Source Creation =====
  /**
   * @brief Create a MockDataSourceSpec with given output expressions.
   * @param output_exprs The column expressions to output
   * @param filter_exprs Optional filter expressions for WHERE clause filtering
   * @return Pointer to the created spec
   */
  MockDataSourceSpec *create_mock_data_source_spec(const ExprFixedArray &output_exprs,
                                                    const ExprFixedArray *filter_exprs = nullptr);

  /**
   * @brief Create a MockDataSourceOp with given spec.
   * @param spec The spec to use
   * @return Pointer to the created operator
   */
  MockDataSourceOp *create_mock_data_source_op(MockDataSourceSpec &spec);

  // Deprecated: Use create_mock_data_source_spec + create_mock_data_source_op instead
  MockDataSourceOp *create_mock_data_source(const ExprFixedArray &output_exprs,
                                             const ExprFixedArray *filter_exprs = nullptr);

  // ===== Allocator Access =====
  common::ObIAllocator &get_allocator() { return allocator_; }

  // ===== Session Access =====
  ObSQLSessionInfo &get_session_info() { return session_info_; }

  // ===== Expression Factory Access =====
  ObRawExprFactory &get_expr_factory() { return expr_factory_; }

  // ===== Pending Raw Exprs (for pseudo columns created outside SQL resolution) =====
  void add_pending_raw_expr(ObRawExpr *expr) {
    if (OB_NOT_NULL(expr)) {
      pending_raw_exprs_.push_back(expr);
    }
  }

  // ===== Performance Recording =====
  /**
   * @brief Enable/disable performance recording.
   * When enabled, execute() measures timing and fills PerfStats in OpTestResult.
   * @param enable True to enable
   */
  void set_perf_record(bool enable) { perf_record_enabled_ = enable; }
  bool get_perf_record() const { return perf_record_enabled_; }

  /**
   * @brief Get the mock timing accumulator pointer.
   * Used by MockDataSourceOp to accumulate its inner_get_next_batch timing.
   */
  std::atomic<int64_t> *get_mock_perf_acc() { return &mock_acc_ns_; }

private:
  /**
   * @brief Collect top-level raw expressions from statement into an array.
   * Does not recursively collect child expressions.
   */
  int collect_raw_exprs(ObDMLStmt &stmt,
                        common::ObIArray<ObRawExpr *> &exprs);

  /**
   * @brief Collect batch results from an opened operator.
   * Called internally by execute() - the operator should already be opened.
   * @param op The operator to collect from (must already be opened)
   * @param output_exprs Optional SELECT expressions to evaluate on output
   * @return OpTestResult containing output rows
   */
  OpTestResult collect_batch_results(ObOperator *op, const ExprFixedArray *output_exprs);

  /**
   * @brief Get current tenant memory hold in bytes.
   * Uses lib::get_tenant_memory_hold() to get memory usage.
   * @return Memory usage in bytes, or 0 if tenant ID is invalid
   */
  int64_t get_memory_hold_bytes() const;

private:
  bool inited_;
  SqlMode sql_mode_ = SqlMode::MYSQL;
  int64_t batch_size_ = DEFAULT_BATCH_SIZE;
  bool use_row_by_row_eval_ = false;  // For custom eval_func testing
  bool enable_sql_operator_dump_ = false;  // SQL operator dump flag
  bool saved_gconf_dump_enabled_ = false;  // Saved GCONF state for restore
  DumpVerifyMode dump_verify_mode_ = DumpVerifyMode::FULL_ROWS;  // Dump verification mode
  ObPhysicalPlan phy_plan_;
  ObPhysicalPlanCtx *phy_plan_ctx_;  // Physical plan context for operators that need it
  common::ObArenaAllocator allocator_;

  // Rescan memory check configuration
  bool rescan_memory_check_ = true;  // Enable rescan memory check by default
  int64_t rescan_memory_tolerance_bytes_ = 0;  // 0 means auto (max(first_scan * 10%, 64KB))

  // Pending raw exprs: pseudo columns (e.g., grouping_id) created outside SQL resolution
  // Added to CG raw_exprs in generate_exprs(), cleared after use
  std::vector<ObRawExpr *> pending_raw_exprs_;

  // Performance recording
  bool perf_record_enabled_ = false;
  std::atomic<int64_t> mock_acc_ns_{0};  // Accumulator for MockDataSource timing (ns)

  DISALLOW_COPY_AND_ASSIGN(OpTestEngine);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_ENGINE_H_

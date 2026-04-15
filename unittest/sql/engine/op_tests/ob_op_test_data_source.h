/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_DATA_SOURCE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_DATA_SOURCE_H_

#include "sql/engine/ob_operator.h"
#include "unittest/sql/engine/op_tests/ob_op_test_types.h"
#include <functional>
#include <fstream>

namespace oceanbase
{
namespace sql
{

/**
 * @brief Data source mode for MockDataSourceOp.
 * - ARRAY: Traditional array-based data (default, stores all rows in memory)
 * - GENERATOR: Generator function mode (rows generated on-demand)
 * - STREAM_CSV: Stream CSV file mode (rows read from file on-demand)
 */
enum class MockDataSourceMode {
  ARRAY,       // Default: store all rows in memory
  GENERATOR,   // Generate rows on-demand using callback
  STREAM_CSV   // Stream rows from CSV file
};

/**
 * @brief Generator function type for GENERATOR mode.
 * @param row_idx The row index to generate (0-based)
 * @return TestRow for the given index, or empty TestRow to signal end
 */
using RowGenerator = std::function<TestRow(int64_t row_idx)>;

/**
 * @brief MockDataSourceSpec - Minimal spec for mock data source.
 */
class MockDataSourceSpec : public ObOpSpec
{
public:
  MockDataSourceSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type)
  {}
  ~MockDataSourceSpec() = default;
};

/**
 * @brief MockDataSourceOp - Mock data source operator.
 * Replaces TableScan for unit testing, produces deterministic test data.
 *
 * Design:
 * - Takes TestRow data from user (ARRAY mode, default)
 * - Supports GENERATOR mode for large data without memory overhead
 * - Supports STREAM_CSV mode for file-based large data
 * - Supports int, double, varchar, number types
 */
class MockDataSourceOp : public ObOperator
{
public:
  MockDataSourceOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      cur_idx_(0),
      mock_allocator_(nullptr),
      data_source_mode_(MockDataSourceMode::ARRAY),
      row_generator_(nullptr),
      generator_total_rows_(0),
      csv_file_path_(""),
      csv_file_(nullptr),
      csv_current_line_(0)
  {}
  ~MockDataSourceOp() = default;

  // ===== ARRAY mode (default) =====

  /**
   * @brief Set test data to be produced.
   * @param rows Vector of TestRow data
   */
  void set_test_data(const std::vector<TestRow> &rows) { rows_ = rows; }
  void set_test_data(std::vector<TestRow> &&rows) { rows_ = std::move(rows); }

  // ===== GENERATOR mode =====

  /**
   * @brief Set generator function for GENERATOR mode.
   * @param generator Function that generates TestRow for each index
   * @param total_rows Total number of rows the generator will produce
   */
  void set_row_generator(RowGenerator generator, int64_t total_rows)
  {
    data_source_mode_ = MockDataSourceMode::GENERATOR;
    row_generator_ = std::move(generator);
    generator_total_rows_ = total_rows;
  }

  // ===== STREAM_CSV mode =====

  /**
   * @brief Set CSV file for STREAM_CSV mode.
   * @param file_path Path to CSV file
   * @param has_header Whether CSV has header row (will be skipped)
   */
  void set_csv_file(const std::string &file_path, bool has_header = true)
  {
    data_source_mode_ = MockDataSourceMode::STREAM_CSV;
    csv_file_path_ = file_path;
    csv_has_header_ = has_header;
  }

  // ===== Mode query =====

  /**
   * @brief Get current data source mode.
   */
  MockDataSourceMode get_data_source_mode() const { return data_source_mode_; }

  /**
   * @brief Set mock side allocator for isolation.
   * Mock internal buffer uses this allocator, NOT counted as material tenant memory.
   * @param alloc The allocator to use for mock data (should be separate from tenant memory)
   */
  void set_mock_side_allocator(common::ObIAllocator *alloc) { mock_allocator_ = alloc; }

  int inner_open() override
  {
    int ret = OB_SUCCESS;
    cur_idx_ = 0;
    output_cnt_ = 0;

    // Mode-specific initialization
    if (data_source_mode_ == MockDataSourceMode::STREAM_CSV && !csv_file_path_.empty()) {
      csv_file_ = new std::ifstream(csv_file_path_);
      csv_current_line_ = 0;
      // Skip header if needed
      if (csv_has_header_ && csv_file_ && csv_file_->is_open()) {
        std::string line;
        std::getline(*csv_file_, line);
        csv_current_line_++;
      }
    }

    // Sort rows using real SQL expression eval if sort key specs are set
    if (OB_SUCC(ret) && !sort_key_specs_.empty() && data_source_mode_ == MockDataSourceMode::ARRAY) {
      if (OB_FAIL(sort_data_by_exprs())) {
        // sort failed, return error
      } else {
        sort_key_specs_.clear();  // sort once only
      }
    }

    // Apply offset: skip first offset_ rows
    if (OB_SUCC(ret) && has_offset_ && offset_ > 0 && data_source_mode_ == MockDataSourceMode::ARRAY) {
      cur_idx_ = std::min(offset_, static_cast<int64_t>(rows_.size()));
    }
    return ret;
  }

  int inner_get_next_row() override
  {
    // Not used in batch mode
    return OB_NOT_IMPLEMENT;
  }

  int inner_get_next_batch(const int64_t max_row_cnt) override;

  int inner_close() override
  {
    // Clean up CSV file if opened
    if (csv_file_ != nullptr) {
      csv_file_->close();
      delete csv_file_;
      csv_file_ = nullptr;
    }
    return OB_SUCCESS;
  }

  int inner_rescan() override
  {
    cur_idx_ = 0;
    output_cnt_ = 0;

    // Mode-specific rescan handling
    if (data_source_mode_ == MockDataSourceMode::STREAM_CSV) {
      // Reopen CSV file for rescan
      if (csv_file_ != nullptr) {
        csv_file_->close();
        delete csv_file_;
        csv_file_ = nullptr;
      }
      if (!csv_file_path_.empty()) {
        csv_file_ = new std::ifstream(csv_file_path_);
        csv_current_line_ = 0;
        // Skip header if needed
        if (csv_has_header_ && csv_file_ && csv_file_->is_open()) {
          std::string line;
          std::getline(*csv_file_, line);
          csv_current_line_++;
        }
      }
    }

    // Apply offset on rescan too
    if (has_offset_ && offset_ > 0 && data_source_mode_ == MockDataSourceMode::ARRAY) {
      cur_idx_ = std::min(offset_, static_cast<int64_t>(rows_.size()));
    }
    return ObOperator::inner_rescan();
  }

  void destroy() override
  {
    rows_.clear();
    cur_idx_ = 0;
    row_generator_ = nullptr;

    // Clean up CSV file
    if (csv_file_ != nullptr) {
      csv_file_->close();
      delete csv_file_;
      csv_file_ = nullptr;
    }

    ObOperator::destroy();
  }

  /**
   * @brief Set vector format for string columns.
   * Only applies to variable-length types (varchar, text, etc.).
   * Fixed-length types always use VEC_FIXED.
   * @param fmt Vector format (VEC_DISCRETE or VEC_CONTINUOUS)
   */
  void set_vector_format(VectorFormat fmt) { vector_format_ = fmt; }

  /**
   * @brief Set sort key specs for pre-execution sorting via real SQL expression eval.
   * Called by ob_op_test_base.h after generate_exprs() has resolved ObExpr* pointers.
   * Sorting happens in inner_open() once eval_ctx_ is available.
   */
  void set_sort_key_specs(const std::vector<SortKeySpec> &specs)
  {
    sort_key_specs_ = specs;
  }

  /**
   * @brief Set LIMIT value. Only this many rows will be output.
   * @param limit Maximum rows to output (-1 means no limit)
   */
  void set_limit(int64_t limit) { limit_ = limit; has_limit_ = true; }

  /**
   * @brief Set OFFSET value. Skip this many rows before output.
   * @param offset Number of rows to skip
   */
  void set_offset(int64_t offset) { offset_ = offset; has_offset_ = true; }

private:
  /**
   * @brief Sort rows_ using sort_key_specs_ by evaluating ObExpr* in row mode.
   * Called from inner_open() after eval_ctx_ is available.
   */
  int sort_data_by_exprs();

  /**
   * @brief Fill a single value directly into frame buffers.
   */
  int fill_value_directly(ObExpr *expr, int64_t row_idx, const TestValue &value,
                          VectorFormat fmt);

  /**
   * @brief Parse a CSV line into TestRow.
   * @param line The CSV line to parse
   * @return TestRow containing parsed values
   */
  TestRow parse_csv_line(const std::string &line);

  std::vector<TestRow> rows_;
  int64_t cur_idx_;
  VectorFormat vector_format_ = VEC_DISCRETE;  // 默认 discrete

  // LIMIT/OFFSET support
  int64_t limit_ = -1;
  int64_t offset_ = 0;
  bool has_limit_ = false;
  bool has_offset_ = false;
  int64_t output_cnt_ = 0;  // Number of rows output (for LIMIT)

  // Mock allocator for isolation - not counted as material tenant memory
  common::ObIAllocator *mock_allocator_;

  // ===== GENERATOR mode members =====
  MockDataSourceMode data_source_mode_ = MockDataSourceMode::ARRAY;
  RowGenerator row_generator_;
  int64_t generator_total_rows_ = 0;

  // ===== STREAM_CSV mode members =====
  std::string csv_file_path_;
  std::ifstream *csv_file_;
  bool csv_has_header_ = false;
  int64_t csv_current_line_ = 0;

  // ===== Sort key specs (for with_sorted_data + arbitrary SQL expressions) =====
  std::vector<SortKeySpec> sort_key_specs_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_DATA_SOURCE_H_
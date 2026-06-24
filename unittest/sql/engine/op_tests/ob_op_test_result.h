/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_RESULT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_RESULT_H_

#include <algorithm>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include "unittest/sql/engine/op_tests/ob_op_test_types.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief RescanMemoryInfo tracks memory usage during rescan operations.
 * Used to detect memory leaks after rescan.
 */
struct RescanMemoryInfo
{
  int64_t memory_after_first_scan_;      // Memory usage after first scan (bytes)
  int64_t max_memory_after_rescan_;      // Maximum memory usage during rescans (bytes)
  int64_t min_memory_after_rescan_;      // Minimum memory usage during rescans (bytes)
  int64_t rescan_count_;                  // Number of rescan operations performed
  bool memory_consistent_;                // True if all rescan memory within tolerance

  RescanMemoryInfo()
    : memory_after_first_scan_(0),
      max_memory_after_rescan_(0),
      min_memory_after_rescan_(0),
      rescan_count_(0),
      memory_consistent_(true)
  {}

  void reset()
  {
    memory_after_first_scan_ = 0;
    max_memory_after_rescan_ = 0;
    min_memory_after_rescan_ = 0;
    rescan_count_ = 0;
    memory_consistent_ = true;
  }
};

/**
 * @brief OpTestResult collects and verifies operator execution results.
 *
 * Design:
 * - Stores rows as strings for easy comparison
 * - Provides ordered/unordered verification
 * - Outputs readable diff on failure
 */
class OpTestResult
{
public:
  OpTestResult() = default;
  ~OpTestResult() = default;

  // Disallow copy, allow move
  OpTestResult(const OpTestResult &) = delete;
  OpTestResult &operator=(const OpTestResult &) = delete;
  OpTestResult(OpTestResult &&) = default;
  OpTestResult &operator=(OpTestResult &&) = default;

  // ===== Row collection =====

  /**
   * @brief Add a row to the result.
   * @param row The row to add (as vector of string values)
   */
  void add_row(const std::vector<std::string> &row)
  {
    rows_.push_back(row);
  }

  /**
   * @brief Add a row from TestRow.
   * @param row The TestRow to add
   */
  void add_row(const TestRow &row)
  {
    std::vector<std::string> str_row;
    str_row.reserve(row.size());
    for (const auto &val : row) {
      str_row.push_back(val.to_string());
    }
    rows_.push_back(std::move(str_row));
  }

  // ===== Query methods =====

  /**
   * @brief Get the number of rows in the result.
   */
  int64_t row_count() const { return static_cast<int64_t>(rows_.size()); }

  // ===== Batch count =====
  void set_batch_count(int64_t count) { batch_count_ = count; }
  int64_t get_batch_count() const { return batch_count_; }

  void set_ret_code(int ret_code) { ret_code_ = ret_code; }
  int get_ret_code() const { return ret_code_; }

  /**
   * @brief Check if the result is empty.
   */
  bool empty() const { return rows_.empty(); }

  /**
   * @brief Get a specific row.
   * @param idx Row index
   * @return The row as vector of strings
   */
  const std::vector<std::string> &get_row(int64_t idx) const
  {
    return rows_.at(idx);
  }

  const std::vector<std::vector<std::string>> &get_rows() const
  {
    return rows_;
  }

  const std::string get_row_string(int64_t idx) const
  {
    std::string result = "{";
    const std::vector<std::string> &row = get_row(idx);
    for (int i = 0; i < row.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += row[i];
    }
    result += "}";
    return result;
  }

  // ===== Verification methods =====

  /**
   * @brief Verify rows in exact order.
   * @param expected Expected rows
   * @return true if all rows match in order, false otherwise
   */
  bool verify_ordered(const std::vector<TestRow> &expected)
  {
    std::vector<std::vector<std::string>> expected_strs;
    expected_strs.reserve(expected.size());
    for (const auto &row : expected) {
      expected_strs.push_back(row_to_strings(row));
    }
    return verify_ordered_internal(expected_strs);
  }

  /**
   * @brief Verify rows in exact order using per-column generators.
   * Generates expected rows from generators and delegates to verify_ordered(vector<TestRow>).
   *
   * Example:
   *   result.verify_ordered(1000, gen::sequential(1), gen::sequential(10, 10));
   */
  template <typename... Generators>
  bool verify_ordered(int64_t row_num, Generators&&... generators)
  {
    std::vector<ColumnGenerator> col_gens = {ColumnGenerator(std::forward<Generators>(generators))...};
    std::vector<TestRow> expected;
    expected.reserve(row_num);
    for (int64_t i = 0; i < row_num; ++i) {
      std::vector<TestValue> values;
      values.reserve(col_gens.size());
      for (auto &g : col_gens) {
        values.push_back(g(i));
      }
      expected.emplace_back(std::move(values));
    }
    return verify_ordered(expected);
  }

  /**
   * @brief Verify a specific column across all rows.
   * @param col_idx Column index
   * @param expected Expected values for this column
   * @return true if column values match, false otherwise
   */
  bool verify_column(int64_t col_idx, const std::vector<TestValue> &expected)
  {
    // Unit tests for verify_column are in test_material_op.cpp
    if (row_count() != static_cast<int64_t>(expected.size())) {
      std::cerr << "verify_column failed: row count mismatch. "
                << "Expected " << expected.size() << " rows, got " << row_count() << std::endl;
      return false;
    }

    for (int64_t i = 0; i < row_count(); ++i) {
      const std::string &actual = rows_[i].at(col_idx);
      std::string exp_str = expected[i].to_string();
      if (actual != exp_str) {
        std::cerr << "verify_column failed at row " << i << ", column " << col_idx << ". "
                  << "Expected: " << exp_str << ", Got: " << actual << std::endl;
        return false;
      }
    }
    return true;
  }

  /**
   * @brief Verify a specific column using a generator.
   * @param col_idx Column index
   * @param row_num Number of expected rows
   * @param generator ColumnGenerator to produce expected values
   */
  bool verify_column(int64_t col_idx, int64_t row_num, ColumnGenerator generator)
  {
    std::vector<TestValue> expected;
    expected.reserve(row_num);
    for (int64_t i = 0; i < row_num; ++i) {
      expected.push_back(generator(i));
    }
    return verify_column(col_idx, expected);
  }

  /**
   * @brief Verify rows ignoring order (sort both actual and expected, then compare).
   * Suitable for HashGroupBy and other operators with non-deterministic output order.
   */
  bool verify_unordered(const std::vector<TestRow> &expected)
  {
    std::vector<std::vector<std::string>> expected_strs;
    expected_strs.reserve(expected.size());
    for (const auto &row : expected) {
      expected_strs.push_back(row_to_strings(row));
    }
    return verify_unordered_internal(expected_strs);
  }

  /**
   * @brief Verify rows ignoring order using per-column generators.
   * Generates expected rows, then delegates to verify_unordered(vector<TestRow>).
   *
   * Example: result.verify_unordered(1000, gen_a, gen_count, gen_sum);
   */
  template <typename... Generators>
  bool verify_unordered(int64_t row_num, Generators&&... generators)
  {
    std::vector<ColumnGenerator> col_gens = {ColumnGenerator(std::forward<Generators>(generators))...};
    std::vector<TestRow> expected;
    expected.reserve(row_num);
    for (int64_t i = 0; i < row_num; ++i) {
      std::vector<TestValue> values;
      values.reserve(col_gens.size());
      for (auto &g : col_gens) {
        values.push_back(g(i));
      }
      expected.emplace_back(std::move(values));
    }
    return verify_unordered(expected);
  }

  // ===== Debug output =====

  /**
   * @brief Print all rows to stdout.
   */
  void print() const
  {
    std::cout << "OpTestResult (" << row_count() << " rows):" << std::endl;
    for (int64_t i = 0; i < row_count(); ++i) {
      std::cout << "  [" << i << "] ";
      print_row(rows_[i]);
      std::cout << std::endl;
    }
  }

  /**
   * @brief Convert result to string representation.
   */
  std::string to_string() const
  {
    std::stringstream ss;
    ss << "OpTestResult(" << row_count() << " rows): [";
    for (int64_t i = 0; i < row_count(); ++i) {
      if (i > 0) ss << ", ";
      ss << "[";
      for (size_t j = 0; j < rows_[i].size(); ++j) {
        if (j > 0) ss << ", ";
        ss << rows_[i][j];
      }
      ss << "]";
    }
    ss << "]";
    return ss.str();
  }

  /**
   * @brief Compare two results for equality.
   * @param other The result to compare with
   * @return true if both results have identical rows in the same order
   */
  bool equals(const OpTestResult &other) const
  {
    if (row_count() != other.row_count()) {
      return false;
    }
    for (int64_t i = 0; i < row_count(); ++i) {
      if (rows_[i] != other.rows_[i]) {
        return false;
      }
    }
    return true;
  }

  // ===== Checksum Support for Large Data =====

  /**
   * @brief Compute a simple checksum from the result rows.
   * Used for verifying large data results without storing all rows in memory.
   * @return A 64-bit checksum value
   */
  uint64_t compute_checksum() const
  {
    uint64_t checksum = 0;
    for (const auto &row : rows_) {
      for (const auto &cell : row) {
        // Simple hash combining - not cryptographic, just for verification
        uint64_t h = 0;
        for (char c : cell) {
          h = h * 31 + static_cast<uint64_t>(c);
        }
        checksum ^= h + 0x9e3779b9 + (checksum << 6) + (checksum >> 2);
      }
    }
    return checksum;
  }

  /**
   * @brief Get row count (works for both FULL_ROWS and CHECKSUM modes).
   */
  int64_t get_row_count() const { return static_cast<int64_t>(rows_.size()); }

  // ===== CHECKSUM Mode Support =====

  /**
   * @brief Set checksum info (used by CHECKSUM mode in engine).
   * @param checksum The computed checksum value
   * @param row_count Total row count
   */
  void set_checksum_info(uint64_t checksum, int64_t row_count)
  {
    checksum_ = checksum;
    checksum_row_count_ = row_count;
  }

  /**
   * @brief Get the computed checksum (for CHECKSUM mode).
   */
  uint64_t get_checksum() const { return checksum_; }

  /**
   * @brief Get the row count stored during CHECKSUM mode.
   * This is different from row_count() which returns rows_.size().
   * For CHECKSUM mode, rows_ is empty but checksum_row_count_ holds the count.
   */
  int64_t get_checksum_row_count() const { return checksum_row_count_; }

  // ===== Rescan Memory Info Support =====

  /**
   * @brief Set rescan memory info (used by engine during rescan).
   * @param info The rescan memory info to set
   */
  void set_rescan_memory_info(const RescanMemoryInfo &info)
  {
    rescan_memory_info_ = info;
  }

  /**
   * @brief Get the rescan memory info.
   */
  const RescanMemoryInfo &get_rescan_memory_info() const { return rescan_memory_info_; }

  /**
   * @brief Check if rescan memory is consistent (no leak detected).
   */
  bool is_rescan_memory_consistent() const { return rescan_memory_info_.memory_consistent_; }

  /**
   * @brief Get memory usage after first scan.
   */
  int64_t get_memory_after_first_scan() const { return rescan_memory_info_.memory_after_first_scan_; }

  /**
   * @brief Get the number of rescan operations performed.
   */
  int64_t get_rescan_count() const { return rescan_memory_info_.rescan_count_; }

  // ===== Performance Recording Support =====

  struct PerfStats {
    int64_t op_total_ns = 0;     // 父算子 open+get_next_batch+close 总耗时 (ns)
    int64_t mock_total_ns = 0;   // 所有 MockDataSource::inner_get_next_batch 累加耗时 (ns)
    int64_t batch_count = 0;     // get_next_batch 调用次数
    int64_t row_count = 0;       // 总行数
    int64_t operator_rt_ns() const { return op_total_ns - mock_total_ns; }
    double operator_rt_us() const { return static_cast<double>(operator_rt_ns()) / 1000.0; }
    double operator_rt_ms() const { return static_cast<double>(operator_rt_ns()) / 1000000.0; }
  };

  void set_perf_stats(const PerfStats &s) { perf_stats_ = s; }
  const PerfStats &get_perf_stats() const { return perf_stats_; }
  bool has_perf_stats() const { return perf_stats_.op_total_ns > 0; }

private:
  std::vector<std::vector<std::string>> rows_;
  int64_t batch_count_ = 0;  // Number of get_next_batch calls
  int ret_code_ = 0;

  // For CHECKSUM mode - store checksum instead of all rows
  uint64_t checksum_ = 0;
  int64_t checksum_row_count_ = 0;

  // For rescan memory tracking
  RescanMemoryInfo rescan_memory_info_;

  // For performance recording
  PerfStats perf_stats_;

  // Convert TestRow to vector of strings
  std::vector<std::string> row_to_strings(const TestRow &row) const
  {
    std::vector<std::string> result;
    result.reserve(row.size());
    for (const auto &val : row) {
      result.push_back(val.to_string());
    }
    return result;
  }

  // Print a single row
  void print_row(const std::vector<std::string> &row) const
  {
    std::cout << "[";
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) std::cout << ", ";
      std::cout << row[i];
    }
    std::cout << "]";
  }

  /**
   * @brief Normalize a decimal string for logical value comparison.
   * Strips trailing zeros after decimal point, and removes the decimal point if no fractional part.
   * Examples: "4.000" → "4", "123.450" → "123.45", "0.10" → "0.1", "100" → "100"
   * For non-decimal strings (no decimal point), returns as-is.
   */
  static std::string normalize_decimal_str(const std::string &s)
  {
    // If no decimal point, return as-is (non-decimal or already normalized)
    if (s.find('.') == std::string::npos) {
      return s;
    }
    std::string result = s;
    // Strip trailing zeros
    size_t last_nonzero = result.find_last_not_of('0');
    if (last_nonzero != std::string::npos) {
      result.erase(last_nonzero + 1);
    }
    // If decimal point is the last character, remove it
    if (!result.empty() && result.back() == '.') {
      result.pop_back();
    }
    return result;
  }

  /**
   * @brief Normalize a row of strings for logical comparison.
   * Applies decimal normalization to each cell.
   */
  static std::vector<std::string> normalize_row(const std::vector<std::string> &row)
  {
    std::vector<std::string> result;
    result.reserve(row.size());
    for (const auto &cell : row) {
      result.push_back(normalize_decimal_str(cell));
    }
    return result;
  }

  // Internal ordered verification
  bool verify_ordered_internal(const std::vector<std::vector<std::string>> &expected)
  {
    if (row_count() != static_cast<int64_t>(expected.size())) {
      print_diff(expected, "row count mismatch");
      return false;
    }

    for (int64_t i = 0; i < row_count(); ++i) {
      if (normalize_row(rows_[i]) != normalize_row(expected[i])) {
        print_diff(expected, "row " + std::to_string(i) + " mismatch");
        return false;
      }
    }
    return true;
  }

  // Internal unordered verification
  bool verify_unordered_internal(const std::vector<std::vector<std::string>> &expected)
  {
    if (row_count() != static_cast<int64_t>(expected.size())) {
      print_diff_unordered(expected, "row count mismatch");
      return false;
    }

    auto sorted_actual = rows_;
    auto sorted_expected = expected;
    // Normalize both before sorting
    for (auto &row : sorted_actual) { row = normalize_row(row); }
    for (auto &row : sorted_expected) { row = normalize_row(row); }
    std::sort(sorted_actual.begin(), sorted_actual.end());
    std::sort(sorted_expected.begin(), sorted_expected.end());

    for (int64_t i = 0; i < row_count(); ++i) {
      if (sorted_actual[i] != sorted_expected[i]) {
        print_diff_unordered(expected, "rows differ after sorting");
        return false;
      }
    }
    return true;
  }

  // Print diff for unordered verification
  void print_diff_unordered(const std::vector<std::vector<std::string>> &expected,
                             const std::string &reason) const
  {
    std::cerr << "\n=== Unordered Verification FAILED: " << reason << " ===" << std::endl;
    std::cerr << "\nExpected (" << expected.size() << " rows, unordered):" << std::endl;
    for (size_t i = 0; i < expected.size(); ++i) {
      std::cerr << "  [" << i << "] ";
      for (size_t j = 0; j < expected[i].size(); ++j) {
        if (j > 0) std::cerr << ", ";
        std::cerr << expected[i][j];
      }
      std::cerr << std::endl;
    }
    std::cerr << "\nActual (" << row_count() << " rows):" << std::endl;
    for (int64_t i = 0; i < row_count(); ++i) {
      std::cerr << "  [" << i << "] ";
      for (size_t j = 0; j < rows_[i].size(); ++j) {
        if (j > 0) std::cerr << ", ";
        std::cerr << rows_[i][j];
      }
      std::cerr << std::endl;
    }
    std::cerr << "=============================================" << std::endl;
  }

  // Print diff information on failure
  void print_diff(const std::vector<std::vector<std::string>> &expected,
                  const std::string &reason) const
  {
    std::cerr << "\n=== Verification FAILED: " << reason << " ===" << std::endl;
    std::cerr << "\nExpected (" << expected.size() << " rows):" << std::endl;
    for (size_t i = 0; i < expected.size(); ++i) {
      std::cerr << "  [" << i << "] ";
      for (size_t j = 0; j < expected[i].size(); ++j) {
        if (j > 0) std::cerr << ", ";
        std::cerr << expected[i][j];
      }
      std::cerr << std::endl;
    }

    std::cerr << "\nActual (" << row_count() << " rows):" << std::endl;
    for (int64_t i = 0; i < row_count(); ++i) {
      std::cerr << "  [" << i << "] ";
      for (size_t j = 0; j < rows_[i].size(); ++j) {
        if (j > 0) std::cerr << ", ";
        std::cerr << rows_[i][j];
      }
      std::cerr << std::endl;
    }
    std::cerr << "=============================================" << std::endl;
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_RESULT_H_

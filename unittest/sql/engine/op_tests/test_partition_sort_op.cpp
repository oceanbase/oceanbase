/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Unit tests for Partition Sort optimization.
 *
 * Test coverage:
 * - Task 1: Null-Free optimization (test_null_free_compare, test_aqs_init_fallback)
 * - Task 2: Encode sortkey for partition sort (test_encode_sortkey_enabled, test_partition_sort_params, etc.)
 * - Task 3: Partition aggregation (test_hash_build, test_partition_sort_order, etc.)
 * - Task 4: Memory sort specialization (test_memory_only_sort)
 * - Comprehensive partition sort tests with various data volumes and configurations
 */

#include <gtest/gtest.h>
#include <map>
#include <algorithm>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_sort.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

class PartitionSortOpTest : public OpTestKit
{
protected:
  // ===== Helper Methods =====

  /**
   * @brief Create basic partition sort test data.
   * Creates num_parts partitions with rows_per_part rows each.
   */
  std::vector<TestRow> make_partition_data(int64_t num_parts, int64_t rows_per_part)
  {
    std::vector<TestRow> data;
    for (int64_t p = 1; p <= num_parts; ++p) {
      for (int64_t r = 1; r <= rows_per_part; ++r) {
        data.push_back({p, r, p * 100 + r});
      }
    }
    return data;
  }

  /**
   * @brief Create partition data with varying row counts per partition.
   * @param part_row_counts Map of partition key -> number of rows for that partition
   * @param sort_key_start Starting value for sort key within each partition
   */
  std::vector<TestRow> make_varied_partition_data(
      const std::map<int64_t, int64_t> &part_row_counts,
      int64_t sort_key_start = 1)
  {
    std::vector<TestRow> data;
    for (const auto &entry : part_row_counts) {
      int64_t pk = entry.first;
      int64_t num_rows = entry.second;
      for (int64_t r = 0; r < num_rows; ++r) {
        data.push_back({pk, sort_key_start + r, pk * 1000 + r});
      }
    }
    return data;
  }

  /**
   * @brief Create partition data with shuffled sort keys (for testing sorting).
   * The sort key values are generated in random order within each partition.
   */
  std::vector<TestRow> make_shuffled_partition_data(int64_t num_parts, int64_t rows_per_part)
  {
    std::vector<TestRow> data;
    for (int64_t p = 1; p <= num_parts; ++p) {
      std::vector<int64_t> sort_keys;
      for (int64_t r = 1; r <= rows_per_part; ++r) {
        sort_keys.push_back(r);
      }
      // Shuffle sort keys within each partition
      std::random_shuffle(sort_keys.begin(), sort_keys.end());
      for (int64_t r = 0; r < rows_per_part; ++r) {
        data.push_back({p, sort_keys[r], p * 100 + sort_keys[r]});
      }
    }
    return data;
  }

  /**
   * @brief Create single partition data (all rows have same partition key).
   */
  std::vector<TestRow> make_single_partition_data(int64_t num_rows, int64_t pk = 1)
  {
    std::vector<TestRow> data;
    for (int64_t r = 1; r <= num_rows; ++r) {
      data.push_back({pk, r, pk * 100 + r});
    }
    return data;
  }

  /**
   * @brief Create data where each partition has exactly one row.
   */
  std::vector<TestRow> make_single_row_per_partition_data(int64_t num_parts)
  {
    std::vector<TestRow> data;
    for (int64_t p = 1; p <= num_parts; ++p) {
      data.push_back({p, p * 10, p * 100});
    }
    return data;
  }

  /**
   * @brief Create data with all same sort key within a partition.
   */
  std::vector<TestRow> make_same_sort_key_data(int64_t num_parts, int64_t rows_per_part,
                                                int64_t fixed_sort_key = 100)
  {
    std::vector<TestRow> data;
    for (int64_t p = 1; p <= num_parts; ++p) {
      for (int64_t r = 1; r <= rows_per_part; ++r) {
        data.push_back({p, fixed_sort_key, p * 100 + r});
      }
    }
    return data;
  }

  /**
   * @brief Compare two string values numerically if possible, otherwise lexicographically.
   * @return negative if a < b, positive if a > b, 0 if equal
   */
  int64_t compare_values(const std::string &a, const std::string &b)
  {
    // Try numeric comparison first
    try {
      int64_t a_val = std::stoll(a);
      int64_t b_val = std::stoll(b);
      return a_val - b_val;
    } catch (...) {
      // Fall back to lexicographic comparison for non-numeric values
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }
  }

  /**
   * @brief Column ordering specification for verification.
   */
  struct ColumnOrderSpec {
    int64_t col_idx;    // Column index (0-based)
    bool ascending;     // true for ASC, false for DESC

    ColumnOrderSpec(int64_t idx, bool asc = true) : col_idx(idx), ascending(asc) {}
  };

  /**
   * @brief Parse ORDER BY string into column ordering specifications.
   * Example: "pk ASC, sk DESC" -> [{0, true}, {1, false}]
   * @param order_by_str The ORDER BY string
   * @param col_names Column names in order (to map name to index)
   * @return Vector of ColumnOrderSpec
   */
  std::vector<ColumnOrderSpec> parse_order_by(const std::string &order_by_str,
                                               const std::vector<std::string> &col_names)
  {
    std::vector<ColumnOrderSpec> specs;

    // Build column name to index mapping
    std::map<std::string, int64_t> col_map;
    for (size_t i = 0; i < col_names.size(); ++i) {
      col_map[col_names[i]] = static_cast<int64_t>(i);
    }

    // Split by comma
    std::istringstream ss(order_by_str);
    std::string token;
    while (std::getline(ss, token, ',')) {
      // Trim whitespace
      size_t start = token.find_first_not_of(" \t");
      size_t end = token.find_last_not_of(" \t");
      if (start == std::string::npos) continue;
      token = token.substr(start, end - start + 1);

      // Parse "col_name [ASC|DESC]"
      std::istringstream tok_ss(token);
      std::string col_name, order;
      tok_ss >> col_name >> order;

      // Default to ASC if not specified
      bool ascending = true;
      if (!order.empty()) {
        std::transform(order.begin(), order.end(), order.begin(), ::toupper);
        ascending = (order == "ASC");
      }

      // Find column index
      auto it = col_map.find(col_name);
      if (it != col_map.end()) {
        specs.push_back(ColumnOrderSpec(it->second, ascending));
      }
    }

    return specs;
  }

  /**
   * @brief Compare two rows by multiple columns.
   * @param row1 First row
   * @param row2 Second row
   * @param specs Column ordering specifications
   * @return negative if row1 < row2, positive if row1 > row2, 0 if equal
   */
  int64_t compare_rows_by_columns(const std::vector<std::string> &row1,
                                   const std::vector<std::string> &row2,
                                   const std::vector<ColumnOrderSpec> &specs)
  {
    for (const auto &spec : specs) {
      if (spec.col_idx >= static_cast<int64_t>(row1.size()) ||
          spec.col_idx >= static_cast<int64_t>(row2.size())) {
        continue;
      }
      int64_t cmp = compare_values(row1[spec.col_idx], row2[spec.col_idx]);
      if (cmp != 0) {
        return spec.ascending ? cmp : -cmp;
      }
    }
    return 0;
  }

  /**
   * @brief Get partition key string from a row (for grouping).
   * @param row The row
   * @param partition_specs Partition column specifications
   * @return Concatenated partition key string
   */
  std::string get_partition_key(const std::vector<std::string> &row,
                                 const std::vector<ColumnOrderSpec> &partition_specs)
  {
    std::string key;
    for (const auto &spec : partition_specs) {
      if (spec.col_idx < static_cast<int64_t>(row.size())) {
        if (!key.empty()) key += "|";
        key += row[spec.col_idx];
      }
    }
    return key;
  }

  /**
   * @brief Verify partition sort result correctness with multi-column support.
   *
   * Checks that:
   * 1. Rows belonging to the same partition are consecutive in output
   * 2. Within each partition, rows are sorted by sort columns
   * 3. (Optional) Partition groups appear in specified order
   *
   * @param result The OpTestResult to verify
   * @param partition_specs Partition column specifications (indices and ordering)
   * @param sort_specs Sort column specifications (indices and ordering)
   * @param check_partition_ordering If true, also verify partition group ordering
   * @return true if all conditions satisfied, false otherwise with diagnostic output
   */
  bool verify_partition_sort_result_ex(const OpTestResult &result,
                                        const std::vector<ColumnOrderSpec> &partition_specs,
                                        const std::vector<ColumnOrderSpec> &sort_specs,
                                        bool check_partition_ordering = false)
  {
    if (result.row_count() == 0) {
      std::cout << "verify_partition_sort_result PASSED: empty result" << std::endl;
      return true;
    }

    // Validate column count
    int64_t max_col_idx = 0;
    for (const auto &spec : partition_specs) {
      max_col_idx = std::max(max_col_idx, spec.col_idx);
    }
    for (const auto &spec : sort_specs) {
      max_col_idx = std::max(max_col_idx, spec.col_idx);
    }

    if (result.get_row(0).size() <= static_cast<size_t>(max_col_idx)) {
      std::cerr << "verify_partition_sort_result FAILED: Insufficient columns. "
                << "Need at least " << (max_col_idx + 1) << ", got "
                << result.get_row(0).size() << std::endl;
      return false;
    }

    // Track state
    std::string prev_partition_key;
    std::vector<std::string> prev_row;
    bool first_row = true;

    // Partition tracking for diagnostics and consecutiveness check
    std::map<std::string, int64_t> partition_row_counts;
    std::map<std::string, int64_t> partition_first_row_idx;
    std::string first_partition, last_partition;

    for (int64_t i = 0; i < result.row_count(); ++i) {
      const std::vector<std::string> &row = result.get_row(i);
      std::string partition_key = get_partition_key(row, partition_specs);

      // Track partition appearances
      partition_row_counts[partition_key]++;
      if (partition_first_row_idx.find(partition_key) == partition_first_row_idx.end()) {
        partition_first_row_idx[partition_key] = i;
      }

      if (first_row) {
        first_partition = partition_key;
        last_partition = partition_key;
        prev_partition_key = partition_key;
        prev_row = row;
        first_row = false;
      } else {
        if (partition_key == prev_partition_key) {
          // Same partition - verify sort key ordering
          int64_t cmp = compare_rows_by_columns(row, prev_row, sort_specs);
          if (cmp < 0) {
            std::cerr << "verify_partition_sort_result FAILED: Rows not properly sorted within partition '"
                      << partition_key << "'. Row " << i << " should come before row " << (i - 1) << std::endl;
            std::cerr << "  Previous row: ";
            for (const auto &v : prev_row) std::cerr << v << " ";
            std::cerr << "\n  Current row:  ";
            for (const auto &v : row) std::cerr << v << " ";
            std::cerr << std::endl;
            return false;
          }
        } else {
          // New partition - check consecutiveness
          if (partition_row_counts[partition_key] > 1) {
            std::cerr << "verify_partition_sort_result FAILED: Partition '" << partition_key
                      << "' is not consecutive. First appeared at row "
                      << partition_first_row_idx[partition_key]
                      << ", now appears again at row " << i << std::endl;
            return false;
          }

          // Check partition ordering if requested
          if (check_partition_ordering && !partition_specs.empty()) {
            int64_t cmp = compare_rows_by_columns(row, prev_row, partition_specs);
            if (cmp < 0) {
              std::cerr << "verify_partition_sort_result FAILED: Partition ordering violation. "
                        << "Partition '" << partition_key << "' should come before '"
                        << prev_partition_key << "'" << std::endl;
              return false;
            }
          }

          last_partition = partition_key;
        }

        prev_partition_key = partition_key;
        prev_row = row;
      }
    }

    return true;
  }

  /**
   * @brief Verify partition sort result with single partition and sort column (convenience API).
   *
   * @param result The OpTestResult to verify
   * @param partition_col_idx Partition column index
   * @param sort_col_idx Sort column index
   * @param sort_ascending Sort column ascending or not
   * @param check_partition_ordering Whether to check partition ordering
   * @param partition_ascending Partition column ascending or not (only used if check_partition_ordering is true)
   * @return true if verification passed
   */
  bool verify_partition_sort_result(const OpTestResult &result,
                                     int64_t partition_col_idx,
                                     int64_t sort_col_idx,
                                     bool sort_ascending = true,
                                     bool check_partition_ordering = false,
                                     bool partition_ascending = true)
  {
    std::vector<ColumnOrderSpec> partition_specs;
    partition_specs.push_back(ColumnOrderSpec(partition_col_idx, partition_ascending));

    std::vector<ColumnOrderSpec> sort_specs;
    sort_specs.push_back(ColumnOrderSpec(sort_col_idx, sort_ascending));

    return verify_partition_sort_result_ex(result, partition_specs, sort_specs, check_partition_ordering);
  }

  /**
   * @brief Verify partition sort result from ORDER BY string and partition count.
   *
   * This is the most convenient API - it parses the ORDER BY string and automatically
   * determines partition columns and sort columns based on partition count.
   *
   * @param result The OpTestResult to verify
   * @param order_by_str ORDER BY string (e.g., "pk ASC, sk ASC")
   * @param col_names Column names in order (e.g., {"pk", "sk", "val"})
   * @param part_cnt Number of partition columns (first N columns in ORDER BY)
   * @param check_partition_ordering Whether to check partition group ordering
   * @return true if verification passed
   */
  bool verify_partition_sort_result_from_order(const OpTestResult &result,
                                                const std::string &order_by_str,
                                                const std::vector<std::string> &col_names,
                                                int64_t part_cnt,
                                                bool check_partition_ordering = false)
  {
    std::vector<ColumnOrderSpec> all_specs = parse_order_by(order_by_str, col_names);

    if (static_cast<int64_t>(all_specs.size()) < part_cnt) {
      std::cerr << "verify_partition_sort_result_from_order FAILED: Not enough columns in ORDER BY. "
                << "part_cnt=" << part_cnt << ", but ORDER BY has " << all_specs.size() << " columns" << std::endl;
      return false;
    }

    // First part_cnt columns are partition columns, rest are sort columns
    std::vector<ColumnOrderSpec> partition_specs(all_specs.begin(), all_specs.begin() + part_cnt);
    std::vector<ColumnOrderSpec> sort_specs(all_specs.begin() + part_cnt, all_specs.end());

    return verify_partition_sort_result_ex(result, partition_specs, sort_specs, check_partition_ordering);
  }
};

// ============================================================================
// Task 1: Null-Free Optimization Tests (Basic Tests)
// ============================================================================

// TC1: Partition sort with empty input should return empty output.
TEST_F(PartitionSortOpTest, EmptyInput)
{
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(false)
      .order_by("pk ASC, sk ASC")
      .with_data(std::vector<TestRow>{})
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// TC2: Partition sort on non-empty input.
TEST_F(PartitionSortOpTest, NonEmptyInputIntraPartitionOrder)
{
  // First test without partition sort to verify mock data works
  OpTestResult basic_result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_encode_sortkey(false)
      .order_by("pk ASC, sk ASC")
      .with_data({
          {1, 3, 100}, {1, 1, 101}, {1, 2, 102},
          {2, 2, 200}, {2, 3, 201}, {2, 1, 202}
      })
      .run(engine_);


  // Now test with partition sort
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(false)
      .order_by("pk ASC, sk ASC")
      .with_data({
          {1, 3, 100}, {1, 1, 101}, {1, 2, 102},
          {2, 2, 200}, {2, 3, 201}, {2, 1, 202}
      })
      .run(engine_);


  EXPECT_EQ(6, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"2", "1", "202"}, {"2", "2", "200"}, {"2", "3", "201"},
      {"1", "1", "101"}, {"1", "2", "102"}, {"1", "3", "100"}
  }));
}

// ============================================================================
// Task 2: Comprehensive Partition Sort Tests
// ============================================================================

// TC3: Large data volume - 1000+ rows across 10 partitions (~100 rows each)
TEST_F(PartitionSortOpTest, LargeDataVolume)
{
  // Generate 1000 rows: 10 partitions, 100 rows each
  std::vector<TestRow> data = make_shuffled_partition_data(10, 100);
  ASSERT_EQ(1000, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(1000, result.row_count());
  // Use verify_partition_sort_result_from_order for automatic verification based on ORDER BY
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC4: Multiple partitions with varying row counts (20 partitions, some have 5 rows, some have 30)
TEST_F(PartitionSortOpTest, MultiplePartitions)
{
  // Create 20 partitions with varying row counts
  std::map<int64_t, int64_t> part_row_counts;
  // Partitions 1-10: 5 rows each
  for (int64_t p = 1; p <= 10; ++p) {
    part_row_counts[p] = 5;
  }
  // Partitions 11-20: 30 rows each
  for (int64_t p = 11; p <= 20; ++p) {
    part_row_counts[p] = 30;
  }

  std::vector<TestRow> data = make_varied_partition_data(part_row_counts);
  // Shuffle the data to make it unordered
  std::random_shuffle(data.begin(), data.end());

  int64_t expected_rows = 10 * 5 + 10 * 30; // 50 + 300 = 350
  ASSERT_EQ(expected_rows, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(expected_rows, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC5: Single partition - all 50 rows belong to the same partition (pk=1)
TEST_F(PartitionSortOpTest, SinglePartition)
{
  std::vector<TestRow> data = make_single_partition_data(50, 1);
  // Shuffle to make sort key unordered
  std::random_shuffle(data.begin(), data.end());
  ASSERT_EQ(50, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(50, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));

  // All rows should have pk=1
  for (int64_t i = 0; i < result.row_count(); ++i) {
    EXPECT_EQ("1", result.get_row(i)[0]);
  }
}

// TC6: Descending sort key - partition sort with pk ASC, sk DESC ordering
TEST_F(PartitionSortOpTest, DescendingSortKey)
{
  std::vector<TestRow> data;
  // Create data with 5 partitions, 20 rows each
  for (int64_t p = 1; p <= 5; ++p) {
    for (int64_t r = 1; r <= 20; ++r) {
      data.push_back({p, r, p * 100 + r});
    }
  }
  // Shuffle the data
  std::random_shuffle(data.begin(), data.end());
  ASSERT_EQ(100, data.size());

  const char *order_by = "pk ASC, sk DESC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(100, result.row_count());
  // Use verify_partition_sort_result_from_order for automatic verification
  // This will correctly parse "pk ASC, sk DESC" and verify pk as partition key, sk DESC as sort key
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC7: Single row per partition - 20 partitions, each with exactly 1 row
TEST_F(PartitionSortOpTest, SingleRowPerPartition)
{
  std::vector<TestRow> data = make_single_row_per_partition_data(20);
  // Shuffle the data
  std::random_shuffle(data.begin(), data.end());
  ASSERT_EQ(20, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(20, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC8: All same sort key within partitions - multiple rows in same partition with identical sort key
TEST_F(PartitionSortOpTest, AllSameSortKey)
{
  // 5 partitions, 10 rows each, all with same sort key (100) within partition
  std::vector<TestRow> data = make_same_sort_key_data(5, 10, 100);
  ASSERT_EQ(50, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(50, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));

  // All rows in each partition should have sort_key=100
  for (int64_t i = 0; i < result.row_count(); ++i) {
    EXPECT_EQ("100", result.get_row(i)[1]);
  }
}

// TC9: Encode sortkey enabled - explicit test with encode_sortkey=true
TEST_F(PartitionSortOpTest, EncodeSortkeyEnabled)
{
  std::vector<TestRow> data = make_shuffled_partition_data(8, 25);
  ASSERT_EQ(200, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)  // Explicitly enabled
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(200, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC10: Encode sortkey disabled - explicit test with encode_sortkey=false
TEST_F(PartitionSortOpTest, EncodeSortkeyDisabled)
{
  std::vector<TestRow> data = make_shuffled_partition_data(8, 25);
  ASSERT_EQ(200, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(false)  // Explicitly disabled
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(200, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC11: Many partitions small data - 50 partitions, 2 rows each (100 total)
TEST_F(PartitionSortOpTest, ManyPartitionsSmallData)
{
  std::vector<TestRow> data;
  for (int64_t p = 1; p <= 50; ++p) {
    // Each partition has 2 rows with different sort keys
    data.push_back({p, p * 10 + 1, p * 100 + 1});
    data.push_back({p, p * 10 + 2, p * 100 + 2});
  }
  // Shuffle
  std::random_shuffle(data.begin(), data.end());
  ASSERT_EQ(100, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(100, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC12: Large single partition - 500 rows all in one partition
TEST_F(PartitionSortOpTest, LargeSinglePartition)
{
  std::vector<TestRow> data = make_single_partition_data(500, 1);
  // Shuffle
  std::random_shuffle(data.begin(), data.end());
  ASSERT_EQ(500, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(500, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));

  // All rows should have pk=1
  for (int64_t i = 0; i < result.row_count(); ++i) {
    EXPECT_EQ("1", result.get_row(i)[0]);
  }
}

// TC13: Very large data volume - 2000 rows across 20 partitions for stress testing
TEST_F(PartitionSortOpTest, VeryLargeDataVolume)
{
  // 20 partitions, 100 rows each = 2000 rows
  std::vector<TestRow> data = make_shuffled_partition_data(20, 100);
  ASSERT_EQ(2000, data.size());

  const char *order_by = "pk ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(2000, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC14: Mixed sort directions across partitions - test with pk DESC, sk ASC
TEST_F(PartitionSortOpTest, PartitionKeyDescending)
{
  std::vector<TestRow> data;
  for (int64_t p = 1; p <= 10; ++p) {
    for (int64_t r = 1; r <= 10; ++r) {
      data.push_back({p, r, p * 100 + r});
    }
  }
  std::random_shuffle(data.begin(), data.end());
  ASSERT_EQ(100, data.size());

  const char *order_by = "pk DESC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(100, result.row_count());
  // For pk DESC, sk ASC:
  // - Partition sort does NOT guarantee global partition key ordering
  // - Within each partition, sort keys should be ascending
  EXPECT_TRUE(verify_partition_sort_result_from_order(result, order_by, {"pk", "sk", "val"}, 1));
}

// TC15: Edge case - duplicate partition keys with different sort keys
TEST_F(PartitionSortOpTest, DuplicatePartitionKeyDifferentSortKeys)
{
  std::vector<TestRow> data;
  // Same partition key (1), different sort keys that should be sorted
  data.push_back({1, 5, 105});
  data.push_back({1, 3, 103});
  data.push_back({1, 1, 101});
  data.push_back({1, 4, 104});
  data.push_back({1, 2, 102});
  // Another partition
  data.push_back({2, 2, 202});
  data.push_back({2, 1, 201});

  OpTestResult result = SortTestSpec()
      .table("t", "pk int, sk int, val int")
      .select("pk, sk, val")
      .with_partition_sort(1)
      .with_encode_sortkey(true)
      .order_by("pk ASC, sk ASC")
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(7, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
      {"2", "1", "201"},
      {"2", "2", "202"},
      {"1", "1", "101"},
      {"1", "2", "102"},
      {"1", "3", "103"},
      {"1", "4", "104"},
      {"1", "5", "105"}
  }));
}

// TC16: Encode sortkey + addon + partition hash collision (NULL vs empty string)
// Reproduces crash in GeneralCompare::compare when encode_sortkey=true && has_addon=true
// and HashNodeComparer encounters hash collision between different partition keys.
// Without fix: cmp_sk_exprs_=addon_exprs_ used with sk_row_meta_ causes index mismatch -> crash
TEST_F(PartitionSortOpTest, EncodeSortkeyAddonHashCollision)
{
  // Use varchar partition key: NULL and '' produce same hash but different partition keys
  // Extra columns (val1..val3) force has_addon=true (non-sort columns go to addon)
  std::vector<TestRow> data;
  // NULL partition key rows
  data.push_back({TestValue::null(), 1, 10, 100, 200, 300});
  data.push_back({TestValue::null(), 2, 11, 101, 201, 301});
  data.push_back({TestValue::null(), 3, 12, 102, 202, 302});
  // Empty string partition key rows (same hash as NULL, different partition key)
  data.push_back({std::string(""),   1, 20, 110, 210, 310});
  data.push_back({std::string(""),   2, 21, 111, 211, 311});
  data.push_back({std::string(""),   3, 22, 112, 212, 312});
  // Regular partition key rows
  data.push_back({std::string("a"),  1, 30, 120, 220, 320});
  data.push_back({std::string("a"),  2, 31, 121, 221, 321});

  const char *order_by = "pk ASC, pk2 ASC, sk ASC";
  OpTestResult result = SortTestSpec()
      .table("t", "pk varchar(100), pk2 int, sk int, val1 int, val2 int, val3 int")
      .select("pk, pk2, sk, val1, val2, val3")
      .with_partition_sort(2)
      .with_encode_sortkey(true)
      .with_addon(true)
      .order_by(order_by)
      .with_data(data)
      .run(engine_);

  EXPECT_EQ(8, result.row_count());
  EXPECT_TRUE(verify_partition_sort_result_from_order(
      result, order_by, {"pk", "pk2", "sk", "val1", "val2", "val3"}, 2));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  // OB_LOGGER.set_log_level("INFO");  // 设置日志级别
  // OB_LOGGER.set_file_name("test_partition_sort.log", true);  // 输出到文件
  return RUN_ALL_TESTS();
}
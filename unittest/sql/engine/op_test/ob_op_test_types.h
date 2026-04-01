/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_TYPES_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_TYPES_H_

#include <cstdint>
#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <sstream>
#include <algorithm>
#include <map>

namespace oceanbase
{
namespace sql
{

/**
 * @brief Null ordering mode for comparisons
 */
enum class NullOrdering {
  NULL_FIRST,   // NULL is smallest (default)
  NULL_LAST     // NULL is largest
};

// TODO: use VecTCCmpCalc for comparison
/**
 * @brief TestValue represents a single value in test data.
 * Supports int64, double, string, and null.
 *
 * Design goals:
 * 1. Easy to construct: TestValue v = 123; or TestValue v = "abc";
 * 2. Type-safe: variant ensures only valid types are stored
 * 3. Null handling: std::monostate represents NULL
 */
class TestValue
{
public:
  using ValueType = std::variant<
    std::monostate,    // represents NULL
    int64_t,           // integer values
    double,            // floating point values
    std::string        // string values (varchar/char/number as string)
  >;

  // Default constructor creates NULL value
  TestValue() : value_(std::monostate{}) {}

  // Constructors for different types
  TestValue(int64_t v) : value_(v) {}
  TestValue(int32_t v) : value_(static_cast<int64_t>(v)) {}
  TestValue(double v) : value_(v) {}
  TestValue(float v) : value_(static_cast<double>(v)) {}
  TestValue(const char *v) : value_(std::string(v)) {}
  TestValue(const std::string &v) : value_(v) {}

  // Factory method for explicit NULL
  static TestValue null() { return TestValue(); }

  // Static null ordering configuration (default: NULL_FIRST)
  static NullOrdering null_ordering_;

  static void set_null_ordering(NullOrdering ordering) {
    null_ordering_ = ordering;
  }

  static NullOrdering get_null_ordering() {
    return null_ordering_;
  }

  // Check if value is NULL
  bool is_null() const { return std::holds_alternative<std::monostate>(value_); }

  // Type queries
  bool is_int() const { return std::holds_alternative<int64_t>(value_); }
  bool is_double() const { return std::holds_alternative<double>(value_); }
  bool is_string() const { return std::holds_alternative<std::string>(value_); }

  // Value accessors (undefined behavior if type doesn't match)
  int64_t get_int() const { return std::get<int64_t>(value_); }
  double get_double() const { return std::get<double>(value_); }
  const std::string &get_string() const { return std::get<std::string>(value_); }

  // Convert to string for comparison/debugging
  std::string to_string() const
  {
    if (is_null()) {
      return "NULL";
    } else if (is_int()) {
      return std::to_string(get_int());
    } else if (is_double()) {
      return std::to_string(get_double());
    } else {
      return get_string();
    }
  }

  // Comparison operators
  // std::variant comparison handles all cases correctly:
  // - Both null (monostate): equal
  // - Different types: not equal
  // - Same type: compare values
  bool operator==(const TestValue &other) const
  {
    return value_ == other.value_;
  }

  bool operator!=(const TestValue &other) const
  {
    return value_ != other.value_;
  }

  bool operator<(const TestValue &other) const
  {
    // NULL comparison based on null ordering mode
    if (is_null() && other.is_null()) {
      return false;  // Equal
    }
    if (is_null()) {
      return null_ordering_ == NullOrdering::NULL_FIRST;
    }
    if (other.is_null()) {
      return null_ordering_ == NullOrdering::NULL_LAST;
    }
    return value_ < other.value_;
  }

private:
  ValueType value_;
};

// Static member definition
inline NullOrdering TestValue::null_ordering_ = NullOrdering::NULL_FIRST;

/**
 * @brief TestRow represents a row of test data.
 * A row is a vector of TestValue objects.
 *
 * Supports initializer list construction:
 *   TestRow row = {1, 2.5, "abc", TestValue::null()};
 */
class TestRow
{
public:
  TestRow() = default;

  // Constructor from initializer list
  TestRow(std::initializer_list<TestValue> values) : values_(values) {}

  // Constructor from vector
  explicit TestRow(std::vector<TestValue> values) : values_(std::move(values)) {}

  // Accessors
  size_t size() const { return values_.size(); }
  bool empty() const { return values_.empty(); }
  const TestValue &operator[](size_t idx) const { return values_[idx]; }
  TestValue &operator[](size_t idx) { return values_[idx]; }

  // Iterators
  std::vector<TestValue>::const_iterator begin() const { return values_.begin(); }
  std::vector<TestValue>::const_iterator end() const { return values_.end(); }
  std::vector<TestValue>::iterator begin() { return values_.begin(); }
  std::vector<TestValue>::iterator end() { return values_.end(); }

  // Add a value
  void push_back(const TestValue &v) { values_.push_back(v); }
  void push_back(TestValue &&v) { values_.push_back(std::move(v)); }

  // Comparison
  bool operator==(const TestRow &other) const { return values_ == other.values_; }
  bool operator!=(const TestRow &other) const { return values_ != other.values_; }

  // Convert to string for debugging
  std::string to_string() const
  {
    std::string result = "{";
    for (size_t i = 0; i < values_.size(); ++i) {
      if (i > 0) {
        result += ", ";
      }
      result += values_[i].to_string();
    }
    result += "}";
    return result;
  }

private:
  std::vector<TestValue> values_;
};

/**
 * @brief SortSpec describes how to sort by one column
 */
struct SortSpec {
  int64_t col_idx;    // Column index (0-based)
  bool ascending;     // true for ASC, false for DESC
  bool nulls_first;   // true for NULLS FIRST, false for NULLS LAST (default: true)
};

/**
 * @brief Parse sort specification string into SortSpec vector
 * @param sort_desc Sort description like "b DESC", "a ASC, b DESC"
 * @param col_names Column names in order (to map name to index)
 * @return Vector of SortSpec
 *
 * Examples:
 *   "b DESC" -> [{col_idx=1, ascending=false}]
 *   "a ASC, b DESC" -> [{col_idx=0, ascending=true}, {col_idx=1, ascending=false}]
 */
inline std::vector<SortSpec> parse_sort_spec(const std::string &sort_desc,
                                              const std::vector<std::string> &col_names)
{
  // TODO: support complex sorted expr, such as a + b desc
  // use sorted functions in desc
  std::vector<SortSpec> specs;

  // Parse column name to index mapping
  std::map<std::string, int64_t> col_map;
  for (size_t i = 0; i < col_names.size(); ++i) {
    col_map[col_names[i]] = static_cast<int64_t>(i);
  }

  // Split by comma
  std::istringstream ss(sort_desc);
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
      ascending = (order == "ASC" || order != "DESC");
    }

    // Find column index
    auto it = col_map.find(col_name);
    if (it != col_map.end()) {
      specs.push_back({it->second, ascending, true});
    }
  }

  return specs;
}

/**
 * @brief Sort TestRows according to sort specification
 * @param rows Rows to sort (will be modified in place)
 * @param specs Sort specification
 */
inline void sort_rows(std::vector<TestRow> &rows, const std::vector<SortSpec> &specs)
{
  if (specs.empty()) return;

  std::sort(rows.begin(), rows.end(), [&specs](const TestRow &a, const TestRow &b) {
    for (const auto &spec : specs) {
      if (spec.col_idx >= static_cast<int64_t>(a.size()) ||
          spec.col_idx >= static_cast<int64_t>(b.size())) {
        continue;
      }

      const TestValue &va = a[spec.col_idx];
      const TestValue &vb = b[spec.col_idx];

      // Handle NULL comparison
      if (va.is_null() && vb.is_null()) continue;
      if (va.is_null()) {
        return spec.nulls_first != spec.ascending;  // NULL first for ASC, last for DESC
      }
      if (vb.is_null()) {
        return !spec.nulls_first != spec.ascending;
      }

      // Compare non-null values
      if (va < vb) return spec.ascending;
      if (vb < va) return !spec.ascending;
      // Equal, continue to next sort column
    }
    return false;  // All columns equal
  });
}

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_TYPES_H_
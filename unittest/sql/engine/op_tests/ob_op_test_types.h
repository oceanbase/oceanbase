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
#include <functional>
#include <random>
#include <memory>
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
 * @brief ColumnGenerator generates a single column value for a given row index.
 * Built-in random generators use std::mt19937_64 with fixed seed.
 * Rescan-safe: RNG reseeds when row_idx == 0.
 */
using ColumnGenerator = std::function<TestValue(int64_t row_idx)>;

namespace gen {

inline ColumnGenerator sequential(int64_t start = 0, int64_t step = 1)
{
  return [start, step](int64_t row_idx) -> TestValue {
    return start + row_idx * step;
  };
}

inline ColumnGenerator constant(TestValue value)
{
  return [value](int64_t) -> TestValue {
    return value;
  };
}

inline ColumnGenerator random_int(int64_t min_val, int64_t max_val, uint64_t seed = 42)
{
  auto rng = std::make_shared<std::mt19937_64>(seed);
  auto dist = std::make_shared<std::uniform_int_distribution<int64_t>>(min_val, max_val);
  return [rng, dist, seed](int64_t row_idx) -> TestValue {
    if (row_idx == 0) { rng->seed(seed); }
    return (*dist)(*rng);
  };
}

inline ColumnGenerator random_double(double min_val, double max_val, uint64_t seed = 42)
{
  auto rng = std::make_shared<std::mt19937_64>(seed);
  auto dist = std::make_shared<std::uniform_real_distribution<double>>(min_val, max_val);
  return [rng, dist, seed](int64_t row_idx) -> TestValue {
    if (row_idx == 0) { rng->seed(seed); }
    return (*dist)(*rng);
  };
}

inline ColumnGenerator random_string(int64_t length = 10, uint64_t seed = 42)
{
  auto rng = std::make_shared<std::mt19937_64>(seed);
  return [rng, length, seed](int64_t row_idx) -> TestValue {
    if (row_idx == 0) { rng->seed(seed); }
    static const char charset[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    std::uniform_int_distribution<int> dist(0, sizeof(charset) - 2);
    std::string s;
    s.reserve(length);
    for (int64_t i = 0; i < length; ++i) {
      s += charset[dist(*rng)];
    }
    return s;
  };
}

inline ColumnGenerator random_decimal(int precision = 10, int scale = 2, uint64_t seed = 42)
{
  int int_digits = precision - scale;
  int64_t int_max = 1;
  for (int i = 0; i < int_digits; ++i) { int_max *= 10; }
  int_max -= 1;
  int64_t frac_max = 1;
  for (int i = 0; i < scale; ++i) { frac_max *= 10; }
  frac_max -= 1;

  auto rng = std::make_shared<std::mt19937_64>(seed);
  return [rng, int_max, frac_max, scale, seed](int64_t row_idx) -> TestValue {
    if (row_idx == 0) { rng->seed(seed); }
    std::uniform_int_distribution<int64_t> int_dist(0, int_max);
    int64_t int_part = int_dist(*rng);
    std::string result = std::to_string(int_part);
    if (scale > 0) {
      std::uniform_int_distribution<int64_t> frac_dist(0, frac_max);
      int64_t frac_part = frac_dist(*rng);
      std::string frac_str = std::to_string(frac_part);
      while (static_cast<int>(frac_str.size()) < scale) {
        frac_str = "0" + frac_str;
      }
      result += "." + frac_str;
    }
    return result;
  };
}

inline ColumnGenerator cycle(std::vector<TestValue> values)
{
  return [values](int64_t row_idx) -> TestValue {
    return values[row_idx % values.size()];
  };
}

namespace detail {

// 将 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 解析为微秒值（自 epoch 1970-01-01 00:00:00 UTC）
// 简化实现：假设有效输入，不考虑时区/闰秒
inline int64_t parse_datetime_us(const std::string &s)
{
  int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
  if (sscanf(s.c_str(), "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second) >= 3) {
    // Valid: at least YYYY-MM-DD parsed
  } else if (sscanf(s.c_str(), "%d-%d-%d", &year, &month, &day) == 3) {
    // Valid: YYYY-MM-DD only
  } else {
    return 0;  // Invalid input
  }
  // Days from 1970-01-01 to year-01-01
  int64_t y = year - 1;
  int64_t days_to_year = y * 365 + y / 4 - y / 100 + y / 400 - 719527;
  // Days in months before this month (non-leap year)
  static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int64_t days_to_month = 0;
  for (int m = 1; m < month; ++m) {
    days_to_month += month_days[m];
    if (m == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
      days_to_month += 1;  // leap year February
    }
  }
  int64_t total_days = days_to_year + days_to_month + day;
  int64_t total_us = total_days * 86400000000LL + hour * 3600000000LL + minute * 60000000LL + second * 1000000LL;
  return total_us;
}

// 将 'YYYY-MM-DD' 解析为天数（自 epoch）
inline int32_t parse_date_day(const std::string &s)
{
  int64_t us = parse_datetime_us(s);
  return static_cast<int32_t>(us / 86400000000LL);
}

// 将 'HH:MM:SS' 解析为微秒
inline int64_t parse_time_us(const std::string &s)
{
  int hour = 0, minute = 0, second = 0;
  if (sscanf(s.c_str(), "%d:%d:%d", &hour, &minute, &second) >= 2) {
    return hour * 3600000000LL + minute * 60000000LL + second * 1000000LL;
  }
  return 0;
}

}  // namespace detail

// 随机 datetime 值（微秒编码，int64_t 载体）
// 重载1：直接微秒范围
inline ColumnGenerator random_datetime(
    int64_t start_us, int64_t end_us, uint64_t seed)
{
  auto rng = std::make_shared<std::mt19937_64>(seed);
  auto dist = std::make_shared<std::uniform_int_distribution<int64_t>>(start_us, end_us);
  return [rng, dist, seed](int64_t row_idx) -> TestValue {
    if (row_idx == 0) { rng->seed(seed); }
    return (*dist)(*rng);
  };
}

// 重载2：字符串日期范围（推荐用法）
// 示例：random_datetime("2020-01-01", "2024-12-31")
//       random_datetime("2020-01-01 00:00:00", "2024-12-31 23:59:59")
inline ColumnGenerator random_datetime(
    const std::string &start_date, const std::string &end_date, uint64_t seed = 42)
{
  int64_t start_us = detail::parse_datetime_us(start_date);
  int64_t end_us = detail::parse_datetime_us(end_date);
  return random_datetime(start_us, end_us, seed);
}

// 重载3：默认范围 2000-01-01 ~ 2030-01-01
inline ColumnGenerator random_datetime(uint64_t seed = 42)
{
  return random_datetime(946684800000000LL, 1893456000000000LL, seed);
}

// 随机 date 值（天数编码，int64_t 载体）
// 重载1：直接天数范围
inline ColumnGenerator random_date(
    int32_t start_day, int32_t end_day, uint64_t seed)
{
  auto rng = std::make_shared<std::mt19937_64>(seed);
  auto dist = std::make_shared<std::uniform_int_distribution<int64_t>>(start_day, end_day);
  return [rng, dist, seed](int64_t row_idx) -> TestValue {
    if (row_idx == 0) { rng->seed(seed); }
    return (*dist)(*rng);
  };
}

// 重载2：字符串日期范围
// 示例：random_date("2020-01-01", "2024-12-31")
inline ColumnGenerator random_date(
    const std::string &start_date, const std::string &end_date, uint64_t seed = 42)
{
  int32_t start_day = detail::parse_date_day(start_date);
  int32_t end_day = detail::parse_date_day(end_date);
  return random_date(start_day, end_day, seed);
}

// 重载3：默认范围 2000-01-01 ~ 2030-01-01
inline ColumnGenerator random_date(uint64_t seed = 42)
{
  return random_date(10957, 21915, seed);
}

// 随机 year 值（1901-2155，int64_t 载体）
inline ColumnGenerator random_year(uint64_t seed = 42)
{
  return gen::random_int(1901, 2155, seed);
}

// 随机 bit 值（0 到 2^max_bits - 1，int64_t 载体）
inline ColumnGenerator random_bit(int max_bits = 64, uint64_t seed = 42)
{
  int64_t max_val = (max_bits >= 64) ? INT64_MAX : ((1LL << max_bits) - 1);
  return gen::random_int(0, max_val, seed);
}

// 随机 time 值（微秒编码，int64_t 载体）
// 重载1：直接微秒范围
inline ColumnGenerator random_time(
    int64_t start_us, int64_t end_us, uint64_t seed)
{
  return gen::random_int(start_us, end_us, seed);
}

// 重载2：字符串时间范围
// 示例：random_time("08:00:00", "18:00:00")
inline ColumnGenerator random_time(
    const std::string &start_time, const std::string &end_time, uint64_t seed = 42)
{
  int64_t start_us = detail::parse_time_us(start_time);
  int64_t end_us = detail::parse_time_us(end_time);
  return gen::random_int(start_us, end_us, seed);
}

// 重载3：默认范围 00:00:00 ~ 23:59:59
inline ColumnGenerator random_time(uint64_t seed = 42)
{
  return gen::random_int(0, 86399000000LL, seed);
}

inline ColumnGenerator nullable(ColumnGenerator inner, int64_t null_every_n = 10)
{
  return [inner, null_every_n](int64_t row_idx) -> TestValue {
    if (null_every_n > 0 && (row_idx % null_every_n == 0)) {
      return TestValue::null();
    }
    return inner(row_idx);
  };
}

}  // namespace gen

/**
 * @brief SortExprItem - Single item in a sort expression (postfix notation)
 */
struct SortExprItem {
  enum Type { COLUMN, CONST_INT, OPERATOR } type;
  int64_t value;  // col_idx for COLUMN, const value for CONST_INT, op char for OPERATOR

  SortExprItem() : type(CONST_INT), value(0) {}
  SortExprItem(Type t, int64_t v) : type(t), value(v) {}

  static SortExprItem column(int64_t idx) { return SortExprItem(COLUMN, idx); }
  static SortExprItem const_val(int64_t v) { return SortExprItem(CONST_INT, v); }
  static SortExprItem op(char c) { return SortExprItem(OPERATOR, c); }
};

/**
 * @brief SortSpec describes how to sort by one column or expression
 * Uses postfix notation for expression evaluation.
 */
struct SortSpec {
  std::vector<SortExprItem> postfix_expr;  // Postfix expression for evaluation
  bool ascending;
  bool nulls_first;

  SortSpec() : ascending(true), nulls_first(true) {}

  // Constructor for simple column
  static SortSpec column(int64_t idx, bool asc = true, bool null_first = true) {
    SortSpec spec;
    spec.postfix_expr.push_back(SortExprItem::column(idx));
    spec.ascending = asc;
    spec.nulls_first = null_first;
    return spec;
  }
};

/**
 * @brief Parse sort specification string into SortSpec vector
 * @param sort_desc Sort description like "b DESC", "a + b ASC", "p%2 ASC"
 * @param col_names Column names in order (to map name to index)
 * @return Vector of SortSpec
 *
 * Examples:
 *   "b DESC" -> [column(1, false)]
 *   "a + b ASC" -> [postfix: a b +]
 *   "p%2 ASC, p DESC" -> [postfix: p 2 %], [column(0, false)]
 */
inline std::vector<SortSpec> parse_sort_spec(const std::string &sort_desc,
                                              const std::vector<std::string> &col_names)
{
  std::vector<SortSpec> specs;

  // Build column name to index mapping
  std::map<std::string, int64_t> col_map;
  for (size_t i = 0; i < col_names.size(); ++i) {
    col_map[col_names[i]] = static_cast<int64_t>(i);
  }

  // Operator precedence for Shunting Yard algorithm
  auto precedence = [](char op) -> int {
    switch (op) {
      case '+': case '-': return 1;
      case '*': case '/': case '%': return 2;
      default: return 0;
    }
  };

  auto is_operator = [](char c) { return c == '+' || c == '-' || c == '*' || c == '/' || c == '%'; };

  // Split by comma (top-level, not inside parentheses)
  std::vector<std::string> sort_items;
  std::string current;
  int paren_depth = 0;
  for (char c : sort_desc) {
    if (c == '(') paren_depth++;
    else if (c == ')') paren_depth--;
    else if (c == ',' && paren_depth == 0) {
      sort_items.push_back(current);
      current.clear();
      continue;
    }
    current += c;
  }
  if (!current.empty()) sort_items.push_back(current);

  for (const std::string &item : sort_items) {
    // Trim whitespace
    size_t start = item.find_first_not_of(" \t");
    size_t end = item.find_last_not_of(" \t");
    if (start == std::string::npos) continue;
    std::string expr_str = item.substr(start, end - start + 1);

    // Parse order direction (last word: ASC/DESC)
    std::string order;
    size_t last_space = expr_str.find_last_of(" \t");
    if (last_space != std::string::npos) {
      std::string last_word = expr_str.substr(last_space + 1);
      std::string upper_word = last_word;
      std::transform(upper_word.begin(), upper_word.end(), upper_word.begin(), ::toupper);
      if (upper_word == "ASC" || upper_word == "DESC") {
        order = upper_word;
        expr_str = expr_str.substr(0, last_space);
        size_t new_end = expr_str.find_last_not_of(" \t");
        if (new_end != std::string::npos) {
          expr_str = expr_str.substr(0, new_end + 1);
        }
      }
    }

    bool ascending = (order.empty() || order == "ASC");

    // Tokenize expression
    std::vector<std::string> tokens;
    std::string token;
    for (size_t i = 0; i < expr_str.size(); ++i) {
      char c = expr_str[i];
      if (std::isspace(c)) {
        if (!token.empty()) {
          tokens.push_back(token);
          token.clear();
        }
      } else if (is_operator(c) || c == '(' || c == ')') {
        if (!token.empty()) {
          tokens.push_back(token);
          token.clear();
        }
        tokens.push_back(std::string(1, c));
      } else {
        token += c;
      }
    }
    if (!token.empty()) tokens.push_back(token);

    // Shunting Yard: convert infix to postfix
    std::vector<std::string> output;
    std::vector<char> op_stack;

    for (const std::string &tok : tokens) {
      if (tok.size() == 1 && is_operator(tok[0])) {
        while (!op_stack.empty() && op_stack.back() != '(' &&
               precedence(op_stack.back()) >= precedence(tok[0])) {
          output.push_back(std::string(1, op_stack.back()));
          op_stack.pop_back();
        }
        op_stack.push_back(tok[0]);
      } else if (tok == "(") {
        op_stack.push_back('(');
      } else if (tok == ")") {
        while (!op_stack.empty() && op_stack.back() != '(') {
          output.push_back(std::string(1, op_stack.back()));
          op_stack.pop_back();
        }
        if (!op_stack.empty()) op_stack.pop_back();  // Remove '('
      } else {
        output.push_back(tok);  // Operand (column or constant)
      }
    }
    while (!op_stack.empty()) {
      output.push_back(std::string(1, op_stack.back()));
      op_stack.pop_back();
    }

    // Convert to SortSpec
    SortSpec spec;
    for (const std::string &tok : output) {
      if (tok.size() == 1 && is_operator(tok[0])) {
        spec.postfix_expr.push_back(SortExprItem::op(tok[0]));
      } else {
        // Check if it's a column name
        auto it = col_map.find(tok);
        if (it != col_map.end()) {
          spec.postfix_expr.push_back(SortExprItem::column(it->second));
        } else {
          // Try to parse as integer constant
          try {
            int64_t val = std::stoll(tok);
            spec.postfix_expr.push_back(SortExprItem::const_val(val));
          } catch (...) {
            // Unknown token, skip
          }
        }
      }
    }
    spec.ascending = ascending;
    spec.nulls_first = true;
    specs.push_back(spec);
  }

  return specs;
}

/**
 * @brief Evaluate postfix expression for a row
 */
inline TestValue eval_postfix_expr(const TestRow &row, const std::vector<SortExprItem> &expr) {
  std::vector<TestValue> stack;

  for (const auto &item : expr) {
    if (item.type == SortExprItem::COLUMN) {
      if (item.value >= 0 && item.value < static_cast<int64_t>(row.size())) {
        stack.push_back(row[item.value]);
      } else {
        stack.push_back(TestValue::null());
      }
    } else if (item.type == SortExprItem::CONST_INT) {
      stack.push_back(TestValue(item.value));
    } else if (item.type == SortExprItem::OPERATOR) {
      if (stack.size() < 2) {
        stack.push_back(TestValue::null());
        continue;
      }
      TestValue b = stack.back(); stack.pop_back();
      TestValue a = stack.back(); stack.pop_back();

      if (a.is_null() || b.is_null()) {
        stack.push_back(TestValue::null());
        continue;
      }

      int64_t av = a.is_int() ? a.get_int() : static_cast<int64_t>(a.get_double());
      int64_t bv = b.is_int() ? b.get_int() : static_cast<int64_t>(b.get_double());
      int64_t result = 0;

      switch (item.value) {
        case '+': result = av + bv; break;
        case '-': result = av - bv; break;
        case '*': result = av * bv; break;
        case '/': result = (bv != 0) ? av / bv : 0; break;
        case '%': result = (bv != 0) ? av % bv : 0; break;
        default: result = av; break;
      }
      stack.push_back(TestValue(result));
    }
  }

  return stack.empty() ? TestValue::null() : stack.back();
}

/**
 * @brief Sort TestRows according to sort specification
 */
inline void sort_rows(std::vector<TestRow> &rows, const std::vector<SortSpec> &specs)
{
  if (specs.empty()) return;

  std::sort(rows.begin(), rows.end(), [&specs](const TestRow &a, const TestRow &b) {
    for (const auto &spec : specs) {
      TestValue va = eval_postfix_expr(a, spec.postfix_expr);
      TestValue vb = eval_postfix_expr(b, spec.postfix_expr);

      // Handle NULL comparison
      if (va.is_null() && vb.is_null()) continue;
      if (va.is_null()) {
        return spec.nulls_first != spec.ascending;
      }
      if (vb.is_null()) {
        return !spec.nulls_first != spec.ascending;
      }

      // Compare non-null values
      if (va < vb) return spec.ascending;
      if (vb < va) return !spec.ascending;
    }
    return false;
  });
}

// ===== Sort Key Spec (for with_sorted_data + arbitrary SQL expressions) =====

/**
 * @brief Forward declaration — ob_op_test_types.h is pure C++/STL,
 * but SortKeySpec only stores a pointer so no full include is needed.
 */
class ObExpr;

/**
 * @brief SortKeySpec — resolved sort key using a real ObExpr* looked up from
 * SELECT items or column_items after generate_exprs().
 * Replaces the text-based SortSpec for with_sorted_data() evaluation.
 */
struct SortKeySpec {
  ObExpr *key_expr;
  bool ascending;
  bool nulls_first;

  SortKeySpec() : key_expr(nullptr), ascending(true), nulls_first(true) {}
  SortKeySpec(ObExpr *e, bool asc, bool nf)
    : key_expr(e), ascending(asc), nulls_first(nf) {}
};

/**
 * @brief Parse order_desc string into (trimmed_expr_str, ascending, nulls_first) tuples.
 * Pure string parsing — expression lookup is done separately in build_sort_key_specs().
 *
 * Example: "a asc, a mod b desc" ->
 *   [("a", true, true), ("a mod b", false, true)]
 */
inline std::vector<std::tuple<std::string, bool, bool>>
parse_order_desc_strings(const std::string &order_desc)
{
  std::vector<std::tuple<std::string, bool, bool>> result;

  // Split by top-level commas (not inside parentheses)
  std::vector<std::string> items;
  std::string current;
  int paren_depth = 0;
  for (char c : order_desc) {
    if (c == '(') paren_depth++;
    else if (c == ')') paren_depth--;
    else if (c == ',' && paren_depth == 0) {
      items.push_back(current);
      current.clear();
      continue;
    }
    current += c;
  }
  if (!current.empty()) items.push_back(current);

  for (const std::string &item : items) {
    size_t s = item.find_first_not_of(" \t");
    size_t e = item.find_last_not_of(" \t");
    if (s == std::string::npos) continue;
    std::string expr_str = item.substr(s, e - s + 1);

    // Strip trailing ASC / DESC
    bool ascending = true;
    size_t last_space = expr_str.rfind(' ');
    if (last_space != std::string::npos) {
      std::string last_word = expr_str.substr(last_space + 1);
      std::string upper;
      for (char c : last_word) upper += static_cast<char>(toupper(c));
      if (upper == "ASC" || upper == "DESC") {
        ascending = (upper == "ASC");
        expr_str = expr_str.substr(0, last_space);
        size_t new_e = expr_str.find_last_not_of(" \t");
        if (new_e != std::string::npos) expr_str = expr_str.substr(0, new_e + 1);
      }
    }
    result.emplace_back(expr_str, ascending, /*nulls_first=*/true);
  }
  return result;
}

// Test value NULL for NULL representation in expected data
// Using TestValue::null() static method for clarity in initializer lists
#define NULL_VAL TestValue::null()

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_TYPES_H_
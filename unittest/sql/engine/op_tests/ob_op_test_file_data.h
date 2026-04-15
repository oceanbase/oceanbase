/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_FILE_DATA_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_FILE_DATA_H_

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_types.h"
#include "lib/utility/ob_macro_utils.h"
#include <fstream>
#include <sstream>
#include <algorithm>

namespace oceanbase
{
namespace sql
{

/**
 * @brief Column type enumeration for CSV parsing
 */
enum class CsvColumnType {
  INT,
  DOUBLE,
  VARCHAR,
  STRING  // alias for VARCHAR, treat as string
};

/**
 * @brief CsvDataReader - Reads test data from CSV files.
 *
 * CSV format specification:
 * - Optional type header line starting with "# type:" followed by comma-separated types
 * - Type can be: int, double, varchar(N), string
 * - NULL values are represented as empty fields
 * - First non-comment line is considered data
 *
 * Example:
 * ```
 * # type: int, double, varchar(32)
 * 1,3.14,hello
 * 2,2.71,world
 * ,1.0,null_value
 * ```
 *
 * If no type header is present, all columns are treated as strings unless
 * set_column_types() is called before open() or after open() before read_row().
 */
class CsvDataReader
{
public:
  CsvDataReader() : is_open_(false), has_type_header_(false) {}
  ~CsvDataReader() { close(); }

  /**
   * @brief Open a CSV file for reading.
   * @param file_path Path to the CSV file
   * @return OB_SUCCESS on success, OB_IO_ERROR if file not found
   */
  int open(const std::string &file_path)
  {
    int ret = OB_SUCCESS;
    file_.open(file_path);
    if (!file_.is_open()) {
      ret = OB_IO_ERROR;
      LOG_WARN("failed to open csv file", K(file_path.c_str()), K(ret));
      return ret;
    }
    file_path_ = file_path;
    is_open_ = true;
    has_type_header_ = false;

    // Try to read type header
    std::string line;
    if (std::getline(file_, line)) {
      // Check if this is a type header line
      if (line.compare(0, 7, "# type:") == 0) {
        std::string type_str = line.substr(7);
        parse_types(type_str);
        has_type_header_ = true;
      } else {
        // Not a type header, put the line back for reading
        // Since we can't put back, store it as first data line
        first_line_ = line;
      }
    }

    return ret;
  }

  /**
   * @brief Read one row from the CSV file.
   * @param row Output TestRow object
   * @return OB_SUCCESS on success, OB_ITER_END at end of file
   */
  int read_row(TestRow &row)
  {
    int ret = OB_SUCCESS;

    if (!is_open_) {
      ret = OB_ERROR;
      LOG_WARN("file not open", K(ret));
      return ret;
    }

    std::string line;

    // If we have a first line from open(), use it
    if (!first_line_.empty()) {
      line = first_line_;
      first_line_.clear();
    } else {
      if (!std::getline(file_, line)) {
        ret = OB_ITER_END;
        return ret;
      }
    }

    // Skip empty lines and comment lines
    while (line.empty() || (line.size() > 0 && line[0] == '#' && line.compare(0, 7, "# type:") != 0)) {
      if (!std::getline(file_, line)) {
        ret = OB_ITER_END;
        return ret;
      }
    }

    ret = parse_csv_line(line, row);
    return ret;
  }

  /**
   * @brief Read all rows from the CSV file.
   * @param rows Output vector of TestRow objects
   * @return OB_SUCCESS on success
   */
  int read_all(std::vector<TestRow> &rows)
  {
    int ret = OB_SUCCESS;
    rows.clear();

    TestRow row;
    while (OB_SUCC(read_row(row))) {
      rows.push_back(row);
    }

    // OB_ITER_END is expected, not an error
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    return ret;
  }

  /**
   * @brief Close the CSV file.
   */
  void close()
  {
    if (is_open_) {
      file_.close();
      is_open_ = false;
    }
    column_types_.clear();
    has_type_header_ = false;
    first_line_.clear();
  }

  /**
   * @brief Set column types manually (when no type header in file).
   * @param types Vector of type strings (e.g., {"int", "double", "varchar"})
   */
  void set_column_types(const std::vector<std::string> &types)
  {
    column_types_.clear();
    column_types_.reserve(types.size());
    for (const auto &type_str : types) {
      column_types_.push_back(parse_single_type(type_str));
    }
  }

  /**
   * @brief Get the number of columns.
   */
  size_t get_column_count() const { return column_types_.size(); }

  /**
   * @brief Check if file has type header.
   */
  bool has_type_header() const { return has_type_header_; }

  /**
   * @brief Check if the reader is open.
   */
  bool is_open() const { return is_open_; }

private:
  /**
   * @brief Parse a single CSV line into TestRow.
   * @param line The CSV line to parse
   * @param row Output TestRow object
   * @return OB_SUCCESS on success
   */
  int parse_csv_line(const std::string &line, TestRow &row)
  {
    int ret = OB_SUCCESS;
    row = TestRow();

    std::vector<std::string> fields;
    split_csv_line(line, fields);

    // If no column types set, treat all as strings
    bool use_types = !column_types_.empty();

    for (size_t i = 0; i < fields.size(); ++i) {
      const std::string &field = fields[i];

      // Empty field means NULL
      if (field.empty()) {
        row.push_back(TestValue::null());
        continue;
      }

      // Parse based on type
      CsvColumnType type = CsvColumnType::STRING;
      if (use_types && i < column_types_.size()) {
        type = column_types_[i];
      }

      TestValue value;
      switch (type) {
        case CsvColumnType::INT:
          value = parse_int_value(field);
          break;
        case CsvColumnType::DOUBLE:
          value = parse_double_value(field);
          break;
        case CsvColumnType::VARCHAR:
        case CsvColumnType::STRING:
        default:
          value = TestValue(field);
          break;
      }
      row.push_back(value);
    }

    return ret;
  }

  /**
   * @brief Parse type string like "int, double, varchar(32)".
   * @param type_str Comma-separated type string
   */
  void parse_types(const std::string &type_str)
  {
    column_types_.clear();

    std::istringstream ss(type_str);
    std::string type;
    while (std::getline(ss, type, ',')) {
      // Trim whitespace
      type.erase(0, type.find_first_not_of(" \t\n\r"));
      type.erase(type.find_last_not_of(" \t\n\r") + 1);
      if (!type.empty()) {
        column_types_.push_back(parse_single_type(type));
      }
    }
  }

  /**
   * @brief Parse a single type string to CsvColumnType.
   * @param type_str Type string (e.g., "int", "varchar(32)")
   * @return CsvColumnType enumeration
   */
  CsvColumnType parse_single_type(const std::string &type_str) const
  {
    std::string lower_type = type_str;
    std::transform(lower_type.begin(), lower_type.end(), lower_type.begin(), ::tolower);

    // Check for int types
    if (lower_type == "int" || lower_type == "integer" ||
        lower_type == "bigint" || lower_type == "smallint" ||
        lower_type == "tinyint") {
      return CsvColumnType::INT;
    }

    // Check for double types
    if (lower_type == "double" || lower_type == "float" ||
        lower_type == "decimal" || lower_type.substr(0, 7) == "decimal" ||
        lower_type == "number" || lower_type == "numeric") {
      return CsvColumnType::DOUBLE;
    }

    // Check for varchar/char types
    if (lower_type.substr(0, 7) == "varchar" || lower_type.substr(0, 4) == "char" ||
        lower_type == "string" || lower_type == "text") {
      return CsvColumnType::VARCHAR;
    }

    // Default to string
    return CsvColumnType::STRING;
  }

  /**
   * @brief Split a CSV line into fields.
   * Handles basic CSV format (no quoted fields with commas).
   * @param line Input CSV line
   * @param fields Output vector of field strings
   */
  void split_csv_line(const std::string &line, std::vector<std::string> &fields) const
  {
    fields.clear();
    std::istringstream ss(line);
    std::string field;

    while (std::getline(ss, field, ',')) {
      // Trim whitespace
      field.erase(0, field.find_first_not_of(" \t\n\r"));
      field.erase(field.find_last_not_of(" \t\n\r") + 1);
      fields.push_back(field);
    }

    // Handle trailing comma (empty last field)
    if (!line.empty() && line.back() == ',') {
      fields.push_back("");
    }
  }

  /**
   * @brief Parse an integer value from string.
   * @param field String field
   * @return TestValue with integer value
   */
  TestValue parse_int_value(const std::string &field) const
  {
    try {
      int64_t value = std::stoll(field);
      return TestValue(value);
    } catch (...) {
      // If parsing fails, return as string
      return TestValue(field);
    }
  }

  /**
   * @brief Parse a double value from string.
   * @param field String field
   * @return TestValue with double value
   */
  TestValue parse_double_value(const std::string &field) const
  {
    try {
      double value = std::stod(field);
      return TestValue(value);
    } catch (...) {
      // If parsing fails, return as string
      return TestValue(field);
    }
  }

private:
  std::ifstream file_;
  std::string file_path_;
  std::vector<CsvColumnType> column_types_;
  bool is_open_;
  bool has_type_header_;
  std::string first_line_;  // First non-type-header line if read during open()

  DISALLOW_COPY_AND_ASSIGN(CsvDataReader);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_FILE_DATA_H_
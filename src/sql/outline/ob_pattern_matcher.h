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

#ifndef OCEANBASE_SQL_OUTLINE_OB_PATTERN_MATCHER_H_
#define OCEANBASE_SQL_OUTLINE_OB_PATTERN_MATCHER_H_

#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace sql
{

// Maximum allowed regex length in pattern variables
static const int64_t OB_PATTERN_MAX_REGEX_LEN = 128;
// Default timeout for single regex match (milliseconds)
static const int64_t OB_PATTERN_MATCH_TIMEOUT_MS = 100;

/**
 * @brief ObPatternVarInfo - Parsed variable info from a pattern string
 *
 * A pattern like "orders_${S:[a-z]+}" is decomposed into:
 *   prefix = "orders_", suffix = "", var_name = "S", var_regex = "[a-z]+"
 *
 * Each part (db_name or table_name) can have at most one variable.
 */
struct ObPatternVarInfo
{
  ObPatternVarInfo()
    : prefix_(),
      suffix_(),
      var_name_(),
      var_regex_(),
      has_var_(false)
  {}

  void reset()
  {
    prefix_.reset();
    suffix_.reset();
    var_name_.reset();
    var_regex_.reset();
    has_var_ = false;
  }

  common::ObString prefix_;    // Fixed prefix before variable
  common::ObString suffix_;    // Fixed suffix after variable
  common::ObString var_name_;  // Variable name (e.g., "S")
  common::ObString var_regex_; // Variable regex (e.g., "[a-z]+")
  bool has_var_;               // Whether this pattern contains a variable

  TO_STRING_KV(K_(prefix), K_(suffix), K_(var_name), K_(var_regex), K_(has_var));
};

/**
 * @brief ObPatternMatcher - Pattern matching engine for BINDING_RULE MAP
 *
 * Matches actual table/db names against pattern strings with variable syntax.
 * Supports:
 *   - Fixed prefix/suffix matching
 *   - ICU regex validation with timeout (cross-platform: x86 / ARM / loongarch)
 *   - Cross-table variable consistency checking via shared var_values map
 *
 * Usage:
 *   ObPatternMatcher matcher;
 *   hash::ObHashMap<ObString, ObString> var_values;
 *   var_values.create(16, ...);
 *   ObPatternVarInfo var_info;
 *   ObPatternMatcher::parse_pattern("orders_${S:[a-z]+}", var_info);
 *   // Match table name against parsed pattern
 *   bool matched = false;
 *   matcher.match_with_var_info("orders_bj", var_info, var_values, matched);
 *   // matched == true, var_values["S"] == "bj"
 */
class ObPatternMatcher
{
public:
  ObPatternMatcher() {}
  ~ObPatternMatcher() {}

  /**
   * @brief Parse a pattern string into prefix/suffix/variable components
   *
   * Pattern format: "prefix${VAR:regex}suffix" or "prefix${VAR}suffix"
   * Each pattern part (db or table) has at most one variable.
   *
   * @param pattern         Pattern string (e.g., "orders_${S:[a-z]+}")
   * @param[out] var_info   Parsed variable info
   * @return OB_SUCCESS on success
   */
  static int parse_pattern(const common::ObString &pattern,
                           ObPatternVarInfo &var_info);

  /**
   * @brief Match an actual name against a parsed pattern
   *
   * Steps:
   *   1. Check prefix match
   *   2. Check suffix match
   *   3. Extract variable value (middle substring)
   *   4. Validate via ICU regex (with timeout)
   *   5. Check variable consistency (same var must have same value)
   *
   * @param actual          Actual name to match (e.g., "orders_bj")
   * @param var_info        Parsed pattern info from parse_pattern()
   * @param var_values      Input/output: variable values HashMap for cross-table consistency
   * @param[out] matched    Whether match succeeded
   * @param timeout_ms      Timeout for regex match in milliseconds
   * @return OB_SUCCESS on success, OB_TIMEOUT on regex timeout
   */
  static int match_with_var_info(const common::ObString &actual,
                                 const ObPatternVarInfo &var_info,
                                 common::hash::ObHashMap<common::ObString, common::ObString> &var_values,
                                 bool &matched,
                                 int64_t timeout_ms = OB_PATTERN_MATCH_TIMEOUT_MS);

  /**
   * @brief Match an exact name (no variable, pattern == actual required)
   *
   * Used for tables not in MAP — they require exact table name comparison
   * at their ast_position.
   *
   * @param actual          Actual table name
   * @param expected        Expected exact table name
   * @param[out] matched    Whether exact match succeeded (case-insensitive)
   * @return OB_SUCCESS
   */
  static int match_exact(const common::ObString &actual,
                         const common::ObString &expected,
                         bool &matched);

private:
  /**
   * @brief Check if actual string starts with prefix (case-insensitive)
   */
  static bool match_prefix(const common::ObString &actual,
                           const common::ObString &prefix);

  /**
   * @brief Check if actual string ends with suffix (case-insensitive)
   */
  static bool match_suffix(const common::ObString &actual,
                           const common::ObString &suffix);

  /**
   * @brief Validate a string against an ICU regex with timeout (full-string match)
   *
   * uregex_matches(re, 0) requires the regex to match the entire input — a
   * full-string match equivalent to ^(regex)$. Case-insensitive in Oracle mode
   * (identifier folding), case-sensitive in MySQL. An invalid persisted regex
   * soft-fails (matched=false) rather than failing the query.
   *
   * @param value           String to validate
   * @param regex           Regex pattern (UTF-8, e.g. "[a-z]+")
   * @param[out] matched    Whether regex matched the whole string
   * @param timeout_ms      Wall-clock timeout in milliseconds
   * @return OB_SUCCESS on success, OB_TIMEOUT on timeout,
   *         OB_ERR_REGEXP_ERROR on regex engine failure
   */
  static int match_regex_icu(const common::ObString &value,
                             const common::ObString &regex,
                             bool &matched,
                             int64_t timeout_ms);

  /**
   * @brief Check variable consistency: if var already has a value, new value must match
   *
   * @param var_name        Variable name
   * @param var_value       New value to check/set
   * @param var_values      HashMap of existing variable values
   * @param[out] consistent Whether values are consistent
   * @return OB_SUCCESS on success
   */
  static int check_var_consistency(const common::ObString &var_name,
                                   const common::ObString &var_value,
                                   common::hash::ObHashMap<common::ObString, common::ObString> &var_values,
                                   bool &consistent);

  DISALLOW_COPY_AND_ASSIGN(ObPatternMatcher);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OUTLINE_OB_PATTERN_MATCHER_H_

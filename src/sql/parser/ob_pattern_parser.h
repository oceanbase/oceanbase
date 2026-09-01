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

#ifndef OCEANBASE_SRC_SQL_PARSER_OB_PATTERN_PARSER_H_
#define OCEANBASE_SRC_SQL_PARSER_OB_PATTERN_PARSER_H_

#include "lib/ob_errno.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ddl/ob_outline_binding_rule.h"

namespace oceanbase
{
namespace sql
{

// Maximum regex length allowed in pattern
static const int64_t MAX_REGEX_LENGTH = 128;

// Maximum variable name length (mirrors SQL identifier limits used elsewhere)
static const int64_t MAX_VAR_NAME_LEN = 64;

// Parse result for a single variable in pattern string
struct PatternVarInfo
{
  common::ObString var_name_;        // variable name (e.g., "S", "D")
  common::ObString var_regex_;       // variable regex (e.g., "[a-z]+", "[0-9]+"), empty if ${var}
  int64_t start_pos_;       // start position of variable in pattern string
  int64_t end_pos_;          // end position of variable in pattern string (exclusive)
  bool is_defined_;          // true if variable is defined with regex (${var:regex})

  PatternVarInfo() : start_pos_(0), end_pos_(0), is_defined_(false) {}

  void reset() {
    var_name_.reset();
    var_regex_.reset();
    start_pos_ = 0;
    end_pos_ = 0;
    is_defined_ = false;
  }
};

// Parse result for a pattern string (db pattern or table pattern)
struct PatternParseResult
{
  common::ObString fixed_prefix_;    // fixed prefix before the variable
  common::ObString fixed_suffix_;    // fixed suffix after the variable
  PatternVarInfo var_info_;  // variable info (only one variable allowed per pattern)
  common::ObString regex_error_;     // ICU error name (u_errorName) when regex compilation fails
                                     // (parser sets ret = OB_ERR_REGEXP_ERROR; resolver surfaces this string)

  PatternParseResult() {}

  void reset() {
    fixed_prefix_.reset();
    fixed_suffix_.reset();
    var_info_.reset();
    regex_error_.reset();
  }

  bool has_variable() const {
    return var_info_.var_name_.length() > 0;
  }
};

// MAP item parse result
struct MapItemParseResult
{
  common::ObString left_db_name_;       // left side db name (empty if only table name)
  common::ObString left_table_name_;    // left side table name
  common::ObString right_pattern_;      // right side pattern string (with ${var} or ${var:regex})
  PatternParseResult db_pattern_result_;   // parsed db pattern
  PatternParseResult table_pattern_result_; // parsed table pattern

  MapItemParseResult() {}

  void reset() {
    left_db_name_.reset();
    left_table_name_.reset();
    right_pattern_.reset();
    db_pattern_result_.reset();
    table_pattern_result_.reset();
  }

  TO_STRING_KV(K_(left_db_name), K_(left_table_name), K_(right_pattern));
};

// BINDING_RULE parse result for validation
struct BindingRuleParseResult
{
  int64_t scope_;  // 0 = DATABASE, 1 = TENANT
  ObArray<MapItemParseResult> map_items_;
  ObArenaAllocator allocator_;  // for storing strings

  BindingRuleParseResult() : scope_(0), allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "BindRuleParse")) {}

  void reset() {
    scope_ = 0;
    map_items_.reset();
    allocator_.reset();
  }
};

class ObPatternParser
{
public:
  ObPatternParser(common::ObIAllocator &allocator) : allocator_(allocator) {}
  ~ObPatternParser() {}

  /** Trial-compile a regex with ICU (the runtime matcher's engine); fails CREATE on syntax error. */
  static bool icu_regex_is_valid(const common::ObString &regex,
                                 common::ObString &err_msg,
                                 common::ObIAllocator &allocator);

  /**
   * Parse a pattern string like "orders_${S}" or "orders_${S:[a-z]+}"
   * @param pattern_str the pattern string to parse
   * @param result parse result
   * @return OB_SUCCESS on success, OB_ERR_PARSER_SYNTAX on error
   */
  int parse_pattern_string(const common::ObString &pattern_str, PatternParseResult &result);

  /**
   * Parse a map item like "orders_sh TO orders_${S:[a-z]+}"
   * @param left_str left side (table name or db.table)
   * @param right_str right side pattern string
   * @param result parse result
   * @return OB_SUCCESS on success, OB_ERR_PARSER_SYNTAX on error
   */
  int parse_map_item(const common::ObString &left_str,
                     const common::ObString &right_str,
                     MapItemParseResult &result);

  /**
   * Validate the complete BINDING_RULE
   * @param parse_result binding rule parse result
   * @return OB_SUCCESS on success, OB_ERR_PARSER_SYNTAX on error
   */
  int validate_binding_rule(BindingRuleParseResult &parse_result);

  /**
   * Check if left and right structures are consistent
   * (both are table only, or both are db.table)
   */
  bool is_structure_consistent(const MapItemParseResult &item) const;

private:
  /**
   * Parse ${var} or ${var:regex} syntax from pattern string
   * @param pattern the pattern string containing ${...}
   * @param result parse result
   * @return OB_SUCCESS on success, OB_ERR_PARSER_SYNTAX on error
   */
  int parse_variable_segment(const common::ObString &pattern, PatternParseResult &result);

  /**
   * Check if variable name is valid (starts with letter, followed by letter/digit/underscore)
   */
  bool is_valid_var_name(const common::ObString &var_name) const;

  /**
   * Check if a character is a valid start char for variable name (letter)
   */
  bool is_valid_var_start_char(char c) const;

  /**
   * Check if a character is a valid char for variable name (letter/digit/underscore)
   */
  bool is_valid_var_char(char c) const;

  /**
   * Check if regex length exceeds limit
   */
  bool is_regex_too_long(const common::ObString &regex) const;

  /**
   * Validate variable reference: must be defined before referenced
   */
  int validate_var_references(BindingRuleParseResult &parse_result);

  /**
   * Validate no variable redefinition (same var name with different regex)
   */
  int validate_no_var_redefinition(BindingRuleParseResult &parse_result);

  /**
   * Validate MAP with db.table requires SCOPE=TENANT
   */
  int validate_scope_for_db_table(BindingRuleParseResult &parse_result);

  common::ObIAllocator &allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPatternParser);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SRC_SQL_PARSER_OB_PATTERN_PARSER_H_

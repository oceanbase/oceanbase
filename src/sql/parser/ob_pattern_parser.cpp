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

#define USING_LOG_PREFIX SQL_PARSER
#include <ctype.h>
#include <regex.h>
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_string_buffer.h"
#include "sql/parser/ob_pattern_parser.h"
#include "sql/resolver/ddl/ob_outline_binding_rule.h"

using oceanbase::common::ObString;

namespace oceanbase
{
namespace sql
{

// Pattern parser state machine.
//
// The pattern body is split into three regions:
//   LITERAL  — text outside ${...}
//   VAR_HEAD — identifier between '${' and ':' (or '}')
//   REGEX    — body between ':' and the terminating '}'
//
// Each region enforces a distinct character grammar:
//
//   LITERAL accepts plain text plus the four backslash escapes
//     \$  → literal '$'
//     \{  → literal '{'
//     \}  → literal '}'
//     \\  → literal '\'
//   and rejects POSIX regex metacharacters (. + * ? [ ] ( ) | ^ $), bare '$'
//   not followed by '{', and bare '{' / '}'. Rejection prevents the silent
//   "dead outline" class of bugs and forces users to wrap
//   pattern-matching segments in ${VAR:regex}.
//
//   VAR_HEAD accepts [A-Za-z][A-Za-z0-9_]* up to MAX_VAR_NAME_LEN,
//   with no embedded whitespace. The terminator must be ':' or '}';
//   anything else surfaces a friendly error rather than a generic 1149.
//
//   REGEX is parsed with brace balance counting and [...] character-class
//   isolation so POSIX repetition '{n,m}' and bracket-expression contents
//   no longer truncate the variable definition. '${' nested inside
//   the regex body is rejected with a dedicated message. Empty
//   regex (':}') is rejected before the resolver's reference-check sees it.
//   Trial regcomp at CREATE time still surfaces invalid syntax
//   via OB_ERR_REGEXP_ERROR.
//
// Each call processes at most one ${...} occurrence per side; a second
// occurrence is rejected with OB_NOT_SUPPORTED so that the single-variable
// PatternParseResult schema cannot be silently violated.
//
// Cross-item variable-redefinition is enforced one level up, in the
// resolver, where all map items are visible.

namespace
{

inline bool is_posix_metachar(char c)
{
  // Characters that have special meaning in POSIX EREs. Literal '{' / '}'
  // are handled separately so we can attach a more targeted error message.
  bool is_meta = false;
  switch (c) {
    case '.': case '+': case '*': case '?':
    case '[': case ']': case '(': case ')':
    case '|': case '^': case '$':
      is_meta = true;
      break;
    default:
      break;
  }
  return is_meta;
}

} // anonymous namespace

int ObPatternParser::parse_pattern_string(const ObString &pattern_str, PatternParseResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();

  if (pattern_str.empty()) {
    ret = OB_ERR_PARSER_SYNTAX;
    SQL_LOG(WARN, "empty pattern string");
  }

  // pattern_str.ptr() is safe even when empty; the while loop below is
  // gated by OB_SUCC(ret) so it will not execute in the empty case.
  const char *pattern = pattern_str.ptr();
  const int64_t length = pattern_str.length();

  // Accumulated literal text with escapes applied. The variable's logical
  // position within the literal stream is recorded as var_split_pos so that
  // fixed_prefix_ / fixed_suffix_ can be sliced after the scan completes.
  common::ObStringBuffer lit_buf(&allocator_);
  int64_t var_split_pos = -1;       // index in lit_buf where the variable would sit
  int64_t var_count = 0;
  int64_t var_start_in_src = -1;    // index of '$' in raw pattern
  int64_t var_end_in_src = -1;      // index of terminating '}' in raw pattern (inclusive)

  ObString tmp_var_name;
  ObString tmp_var_regex;
  bool var_is_defined = false;

  int64_t i = 0;
  while (OB_SUCC(ret) && i < length) {
    const char c = pattern[i];

    // --- LITERAL: backslash escape ---
    if (c == '\\') {
      if (i + 1 >= length) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "trailing backslash in BINDING_RULE pattern is");
        break;
      }
      const char nx = pattern[i + 1];
      if (nx == '$' || nx == '{' || nx == '}' || nx == '\\') {
        if (OB_FAIL(lit_buf.append(&nx, 1))) {
          break;
        }
        i += 2;
        continue;
      }
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "unsupported backslash escape in BINDING_RULE pattern (only \\$ \\{ \\} \\\\) is");
      break;
    }

    // --- LITERAL: variable start ---
    if (c == '$') {
      int64_t bp = i + 1;
      // Tolerate whitespace between $ and { to keep parity with the legacy parser,
      // but the bp == '{' check that follows still requires the '{' to be there.
      while (bp < length && isspace(pattern[bp])) {
        ++bp;
      }
      if (bp >= length || pattern[bp] != '{') {
        // bare '$' not followed by '{' — reject to catch
        // typo'd pattern expressions early at CREATE time
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "bare '$' outside ${} in BINDING_RULE pattern (use \\$ to escape) is");
        break;
      }

      // at most one ${} per side.
      if (var_count >= 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE allows at most one variable per db or table pattern is");
        break;
      }
      var_count = 1;
      var_start_in_src = i;
      var_split_pos = lit_buf.length();

      // --- VAR_HEAD: identifier ---
      int64_t k = bp + 1;
      int64_t name_start = k;
      while (k < length) {
        const char nc = pattern[k];
        if (nc == ':' || nc == '}') {
          break;
        }
        if (nc == '$' && k + 1 < length && pattern[k + 1] == '{') {
          // nested ${
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "BINDING_RULE nested ${} in pattern is");
          break;
        }
        if (isspace(nc)) {
          // whitespace inside variable name
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "BINDING_RULE variable name contains whitespace is");
          break;
        }
        ++k;
      }
      if (OB_FAIL(ret)) {
        break;
      }
      if (k >= length) {
        // unclosed ${ before ':' or '}'
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE unclosed ${ in pattern is");
        break;
      }
      const int64_t name_len = k - name_start;
      if (name_len == 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE variable name must not be empty is");
        break;
      }
      if (name_len > MAX_VAR_NAME_LEN) {
        // variable name length cap
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE variable name length exceeds 64 characters is");
        break;
      }
      ObString name_str(name_len, pattern + name_start);
      if (OB_FAIL(ob_write_string(allocator_, name_str, tmp_var_name))) {
        SQL_LOG(WARN, "fail to deep copy var name", K(ret));
        break;
      }
      if (!is_valid_var_name(tmp_var_name)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE invalid variable name (must be [A-Za-z_][A-Za-z0-9_]*) is");
        break;
      }

      if (pattern[k] == '}') {
        // ${VAR}: reference form
        var_is_defined = false;
        var_end_in_src = k;
        i = k + 1;
        continue;
      }

      // pattern[k] == ':' — parse regex body
      const int64_t regex_start = k + 1;
      int64_t r = regex_start;
      int brace_depth = 0;
      bool in_bracket = false;
      while (r < length) {
        const char rc = pattern[r];
        if (rc == '\\' && r + 1 < length) {
          // Regex body forwards backslash escapes to regcomp unchanged; we only
          // need to skip them for our own brace/bracket bookkeeping.
          r += 2;
          continue;
        }
        if (in_bracket) {
          if (rc == ']') {
            in_bracket = false;
          }
          ++r;
          continue;
        }
        if (rc == '[') {
          in_bracket = true;
          ++r;
          continue;
        }
        if (rc == '$' && r + 1 < length && pattern[r + 1] == '{') {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "BINDING_RULE nested ${} in pattern is");
          break;
        }
        if (rc == '{') {
          ++brace_depth;
          ++r;
          continue;
        }
        if (rc == '}') {
          if (brace_depth == 0) {
            break;
          }
          --brace_depth;
          ++r;
          continue;
        }
        ++r;
      }
      if (OB_FAIL(ret)) {
        break;
      }
      if (r >= length) {
        // regex body without terminator
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE unclosed ${ in pattern is");
        break;
      }
      const int64_t regex_len = r - regex_start;
      if (regex_len == 0) {
        // empty regex
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE variable regex must not be empty is");
        break;
      }
      ObString regex_str_view(regex_len, pattern + regex_start);
      if (is_regex_too_long(regex_str_view)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE regex length exceeds 128 characters is");
        break;
      }
      if (OB_FAIL(ob_write_string(allocator_, regex_str_view, tmp_var_regex))) {
        SQL_LOG(WARN, "fail to deep copy var regex", K(ret));
        break;
      }
      var_is_defined = true;
      var_end_in_src = r;
      i = r + 1;
      continue;
    }

    // --- LITERAL: POSIX metachar / stray brace rejection ---
    if (is_posix_metachar(c)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "POSIX regex metacharacter outside ${} in BINDING_RULE pattern is");
      break;
    }
    if (c == '{' || c == '}') {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "literal '{' or '}' outside ${} in BINDING_RULE pattern (use \\{ or \\}) is");
      break;
    }

    // Plain literal byte — append to buffer.
    if (OB_FAIL(lit_buf.append(&c, 1))) {
      break;
    }
    ++i;
  }

  // ---- Stacked quantifier check on the regex body ----
  // glibc's regcomp silently collapses '++' / '*+' / '+*' / '+?' / '?+' / etc.
  // into a single quantifier. Surface them at CREATE so the outline author
  // notices instead of getting a silently-equivalent regex.
  if (OB_SUCC(ret) && !tmp_var_regex.empty()) {
    bool in_bracket = false;
    bool prev_is_quant = false;
    bool stacked_quant = false;
    for (int64_t j = 0; j < tmp_var_regex.length(); ++j) {
      char c = tmp_var_regex[j];
      if (in_bracket) {
        if (c == ']') {
          in_bracket = false;
        }
        prev_is_quant = false;
        continue;
      }
      if (c == '\\' && j + 1 < tmp_var_regex.length()) {
        ++j;
        prev_is_quant = false;
        continue;
      }
      if (c == '[') {
        in_bracket = true;
        prev_is_quant = false;
        continue;
      }
      bool is_quant = (c == '*' || c == '+' || c == '?');
      if (is_quant && prev_is_quant) {
        stacked_quant = true;
        break;
      }
      prev_is_quant = is_quant;
    }
    if (stacked_quant) {
      const char *msg = "Stacked quantifiers in BINDING_RULE regex";
      ObString tmp_err(msg);
      int tmp_ret = ob_write_string(allocator_, tmp_err, result.regex_error_, true /*c_style*/);
      if (OB_SUCCESS != tmp_ret) {
        SQL_LOG(WARN, "fail to copy regex error msg", K(tmp_ret));
      }
      ret = OB_ERR_REGEXP_ERROR;
    }
  }

  // ---- Trial regcomp ----
  // Surface invalid POSIX regex at CREATE so a dead outline never lands.
  if (OB_SUCC(ret) && !tmp_var_regex.empty()) {
    char regex_buf[MAX_REGEX_LENGTH + 1];
    MEMCPY(regex_buf, tmp_var_regex.ptr(), tmp_var_regex.length());
    regex_buf[tmp_var_regex.length()] = '\0';
    regex_t compiled;
    int reg_ret = regcomp(&compiled, regex_buf, REG_EXTENDED | REG_NOSUB);
    if (0 != reg_ret) {
      char err_buf[256];
      size_t err_len = regerror(reg_ret, &compiled, err_buf, sizeof(err_buf));
      if (err_len >= sizeof(err_buf)) {
        err_buf[sizeof(err_buf) - 1] = '\0';
      }
      ObString tmp_err(err_buf);
      int tmp_ret = ob_write_string(allocator_, tmp_err, result.regex_error_, true /*c_style*/);
      if (OB_SUCCESS != tmp_ret) {
        SQL_LOG(WARN, "fail to copy regex error msg", K(tmp_ret));
      }
      // Per POSIX, regfree must NOT be called after a failed regcomp.
      ret = OB_ERR_REGEXP_ERROR;
    } else {
      regfree(&compiled);
    }
  }

  // ---- Slice cleaned literal into fixed_prefix_ / fixed_suffix_ ----
  if (OB_SUCC(ret)) {
    if (var_count == 0) {
      if (lit_buf.length() > 0) {
        ObString tmp_prefix(lit_buf.length(), lit_buf.ptr());
        if (OB_FAIL(ob_write_string(allocator_, tmp_prefix, result.fixed_prefix_))) {
          SQL_LOG(WARN, "fail to deep copy fixed_prefix", K(ret));
        }
      }
    } else {
      if (var_split_pos > 0) {
        ObString tmp_prefix(var_split_pos, lit_buf.ptr());
        if (OB_FAIL(ob_write_string(allocator_, tmp_prefix, result.fixed_prefix_))) {
          SQL_LOG(WARN, "fail to deep copy fixed_prefix", K(ret));
        }
      }
      if (OB_SUCC(ret) && static_cast<int64_t>(lit_buf.length()) > var_split_pos) {
        ObString tmp_suffix(static_cast<int32_t>(lit_buf.length() - var_split_pos), lit_buf.ptr() + var_split_pos);
        if (OB_FAIL(ob_write_string(allocator_, tmp_suffix, result.fixed_suffix_))) {
          SQL_LOG(WARN, "fail to deep copy fixed_suffix", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        result.var_info_.var_name_ = tmp_var_name;
        result.var_info_.var_regex_ = tmp_var_regex;
        result.var_info_.is_defined_ = var_is_defined;
        result.var_info_.start_pos_ = var_start_in_src;
        result.var_info_.end_pos_ = var_end_in_src + 1;
      }
    }
  }

  return ret;
}

int ObPatternParser::parse_map_item(const ObString &left_str,
                                     const ObString &right_str,
                                     MapItemParseResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();

  if (left_str.empty() || right_str.empty()) {
    ret = OB_ERR_PARSER_SYNTAX;
    SQL_LOG(WARN, "empty left or right in map item");
  } else {
    // Parse left side: either table_name or db.table_name
    const char *left = left_str.ptr();
    int64_t len = left_str.length();
    const char *dot_pos = left_str.find('.');

    if (dot_pos != NULL) {
      // db.table form
      result.left_db_name_ = ObString(dot_pos - left, left);
      result.left_table_name_ = ObString(len - (dot_pos - left) - 1, dot_pos + 1);
    } else {
      // table only form
      result.left_table_name_ = left_str;
    }

    // Parse right side pattern string
    result.right_pattern_ = right_str;
    if (result.left_db_name_.empty()) {
      ret = parse_pattern_string(right_str, result.table_pattern_result_);
    } else {
      // Right side should contain db pattern (with . separator)
      const char *right = right_str.ptr();
      int64_t right_len = right_str.length();
      const char *dot_in_right = right_str.find('.');

      if (dot_in_right == NULL) {
        // Right side doesn't have db part, structure inconsistent
        ret = OB_ERR_PARSER_SYNTAX;
        SQL_LOG(WARN, "right side should have db pattern when left is db.table",
                K(left_str), K(right_str));
      } else {
        // Parse db pattern from right side
        ObString db_pattern_str(dot_in_right - right, right);
        ObString tbl_pattern_str(right_len - (dot_in_right - right) - 1, dot_in_right + 1);
        ret = parse_pattern_string(db_pattern_str, result.db_pattern_result_);
        if (OB_SUCC(ret)) {
          ret = parse_pattern_string(tbl_pattern_str, result.table_pattern_result_);
        }
      }
    }
  }

  return ret;
}

bool ObPatternParser::is_structure_consistent(const MapItemParseResult &item) const
{
  bool consistent = true;
  bool left_has_db = !item.left_db_name_.empty();
  bool right_has_db = item.db_pattern_result_.has_variable() ||
                      (item.db_pattern_result_.fixed_prefix_.length() > 0);
  // When left has no db, parse_map_item doesn't parse db part from right,
  // so right_has_db would be false even if right contains db.table structure.
  // Scan for '.' outside ${...} blocks to detect this case.
  if (!left_has_db && !right_has_db) {
    const char *right = item.right_pattern_.ptr();
    int64_t len = item.right_pattern_.length();
    for (int64_t i = 0; i < len; ++i) {
      if (right[i] == '$') {
        // Skip ${...} block (with possible whitespace between $ and {)
        int64_t bp = i + 1;
        while (bp < len && isspace(right[bp])) { bp++; }
        if (bp < len && right[bp] == '{') {
          i = bp + 1;
          while (i < len && right[i] != '}') {
            i++;
          }
        }
      } else if (right[i] == '.') {
        right_has_db = true;
        break;
      }
    }
  }
  consistent = (left_has_db == right_has_db);
  return consistent;
}

int ObPatternParser::validate_binding_rule(BindingRuleParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  // Validate all map items
  for (int64_t i = 0; OB_SUCC(ret) && i < parse_result.map_items_.count(); ++i) {
    MapItemParseResult &item = parse_result.map_items_[i];

    // Check structure consistency
    if (!is_structure_consistent(item)) {
      ret = OB_ERR_PARSER_SYNTAX;
      SQL_LOG(WARN, "left and right structure inconsistent",
              K(item.left_db_name_), K(item.left_table_name_),
              K(item.right_pattern_));
    }
  }

  // Validate variable references (defined before used)
  if (OB_SUCC(ret)) {
    ret = validate_var_references(parse_result);
  }

  // Validate no variable redefinition
  if (OB_SUCC(ret)) {
    ret = validate_no_var_redefinition(parse_result);
  }

  // Validate SCOPE=TENANT for db.table MAP
  if (OB_SUCC(ret)) {
    ret = validate_scope_for_db_table(parse_result);
  }

  return ret;
}

int ObPatternParser::validate_var_references(BindingRuleParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<ObString, bool> defined_vars;

  if (OB_FAIL(defined_vars.create(32, ObMemAttr(OB_SERVER_TENANT_ID, "VarRefCheck")))) {
    SQL_LOG(WARN, "failed to create hash map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < parse_result.map_items_.count(); ++i) {
    MapItemParseResult &item = parse_result.map_items_[i];
    const PatternParseResult *pattern_results[] = {
      &item.db_pattern_result_,
      &item.table_pattern_result_,
    };
    for (int64_t j = 0; OB_SUCC(ret) && j < ARRAYSIZEOF(pattern_results); ++j) {
      const PatternParseResult &pattern_result = *pattern_results[j];
      if (pattern_result.has_variable()) {
        const ObString &var_name = pattern_result.var_info_.var_name_;
        if (pattern_result.var_info_.is_defined_) {
          if (OB_FAIL(defined_vars.set_refactored(var_name, true))) {
            SQL_LOG(WARN, "failed to set defined var", K(var_name), K(ret));
          }
        } else {
          bool dummy_val = false;
          int hash_ret = defined_vars.get_refactored(var_name, dummy_val);
          if (OB_HASH_NOT_EXIST == hash_ret) {
            ret = OB_ERR_PARSER_SYNTAX;
            SQL_LOG(WARN, "variable not defined before reference", K(var_name));
          } else if (OB_SUCCESS != hash_ret) {
            ret = hash_ret;
            SQL_LOG(WARN, "get_refactored failed", K(var_name), K(hash_ret));
          }
        }
      }
    }
  }

  defined_vars.destroy();
  return ret;
}

int ObPatternParser::validate_no_var_redefinition(BindingRuleParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<ObString, ObString> var_regex_map;

  if (OB_FAIL(var_regex_map.create(32, ObMemAttr(OB_SERVER_TENANT_ID, "VarRedefCheck")))) {
    SQL_LOG(WARN, "failed to create hash map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < parse_result.map_items_.count(); ++i) {
    MapItemParseResult &item = parse_result.map_items_[i];
    const PatternParseResult *pattern_results[] = {
      &item.db_pattern_result_,
      &item.table_pattern_result_,
    };
    for (int64_t j = 0; OB_SUCC(ret) && j < ARRAYSIZEOF(pattern_results); ++j) {
      const PatternParseResult &pattern_result = *pattern_results[j];
      if (pattern_result.has_variable() && pattern_result.var_info_.is_defined_) {
        const ObString &var_name = pattern_result.var_info_.var_name_;
        const ObString &regex = pattern_result.var_info_.var_regex_;
        ObString existing_regex;
        int hash_ret = var_regex_map.get_refactored(var_name, existing_regex);
        if (OB_SUCCESS == hash_ret) {
          if (existing_regex != regex) {
            ret = OB_ERR_PARSER_SYNTAX;
            SQL_LOG(WARN, "variable redefined with different regex",
                    K(var_name), K(regex), K(existing_regex));
          }
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          if (OB_FAIL(var_regex_map.set_refactored(var_name, regex))) {
            SQL_LOG(WARN, "failed to set var regex", K(var_name), K(ret));
          }
        } else {
          ret = hash_ret;
          SQL_LOG(WARN, "get_refactored failed", K(var_name), K(hash_ret));
        }
      }
    }
  }

  var_regex_map.destroy();
  return ret;
}

int ObPatternParser::validate_scope_for_db_table(BindingRuleParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  // If SCOPE != TENANT (i.e., scope == 0 for DATABASE), MAP cannot contain db.table
  if (parse_result.scope_ == 0) {  // DATABASE scope
    for (int64_t i = 0; OB_SUCC(ret) && i < parse_result.map_items_.count(); ++i) {
      MapItemParseResult &item = parse_result.map_items_[i];
      if (!item.left_db_name_.empty()) {
        ret = OB_ERR_PARSER_SYNTAX;
        SQL_LOG(WARN, "MAP with db.table requires SCOPE=TENANT");
      }
    }
  }

  return ret;
}

bool ObPatternParser::is_valid_var_name(const ObString &var_name) const
{
  bool valid = false;
  if (var_name.length() > 0 && is_valid_var_start_char(var_name.ptr()[0])) {
    valid = true;
    for (int64_t i = 1; i < var_name.length() && valid; ++i) {
      if (!is_valid_var_char(var_name.ptr()[i])) {
        valid = false;
      }
    }
  }
  return valid;
}

bool ObPatternParser::is_valid_var_start_char(char c) const
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

bool ObPatternParser::is_valid_var_char(char c) const
{
  return is_valid_var_start_char(c) || (c >= '0' && c <= '9') || c == '_';
}

bool ObPatternParser::is_regex_too_long(const ObString &regex) const
{
  return regex.length() > MAX_REGEX_LENGTH;
}

} // namespace sql
} // namespace oceanbase

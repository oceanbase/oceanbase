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

#define USING_LOG_PREFIX SQL

#include "sql/outline/ob_pattern_matcher.h"
#include "lib/ob_define.h"
#include "lib/worker.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include <icu/i18n/unicode/uregex.h>
#include <icu/common/unicode/ustring.h>
#include <time.h>
#include <ctype.h>
#include <string.h>

namespace oceanbase
{
namespace sql
{

using namespace common;

// ==================== parse_pattern ====================

int ObPatternMatcher::parse_pattern(const ObString &pattern,
                                    ObPatternVarInfo &var_info)
{
  int ret = OB_SUCCESS;
  var_info.reset();

  if (pattern.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty pattern", K(ret));
  } else {
    const char *data = pattern.ptr();
    const int64_t len = pattern.length();

    // Find the start of ${...} variable syntax
    int64_t var_start = -1; // position of '$'
    int64_t var_end = -1;   // position after '}'
    int64_t brace_pos = -1; // position of '{'
    for (int64_t i = 0; i < len - 1; ++i) {
      if (data[i] == '$') {
        // Skip whitespace between $ and {
        int64_t bp = i + 1;
        while (bp < len && isspace(data[bp])) { bp++; }
        if (bp < len && data[bp] == '{') {
          var_start = i;
          brace_pos = bp;
          // Find matching '}'
          for (int64_t j = bp + 1; j < len; ++j) {
            if (data[j] == '}') {
              var_end = j + 1;
              break;
            }
          }
          break; // only one variable per pattern part
        }
      }
    }

    if (var_start < 0) {
      // No variable found — this is a fixed pattern (exact match)
      var_info.prefix_ = pattern;
      var_info.has_var_ = false;
    } else if (var_end < 0) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("unmatched '${' in pattern, missing '}'", K(ret), K(pattern));
    } else {
      var_info.has_var_ = true;

      // Extract prefix: everything before '${'
      if (var_start > 0) {
        var_info.prefix_.assign_ptr(data, static_cast<ObString::obstr_size_t>(var_start));
      }

      // Extract suffix: everything after '}'
      if (var_end < len) {
        var_info.suffix_.assign_ptr(data + var_end,
                                    static_cast<ObString::obstr_size_t>(len - var_end));
      }

      // Parse variable content between ${ and }
      // Format: ${VAR_NAME} or ${VAR_NAME:regex}
      const char *var_content = data + brace_pos + 1; // skip "{"
      int64_t var_content_len = var_end - brace_pos - 2; // exclude "{" and "}"

      // Strip leading/trailing spaces from var_content
      while (var_content_len > 0 && *var_content == ' ') { var_content++; var_content_len--; }
      while (var_content_len > 0 && var_content[var_content_len - 1] == ' ') { var_content_len--; }

      if (var_content_len <= 0) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("empty variable name in pattern", K(ret), K(pattern));
      } else {
        // Find ':' separator between var_name and regex
        int64_t colon_pos = -1;
        for (int64_t i = 0; i < var_content_len; ++i) {
          if (var_content[i] == ':') {
            colon_pos = i;
            break;
          }
        }

        if (colon_pos < 0) {
          // No regex — reference-only variable: ${VAR}
          // Strip trailing spaces from var_name
          int64_t name_len = var_content_len;
          while (name_len > 0 && var_content[name_len - 1] == ' ') { name_len--; }
          var_info.var_name_.assign_ptr(var_content,
                                        static_cast<ObString::obstr_size_t>(name_len));
        } else if (0 == colon_pos) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("empty variable name before ':'", K(ret), K(pattern));
        } else {
          // Has regex: ${VAR:regex}
          // Strip trailing spaces from var_name
          const char *name_start = var_content;
          int64_t name_len = colon_pos;
          while (name_len > 0 && name_start[name_len - 1] == ' ') { name_len--; }
          var_info.var_name_.assign_ptr(name_start,
                                        static_cast<ObString::obstr_size_t>(name_len));

          // Strip leading/trailing spaces from regex
          const char *regex_start = var_content + colon_pos + 1;
          int64_t regex_len = var_content_len - colon_pos - 1;
          while (regex_len > 0 && *regex_start == ' ') { regex_start++; regex_len--; }
          while (regex_len > 0 && regex_start[regex_len - 1] == ' ') { regex_len--; }

          if (regex_len <= 0) {
            ret = OB_ERR_PARSER_SYNTAX;
            LOG_WARN("empty regex after ':'", K(ret), K(pattern));
          } else if (regex_len > OB_PATTERN_MAX_REGEX_LEN) {
            ret = OB_ERR_PARSER_SYNTAX;
            LOG_WARN("regex too long", K(ret), K(regex_len),
                     K(OB_PATTERN_MAX_REGEX_LEN), K(pattern));
          } else {
            var_info.var_regex_.assign_ptr(regex_start,
                                           static_cast<ObString::obstr_size_t>(regex_len));
          }
        }
      }
    }
  }

  return ret;
}

// ==================== match helpers ====================

bool ObPatternMatcher::match_prefix(const ObString &actual,
                                    const ObString &prefix)
{
  if (prefix.empty()) {
    return true;
  }
  if (actual.length() < prefix.length()) {
    return false;
  }
  return 0 == strncasecmp(actual.ptr(), prefix.ptr(), prefix.length());
}

bool ObPatternMatcher::match_suffix(const ObString &actual,
                                    const ObString &suffix)
{
  if (suffix.empty()) {
    return true;
  }
  if (actual.length() < suffix.length()) {
    return false;
  }
  const char *actual_tail = actual.ptr() + actual.length() - suffix.length();
  return 0 == strncasecmp(actual_tail, suffix.ptr(), suffix.length());
}

int ObPatternMatcher::match_regex_icu(const ObString &value,
                                      const ObString &regex,
                                      bool &matched,
                                      int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  matched = false;

  if (value.empty() || regex.empty()) {
    // Empty value cannot match any regex; empty regex is a no-op
    matched = false;
  } else if (regex.length() > OB_PATTERN_MAX_REGEX_LEN) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("regex exceeds max length", K(ret), K(regex.length()));
  } else {
    // UChar count <= UTF-8 byte count, so OB_PATTERN_MAX_REGEX_LEN+1 suffices.
    UChar u_pat[OB_PATTERN_MAX_REGEX_LEN + 1];
    int32_t u_pat_len = 0;
    UErrorCode status = U_ZERO_ERROR;
    u_strFromUTF8(u_pat, ARRAYSIZEOF(u_pat), &u_pat_len,
                  regex.ptr(), static_cast<int32_t>(regex.length()), &status);
    if (U_FAILURE(status)) {
      matched = false;
      LOG_WARN("u_strFromUTF8 failed for regex, soft-fail",
               K(regex), "icu_status", u_errorName(status));
    } else {
      // Oracle mode: case-insensitive (identifier folding); MySQL: case-sensitive.
      const uint32_t re_flags = lib::is_oracle_mode() ? UREGEX_CASE_INSENSITIVE : 0;
      URegularExpression *regexp = uregex_open(u_pat, u_pat_len, re_flags, NULL, &status);
      if (U_FAILURE(status) || OB_ISNULL(regexp)) {
        matched = false;
        LOG_WARN("uregex_open failed on persisted regex, soft-fail",
                 K(regex), "icu_status", u_errorName(status));
        if (OB_NOT_NULL(regexp)) { uregex_close(regexp); }
      } else {
        // Cap at OB_MAX_TABLE_NAME_LENGTH (former POSIX bound); UChar count <= byte count.
        UChar u_val[OB_MAX_TABLE_NAME_LENGTH + 1];
        int32_t u_val_len = 0;
        int64_t copy_len = MIN(value.length(), OB_MAX_TABLE_NAME_LENGTH);
        u_strFromUTF8(u_val, ARRAYSIZEOF(u_val), &u_val_len,
                      value.ptr(), static_cast<int32_t>(copy_len), &status);
        if (U_FAILURE(status)) {
          matched = false;
          LOG_WARN("u_strFromUTF8 failed for value, soft-fail",
                   K(value), "icu_status", u_errorName(status));
        } else {
          // Timeout covers setText + matches.
          struct timespec ts_start, ts_end;
          clock_gettime(CLOCK_MONOTONIC, &ts_start);

          // uregex_setText borrows u_val; keep it in scope through uregex_matches.
          uregex_setText(regexp, u_val, u_val_len, &status);
          // uregex_matches(re, 0) is a full-string match — no manual ^...$ needed.
          UBool found = (U_SUCCESS(status)) ? uregex_matches(regexp, 0, &status) : false;

          clock_gettime(CLOCK_MONOTONIC, &ts_end);
          int64_t elapsed_ms = (ts_end.tv_sec - ts_start.tv_sec) * 1000
                               + (ts_end.tv_nsec - ts_start.tv_nsec) / 1000000;

          if (elapsed_ms > timeout_ms) {
            ret = OB_TIMEOUT;
            LOG_WARN("regex match timeout", K(ret), K(elapsed_ms), K(timeout_ms),
                     K(regex), K(value));
          } else if (U_FAILURE(status)) {
            ret = OB_ERR_REGEXP_ERROR;
            LOG_WARN("uregex_matches failed", K(ret), K(regex), K(value),
                     "icu_status", u_errorName(status));
          } else {
            matched = found;
          }
        }
        uregex_close(regexp);
      }
    }
  }

  return ret;
}

int ObPatternMatcher::check_var_consistency(const ObString &var_name,
                                            const ObString &var_value,
                                            hash::ObHashMap<ObString, ObString> &var_values,
                                            bool &consistent)
{
  int ret = OB_SUCCESS;
  consistent = true;

  if (var_name.empty()) {
    // No variable to check
  } else {
    ObString existing_value;
    int hash_ret = var_values.get_refactored(var_name, existing_value);
    if (OB_SUCCESS == hash_ret) {
      // Variable already has a value — check consistency (case-insensitive)
      if (0 != var_value.case_compare(existing_value)) {
        consistent = false;
        LOG_TRACE("[OUTLINE] template variable inconsistent",
                  K(var_name), K(existing_value), K(var_value));
      }
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      // First occurrence — set the variable value
      if (OB_FAIL(var_values.set_refactored(var_name, var_value))) {
        LOG_WARN("failed to set variable value in hash map", K(ret), K(var_name), K(var_value));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get variable value from hash map", K(ret), K(var_name));
    }
  }

  return ret;
}

// ==================== main matching interface ====================

int ObPatternMatcher::match_with_var_info(const ObString &actual,
                                          const ObPatternVarInfo &var_info,
                                          hash::ObHashMap<ObString, ObString> &var_values,
                                          bool &matched,
                                          int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  matched = false;

  if (actual.empty()) {
    matched = false;
  } else if (!var_info.has_var_) {
    // No variable — exact match (the entire pattern is in prefix)
    matched = (0 == actual.case_compare(var_info.prefix_));
  } else {
    // Step 1: Check prefix
    if (!match_prefix(actual, var_info.prefix_)) {
      matched = false;
    // Step 2: Check suffix
    } else if (!match_suffix(actual, var_info.suffix_)) {
      matched = false;
    } else {
      // Step 3: Extract variable value (middle part between prefix and suffix)
      int64_t prefix_len = var_info.prefix_.length();
      int64_t suffix_len = var_info.suffix_.length();
      int64_t var_value_len = actual.length() - prefix_len - suffix_len;

      if (var_value_len < 0) {
        // Prefix + suffix longer than actual string — overlap, no match
        matched = false;
      } else if (var_value_len == 0 && !var_info.var_regex_.empty()) {
        // Empty variable value with non-empty regex — typically won't match
        // (e.g., "[a-z]+" requires at least one char)
        matched = false;
      } else {
        ObString var_value(static_cast<ObString::obstr_size_t>(var_value_len),
                           actual.ptr() + prefix_len);

        // Step 4: Validate via regex (if regex is defined)
        bool regex_ok = true;
        if (!var_info.var_regex_.empty()) {
          if (OB_FAIL(match_regex_icu(var_value, var_info.var_regex_, regex_ok, timeout_ms))) {
            LOG_WARN("regex match failed", K(ret), K(var_value), K(var_info.var_regex_));
          }
        }

        if (OB_FAIL(ret)) {
          // Error in regex (timeout or invalid), propagate
        } else if (!regex_ok) {
          matched = false;
        } else {
          // Step 5: Check variable consistency
          bool consistent = true;
          if (OB_FAIL(check_var_consistency(var_info.var_name_, var_value,
                                            var_values, consistent))) {
            LOG_WARN("failed to check variable consistency", K(ret),
                     K(var_info.var_name_), K(var_value));
          } else if (!consistent) {
            matched = false;
            LOG_TRACE("[OUTLINE] pattern match failed: variable inconsistent",
                      K(var_info.var_name_), K(var_value));
          } else {
            matched = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObPatternMatcher::match_exact(const ObString &actual,
                                  const ObString &expected,
                                  bool &matched)
{
  int ret = OB_SUCCESS;
  matched = (0 == actual.case_compare(expected));
  return ret;
}

} // namespace sql
} // namespace oceanbase

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_PARSER

#include "sql/parser/ob_parser_fullwidth_converter.h"

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
//=============================================================================
// ObFullWidthSymbolConverter implementation
//=============================================================================

bool ObFullWidthSymbolConverter::is_string_start(const char *input, int64_t pos, int64_t len) const
{
  bool is_start = false;
  if (pos < len) {
    char c = input[pos];
    if (c == '\'' || c == '"') {
      // Standard string literal or quoted identifier
      is_start = true;
    } else if ((c == 'n' || c == 'N') && pos + 1 < len) {
      char next = input[pos + 1];
      if (next == '\'') {
        // Oracle National string literal: n'...' or N'...'
        is_start = true;
      } else if ((next == 'q' || next == 'Q') && pos + 2 < len && input[pos + 2] == '\'') {
        // Oracle National Q-quote string: nq'...' or NQ'...' etc.
        is_start = true;
      }
    } else if ((c == 'q' || c == 'Q') && pos + 1 < len && input[pos + 1] == '\'') {
      // Oracle Q-quote: q'...' or Q'...'
      is_start = true;
    }
  }
  return is_start;
}

// skip literal strings like:
// 1. noram literal string: '...'
// 2. q-quote string: q'[...]', q'{...}', q'<...>', q'(...)'
// 3. national literal string: n'...'
// 4. national q-quote string: nq'[...]', nq'{...}', nq
// 5. also skips quoted identifier like "ident_name"
int64_t ObFullWidthSymbolConverter::skip_string_literal(const char *input, int64_t pos, int64_t len) const
{
  int64_t next_pos = pos;
  if (next_pos < len) {
    char quote_char = '\0';
    bool is_q_quote = false;
    char q_quote_end_delimiter = '\0';

    // Step 1: Parse prefix and determine string type
    char c = input[next_pos];
    if (c == '\'' || c == '"') {
      // Standard string literal or quoted identifier
      quote_char = c;
      ++next_pos;
    } else if ((c == 'n' || c == 'N') && next_pos + 1 < len && input[next_pos + 1] == '\'') {
      // National string: n'...' or N'...' etc.
      quote_char = '\'';
      next_pos += 2;
    } else if ((c == 'q' || c == 'Q') && next_pos + 1 < len && input[next_pos + 1] == '\'') {
      // Q-quote: q'...' or Q'...'
      is_q_quote = true;
      next_pos += 2;
    } else if ((c == 'n' || c == 'N') && next_pos + 2 < len
               && (input[next_pos + 1] == 'q' || input[next_pos + 1] == 'Q')
               && input[next_pos + 2] == '\'') {
      // National Q-quote: nq'...' or NQ'...' etc.
      is_q_quote = true;
      next_pos += 3;
    } else {
      // Not a recognized string literal, just skip one char
      ++next_pos;
    }

    // Step 2: For Q-quote strings, extract the delimiter
    if (is_q_quote && next_pos < len) {
      char delim = input[next_pos++];
      switch (delim) {
        case '[': q_quote_end_delimiter = ']'; break;
        case '{': q_quote_end_delimiter = '}'; break;
        case '<': q_quote_end_delimiter = '>'; break;
        case '(': q_quote_end_delimiter = ')'; break;
        default: q_quote_end_delimiter = delim; break;
      }
    }

    // Step 3: Scan for end of string
    bool found_end = false;
    while (next_pos < len && !found_end) {
      if (is_q_quote) {
        // Q-quote ends with delimiter + '
        if (input[next_pos] == q_quote_end_delimiter && next_pos + 1 < len && input[next_pos + 1] == '\'') {
          next_pos += 2;
          found_end = true;
        } else {
          ++next_pos;
        }
      } else if (input[next_pos] == quote_char) {
        ++next_pos;
        if (next_pos < len && input[next_pos] == quote_char) {
          // Escaped quote, e.g. 'it''s' , skip the second quote
          //                        ^^
          ++next_pos;
        } else {
          found_end = true; // found end of string literal
        }
      } else {
        ++next_pos;
      }
    }
    if (!found_end) {
      next_pos = len;  // Unterminated string, skip to end
    }
  }

  return next_pos;
}

bool ObFullWidthSymbolConverter::is_comment_start(const char *input, int64_t pos, int64_t len) const
{
  bool is_start = false;
  if (pos < len) {
    if (input[pos] == '-' && pos + 1 < len && input[pos + 1] == '-') {
      is_start = true;
    } else if (input[pos] == '/' && pos + 1 < len && input[pos + 1] == '*') {
      is_start = true;
    }
  }
  return is_start;
}

// skip inline comment starting with -- and block comment like /* ... */
int64_t ObFullWidthSymbolConverter::skip_comment(const char *input, int64_t pos, int64_t len) const
{
  int64_t next_pos = pos;
  if (next_pos < len) {
    if (input[next_pos] == '-' && next_pos + 1 < len && input[next_pos + 1] == '-') {
      next_pos += 2;
      while (next_pos < len && input[next_pos] != '\n' && input[next_pos] != '\r') {
        ++next_pos;
      }
    } else if (input[next_pos] == '/' && next_pos + 1 < len && input[next_pos + 1] == '*') {
      next_pos += 2;
      bool found_end = false;
      while (next_pos + 1 < len && !found_end) {
        if (input[next_pos] == '*' && input[next_pos + 1] == '/') {
          next_pos += 2;
          found_end = true;
        } else {
          ++next_pos;
        }
      }
      if (!found_end) {
        next_pos = len;  // Unterminated comment
      }
    }
  }
  return next_pos;
}

/**
 * @brief Skip string literal (including quoted identifier) and comments,
 *        and find next convertible fullwidth in input
 *
 * If a convertible fullwidth is found, returns its position
 *    the converted ASCII character will be set in `ascii`
 *    the length of the fullwidth in bytes will be set in `char_len`
 * If no convertible character is found, returns input length
 *    `ascii` will be set to '\0' and `char_len` will be set to 0
 *
 * @param input   SQL text
 * @param len     Total length of input
 * @param pos     Start position for scanning
 * @param[out] ascii    mapped ASCII character, or '\0' if none found
 * @param[out] char_len length in bytes of the fullwidth, or 0 if none found
 * @return Position of the next convertible character, or len if none found
 */
int64_t ObFullWidthSymbolConverter::find_next_convertible_char_pos(const char *input,
                                                                   int64_t len,
                                                                   int64_t pos,
                                                                   char &ascii,
                                                                   int32_t &char_len) const
{
  int64_t next_pos = pos;
  ascii = '\0';
  char_len = 0;

  while (next_pos < len && '\0' == ascii && 0 == char_len) {
    if (is_string_start(input, next_pos, len)) {
      next_pos = skip_string_literal(input, next_pos, len);
    } else if (is_comment_start(input, next_pos, len)) {
      next_pos = skip_comment(input, next_pos, len);
    } else {
      ascii = ObFullWidthSymbolConverter::map_fullwidth_symbol_to_ascii(input + next_pos,
                                                                        len - next_pos,
                                                                        charset_type_,
                                                                        char_len);
      if (ascii == '\0') {
        // no conversion and no skip, move to the next character
        next_pos += 1;
      }
    }
  }
  return next_pos;
}

int ObFullWidthSymbolConverter::convert(const ObString &input, ObString &output)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("converter not initialized", K(ret));
  } else if (input.empty()) {
    output = input;
  } else {
    const char *src = input.ptr();
    int64_t src_len = input.length();
    char *dst = NULL;  // dst remains NULL unless conversion happens
    int64_t src_pos = 0;
    int64_t dst_pos = 0;
    int64_t last_copied_pos = 0;

    while (OB_SUCC(ret) && src_pos < src_len) {
      int32_t char_len = 0;
      char ascii = '\0';
      const int64_t next_pos = find_next_convertible_char_pos(src, src_len, src_pos, ascii, char_len);
      if (next_pos > src_pos) {
        // no conversion for this src_pos, just skip to next_pos
        src_pos = next_pos;
      } else if (ascii != '\0' && char_len > 0) {
        // the src_pos is a full width that needs conversion, perform conversion and record position mapping
        // delayed malloc for dst until conversion is needed
        if (OB_ISNULL(dst)) {
          dst = static_cast<char *>(allocator_->alloc(src_len + 1));
          if (OB_ISNULL(dst)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate output buffer", K(ret), K(src_len));
          }
        }
        // copy the unchanged segment before the full width to dst
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(dst)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (src_pos > last_copied_pos) {
          const int64_t copy_len = src_pos - last_copied_pos;
          MEMCPY(dst + dst_pos, src + last_copied_pos, copy_len);
          dst_pos += copy_len;
          last_copied_pos = src_pos;
        }
        // add position mapping entry for this conversion, and copy the converted char to dst
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(pos_mapping_.add_entry(static_cast<int32_t>(src_pos),
                                                  static_cast<int32_t>(dst_pos),
                                                  static_cast<int32_t>(char_len),
                                                  1))) {
          LOG_WARN("failed to add pos mapping entry", K(ret));
        } else {
          dst[dst_pos++] = ascii;
          src_pos += char_len;
          last_copied_pos = src_pos;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL == dst) {
        output = input; // no conversion happened
      } else {
        // copy the remaining segment after the last converted char to dst
        if (last_copied_pos < src_pos) {
          const int64_t copy_len = src_pos - last_copied_pos;
          MEMCPY(dst + dst_pos, src + last_copied_pos, copy_len);
          dst_pos += copy_len;
          last_copied_pos = src_pos;
        }
        dst[dst_pos] = '\0';
        output.assign_ptr(dst, static_cast<int32_t>(dst_pos));
      }
    }
  }

  return ret;
}

void ObFullWidthSymbolConverter::adjust_parse_result(ParseResult &parse_result, int32_t stmt_len)
{
  pos_mapping_.adjust_parse_result(parse_result, stmt_len);
}

//=============================================================================
// ObFullWidthSymbolConverter::FullWidthSymbolPosMapping implementation
//=============================================================================

int FullWidthSymbolPosMapping::add_entry(int32_t orig_pos, int32_t conv_pos, int32_t orig_len, int32_t conv_len)
{
  FullWidthSymbolPosMapEntry entry;
  entry.orig_pos_ = orig_pos;
  entry.conv_pos_ = conv_pos;
  entry.orig_len_ = orig_len;
  entry.conv_len_ = conv_len;
  entry.accumulated_offset_ = (orig_len - conv_len)
                              + (count() > 0 ? entries_[count() - 1].accumulated_offset_ : 0);
  return entries_.push_back(entry);
}

void FullWidthSymbolPosMapping::adjust_parse_result(ParseResult &parse_result, int32_t stmt_len)
{
  if (OB_NOT_NULL(parse_result.result_tree_)) {
    adjust_stmt_loc(parse_result.result_tree_, stmt_len);
  }
  parse_result.start_col_ = conv_to_orig_pos(parse_result.start_col_ - 1, false) + 1;
  parse_result.end_col_ = conv_to_orig_pos(parse_result.end_col_ - 1, true) + 1;
}

void FullWidthSymbolPosMapping::adjust_stmt_loc(ParseNode *node, int32_t stmt_len)
{
  if (OB_ISNULL(node)) {
  } else {
    node->stmt_loc_.first_column_ = conv_to_orig_pos(node->stmt_loc_.first_column_, false);
    node->stmt_loc_.last_column_ = conv_to_orig_pos(node->stmt_loc_.last_column_, true);
    if (OB_NOT_NULL(node->children_)) {
      for (int64_t i = 0; i < node->num_child_; ++i) {
        adjust_stmt_loc(node->children_[i], stmt_len);
      }
    }
  }
}

/**
 * @brief Convert a position in the converted SQL back to the original SQL position.
 *
 * When fullwidth symbols are converted to ASCII, character lengths change:
 * - UTF-8 full width: 3 bytes -> ASCII: 1 byte
 * - GBK/HKSCS full width: 2 bytes -> ASCII: 1 byte
 *
 * Example:
 *   Original:  "SELECT全角 1"  (全角 space is 3 bytes at position 6-8)
 *   Converted: "SELECT 1"      (space is 1 byte at position 6)
 *
 *   Position mapping entry: {orig_pos=6, conv_pos=6, orig_len=3, conv_len=1}
 *   there is a converted range: [6, 7] (conv_pos 6, length 1)
 *
 *   conv_pos=5, is_end_pos=false -> 5  (out of converted range, no conversion before this, pos remains unchanged)
 *   conv_pos=6, is_end_pos=false -> 6  (in converted range, return start of full width range)
 *   conv_pos=7, is_end_pos=true  -> 8  (in converted range, return end of full width range)
 *   conv_pos=10, is_end_pos=false -> 12 (out of converted range, add accumulated conversion offset of (3-1) = 2)
 *
 * @param conv_pos Position in the converted SQL string
 * @param is_end_pos If true, return the end of the original character range;
 *                   if false, return the start of the range
 * @return The corresponding position in the original SQL string
 */
int32_t FullWidthSymbolPosMapping::conv_to_orig_pos(int32_t conv_pos, bool is_end_pos) const
{
  int32_t result_pos = conv_pos;
  if (conv_pos < 0) {
    // unexpected, this error should be raised elsewhere
  } else {
    if (entries_.count() > 0) {
      // find the last entry with entry.conv_pos_ <= conv_pos
      int64_t cand_idx = -1;
      int64_t left = 0;
      int64_t right = entries_.count() - 1;
      while (left <= right) {
        int64_t mid = left + (right - left) / 2;
        if (entries_[mid].conv_pos_ <= conv_pos) {
          cand_idx = mid;
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }
      if (cand_idx < 0) {
        // no conversion before this position, return the original position
        result_pos = conv_pos;
      } else {
        const FullWidthSymbolPosMapEntry &entry = entries_[cand_idx];
        if (conv_pos < entry.conv_pos_ + entry.conv_len_) {
          // inside converted range, return its original position
          result_pos = is_end_pos ? (entry.orig_pos_ + entry.orig_len_ - 1) : entry.orig_pos_;
        } else if (INT32_MAX - entry.accumulated_offset_ >= conv_pos) {
          // out of converted range, return the original position + accumulated offset
          result_pos = conv_pos + entry.accumulated_offset_;
        }
      }
    }
  }
  return result_pos;
}

}  // end namespace sql
}  // end namespace oceanbase

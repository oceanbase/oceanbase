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

#define USING_LOG_PREFIX LIB
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprRegexContext::ObExprRegexContext()
  : ObExprOperatorCtx(),
    inited_(false),
    cflags_(0),
    regexp_engine_(NULL)
{
}

ObExprRegexContext::~ObExprRegexContext()
{
  destroy();
}

void ObExprRegexContext::reset()
{
  destroy();
}

void ObExprRegexContext::destroy()
{
  if (inited_) {
    inited_ = false;
    cflags_ = 0;
    if (regexp_engine_ != NULL) {
      uregex_close(regexp_engine_);
      regexp_engine_ = NULL;
    }
  }
}

int ObExprRegexContext::init(ObExprStringBuf &string_buf,
                             const ObExprRegexpSessionVariables &regex_vars,
                             const ObString &origin_pattern,
                             const uint32_t cflags,
                             const bool reusable,
                             const ObCollationType pattern_cs_type)
{
  int ret = OB_SUCCESS;
  ObString pattern;
  ObString origin_pattern_utf16;
  if (OB_UNLIKELY(inited_ && !reusable)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret), K(this));
  } else if (origin_pattern.length() < 0 ||
             (origin_pattern.length() > 0 && OB_ISNULL(origin_pattern.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param pattern", K(ret), K(origin_pattern));
  } else if (CS_TYPE_UTF16_BIN != pattern_cs_type &&
            CS_TYPE_UTF16_GENERAL_CI != pattern_cs_type) {
    //pattern is nchar or nvarchar
    if (origin_pattern.length() >= 2) {
      if (OB_FAIL(ObExprUtil::convert_string_collation(origin_pattern,
                                                  pattern_cs_type,
                                                  origin_pattern_utf16,
                                                  ObCharset::is_bin_sort(pattern_cs_type) ? CS_TYPE_UTF16_BIN : CS_TYPE_UTF16_GENERAL_CI,
                                                  string_buf))) {
        LOG_WARN("convert charset failed", K(ret));
      }
    } else {
      //because uregex_open returns error if u_pattern_length is 0 or u_pattern is null,
      //use ".{0}" to represent an empty pattern when the valid length of the pattern is 0,
      //for example: regexp_count(convert(t1.c1, 'utf8'),'a')
      ObString const_pattern(".{0}");
      if (OB_FAIL(ObExprUtil::convert_string_collation(const_pattern,
                                                CS_TYPE_UTF8MB4_BIN,
                                                origin_pattern_utf16,
                                                CS_TYPE_UTF16_BIN,
                                                string_buf))) {
        LOG_WARN("convert charset failed", K(ret));
      }
    }
  } else {
    origin_pattern_utf16 = origin_pattern;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(origin_pattern_utf16.length() % sizeof(UChar) != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param, source text is null", K(ret), K(origin_pattern_utf16.length()));
  } else if (OB_FAIL(preprocess_pattern(string_buf, origin_pattern_utf16, pattern))) {
    LOG_WARN("failed to prepare process pattern", K(origin_pattern_utf16), K(pattern));
  } else if (reusable && inited_ &&
             pattern_ == ObString(0, pattern.length(), pattern.ptr())
             && cflags_ == cflags) {
    // reuse the previous compile result.
  } else {
    if (inited_) { // reusable && pattern changed
      reset();
    }
    pattern_allocator_.prepare(string_buf);
    pattern_wc_allocator_.prepare(string_buf);
    char *pattern_save = static_cast<char *>(pattern_allocator_.alloc(pattern.length()));
    if (NULL == pattern_save) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      MEMCPY(pattern_save, pattern.ptr(), pattern.length());
      pattern_.assign_ptr(pattern_save, pattern.length());
      cflags_ = cflags;
    }
    int32_t u_pattern_length = 0;
    UChar *u_pattern = NULL;
    UParseError parse_error;
    UErrorCode u_error_code = U_ZERO_ERROR;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_valid_unicode_string(string_buf, pattern, u_pattern, u_pattern_length))) {
      LOG_WARN("failed to get valid unicode string", K(ret));
    } else if (OB_ISNULL(u_pattern)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpcted null", K(ret), K(pattern), K(u_pattern_length));
    } else {
      regexp_engine_ = uregex_open(u_pattern, u_pattern_length, cflags, &parse_error, &u_error_code);
      uregex_setStackLimit(regexp_engine_, regex_vars.regexp_stack_limit_, &u_error_code);
      uregex_setTimeLimit(regexp_engine_, regex_vars.regexp_time_limit_, &u_error_code);
      if (OB_FAIL(check_icu_regexp_status(u_error_code, &parse_error))) {
        LOG_WARN("failed to check icu regexp status", K(ret));
        if (regexp_engine_ != NULL) {
          uregex_close(regexp_engine_);
          regexp_engine_ = NULL;
        }
      } else {
        inited_ = true;
      }
    }
  }
  return ret;
}

int ObExprRegexContext::match(ObExprStringBuf &string_buf,
                              const ObString &text,
                              const ObCollationType cs_type,
                              const int64_t start,
                              bool &result) const
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  UChar *u_text = NULL;
  int32_t u_text_length = 0;
  UErrorCode m_error_code = U_ZERO_ERROR;
  result = false;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(regexp_engine_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(inited_), K(regexp_engine_));
  } else if (OB_FAIL(get_valid_unicode_string(string_buf, text, u_text, u_text_length))) {
    LOG_WARN("failed to get valid unicode string", K(ret));
  } else {
    uregex_setText(regexp_engine_, u_text, u_text_length, &m_error_code);
    result = uregex_find(regexp_engine_, start, &m_error_code);
    if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
      LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
    } else {
      LOG_TRACE("Succeed to match", K(start), K(text.length()), K(result));
    }
  }
  return ret;
}

int ObExprRegexContext::find(ObExprStringBuf &string_buf,
                             const ObString &text,
                             const ObCollationType cs_type,
                             const int64_t start,
                             const int64_t occurrence,
                             const int64_t return_option,
                             const int64_t subexpr,
                             int64_t &result) const
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  UChar *u_text = NULL;
  int32_t u_text_length = 0;
  result = 0;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(regexp_engine_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(inited_), K(regexp_engine_));
  } else if (OB_FAIL(get_valid_unicode_string(string_buf, text, u_text, u_text_length))) {
    LOG_WARN("failed to get valid unicode string", K(ret));
  } else if (0 == u_text_length) {
    //do nothing
  } else {
    UErrorCode m_error_code = U_ZERO_ERROR;
    uregex_setText(regexp_engine_, u_text, u_text_length, &m_error_code);
    bool found = uregex_find(regexp_engine_, start, &m_error_code);
    for (int64_t i = 1; i < occurrence && found; ++i) {
      found = uregex_findNext(regexp_engine_, &m_error_code);
    }
    if (lib::is_oracle_mode() && U_INDEX_OUTOFBOUNDS_ERROR == m_error_code) {
      //compatible oracle, not throw error
    } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
      LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
    } else if (found) {
      int64_t start_pos = uregex_start(regexp_engine_, subexpr, &m_error_code) + 1;
      int64_t end_pos = uregex_end(regexp_engine_, subexpr, &m_error_code)  + 1;
      if (lib::is_oracle_mode() && U_INDEX_OUTOFBOUNDS_ERROR == m_error_code) {
        //compatible oracle, not throw error
      } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
        LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
      } else {
        result = return_option ? end_pos : start_pos;
        LOG_TRACE("succeed to regexp instr", K(result), K(start), K(occurrence), K(return_option),
                                   K(subexpr), K(text), K(text.length()), K(end_pos), K(start_pos));
      }
    } else {
      result = 0;
      LOG_TRACE("succeed to regexp instr", K(result), K(start), K(occurrence), K(return_option),
                                           K(subexpr), K(text), K(text.length()));
    }
  }
  return ret;
}

int ObExprRegexContext::count(ObExprStringBuf &string_buf,
                              const ObString &text,
                              const ObCollationType cs_type,
                              const int32_t start,
                              int64_t &result) const
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  UChar *u_text = NULL;
  int32_t u_text_length = 0;
  result = 0;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(regexp_engine_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(inited_), K(regexp_engine_));
  } else if (OB_FAIL(get_valid_unicode_string(string_buf, text, u_text, u_text_length))) {
    LOG_WARN("failed to get valid unicode string", K(ret));
  } else if (0 == u_text_length) {
    //do nothing
  } else {
    UErrorCode m_error_code = U_ZERO_ERROR;
    uregex_setText(regexp_engine_, u_text, u_text_length, &m_error_code);
    bool found = uregex_find(regexp_engine_, start, &m_error_code);
    result += found ? 1 : 0;
    while (uregex_findNext(regexp_engine_, &m_error_code)) { ++result; }
    if (lib::is_oracle_mode() && U_INDEX_OUTOFBOUNDS_ERROR == m_error_code) {
      //compatible oracle, not throw error
      result = 0;
      LOG_TRACE("succeed get regexp count", K(found), K(result), K(text), K(start));
    } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
      LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
    } else {
      LOG_TRACE("succeed get regexp count", K(found), K(result), K(text), K(start));
    }
  }
  return ret;
}

int ObExprRegexContext::substr(ObExprStringBuf &string_buf,
                               const ObString &text,
                               const ObCollationType cs_type,
                               const int64_t start,
                               const int64_t occurrence,
                               const int64_t subexpr,
                               ObString &result) const
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  UChar *u_text = NULL;
  int32_t u_text_length = 0;
  result.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(regexp_engine_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(inited_), K(regexp_engine_));
  } else if (OB_FAIL(get_valid_unicode_string(string_buf, text, u_text, u_text_length))) {
    LOG_WARN("failed to get valid unicode string", K(ret));
  } else {
    UErrorCode m_error_code = U_ZERO_ERROR;
    int64_t start_pos = 0;
    int64_t end_pos = 0;
    uregex_setText(regexp_engine_, u_text, u_text_length, &m_error_code);
    bool found = uregex_find(regexp_engine_, start, &m_error_code);
    for (int64_t i = 1; i < occurrence && found; ++i) {
      found = uregex_findNext(regexp_engine_, &m_error_code);
    }
    if (lib::is_oracle_mode() && U_INDEX_OUTOFBOUNDS_ERROR == m_error_code) {
        //compatible oracle, not throw error
    } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
      LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
    } else if (found) {
      start_pos = uregex_start(regexp_engine_, subexpr, &m_error_code);
      end_pos = uregex_end(regexp_engine_, subexpr, &m_error_code);
      int64_t sublength = end_pos - start_pos;
      if (lib::is_oracle_mode() && U_INDEX_OUTOFBOUNDS_ERROR == m_error_code) {
        //compatible oracle, not throw error
      } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
        LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
      } else if (sublength > 0) {
        if (OB_UNLIKELY(sizeof(UChar) * end_pos > text.length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(sizeof(UChar) * end_pos), K(text.length()));
        } else {
          result.assign_ptr(text.ptr() + sizeof(UChar) * start_pos, sublength * sizeof(UChar));
        }
      }
    }
    LOG_TRACE("succeed to regexp instr", K(result), K(start), K(occurrence), K(start_pos), K(found),
                                         K(end_pos), K(subexpr), K(text), K(text.length()));
  }
  return ret;
}

int ObExprRegexContext::replace(ObExprStringBuf &string_buf,
                                const ObString &text_string,
                                const ObCollationType cs_type,
                                const ObString &replace_string,
                                const int64_t start,
                                const int64_t occurrence,
                                ObString &result) const
{
  int ret = OB_SUCCESS;
  UNUSED(cs_type);
  UChar *u_text = NULL;
  int32_t u_text_length = 0;
  result.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(regexp_engine_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(inited_), K(regexp_engine_));
  } else if (OB_FAIL(get_valid_unicode_string(string_buf, text_string, u_text, u_text_length))) {
    LOG_WARN("failed to get valid unicode string", K(ret));
  } else if (0 == u_text_length) {
    result = text_string;
  } else {
    UChar *replace_buff = NULL;
    int32_t buff_size = 0;
    int32_t buff_pos = 0;
    UErrorCode m_error_code = U_ZERO_ERROR;
    UChar *u_replace = NULL;
    int32_t u_replace_length = 0;
    uregex_setText(regexp_engine_, u_text, u_text_length, &m_error_code);
    bool found = uregex_find(regexp_engine_, start, &m_error_code);
    int64_t end_of_previous_match = 0;
    for (int i = 1; i < occurrence && found; ++i) {
      end_of_previous_match = uregex_end(regexp_engine_, 0, &m_error_code);
      found = uregex_findNext(regexp_engine_, &m_error_code);
    }
    if (lib::is_oracle_mode() && U_INDEX_OUTOFBOUNDS_ERROR == m_error_code) {
      result = text_string;
    } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
      LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
    } else if (!found) {
      result = text_string;
    } else if (OB_ISNULL(replace_buff = static_cast<UChar *>(string_buf.alloc(text_string.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed.", K(replace_buff), K(text_string.length()), K(ret));
    } else if (OB_FAIL(get_valid_replace_string(string_buf, replace_string, u_replace, u_replace_length))) {
      LOG_WARN("failed to get valid replace string", K(ret));
    } else {
      buff_size = text_string.length() / sizeof(UChar);
      if (OB_FAIL(append_head(string_buf,
                              start > end_of_previous_match ? start : end_of_previous_match,
                              replace_buff,
                              buff_size,
                              buff_pos))) {
        LOG_WARN("failed to append head", K(ret));
      } else {
        do {
          if (OB_FAIL(append_replace_str(string_buf, u_replace, u_replace_length,
                                         replace_buff, buff_size, buff_pos))) {
            LOG_WARN("failed to append replace str", K(ret));
          }
        } while (OB_SUCC(ret) && occurrence == 0 && uregex_findNext(regexp_engine_, &m_error_code));
        if (OB_SUCC(ret)) {
          if (OB_FAIL(append_tail(string_buf, replace_buff, buff_size, buff_pos))) {
            LOG_WARN("failed to append tail", K(ret));
          } else {
            for (int64_t i = 0; i < buff_pos; ++i) {
              replace_buff[i] = ntohs(static_cast<uint16_t>(replace_buff[i]));
            }
            result.assign_ptr(static_cast<char*>((void*)replace_buff), buff_pos * sizeof(UChar));
          }
        }
      }
      LOG_TRACE("succeed to regexp replace", K(result), K(start), K(occurrence), K(found),
                                             K(end_of_previous_match), K(buff_pos), K(text_string),
                                             K(text_string.length()), K(replace_string),
                                             K(replace_string.length()));
    }
  }
  return ret;
}

int ObExprRegexContext::append_head(ObExprStringBuf &string_buf,
                                    const int32_t current_pos,
                                    UChar *&replace_buff,
                                    int32_t &buff_size,
                                    int32_t &buff_pos) const
{
  int ret = OB_SUCCESS;
  if (current_pos <= 0) {
    //do nothing
  } else {
    int32_t text_length = 0;
    UErrorCode m_error_code = U_ZERO_ERROR;
    const UChar *text = uregex_getText(regexp_engine_, &text_length, &m_error_code);
    if (m_error_code == U_ZERO_ERROR) {
      if (OB_UNLIKELY(current_pos > text_length)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(current_pos), K(text_length));
      } else if (buff_size - buff_pos < current_pos) {
        int32_t required_buffer_size = (buff_pos + current_pos) * 2;
        UChar *tmp_buff = NULL;
        if (OB_ISNULL(tmp_buff = static_cast<UChar *>(string_buf.alloc(required_buffer_size * sizeof(UChar))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed.", K(tmp_buff), K(required_buffer_size), K(ret));
        } else {
          MEMCPY(tmp_buff, replace_buff, buff_pos * sizeof(UChar));
          string_buf.free(replace_buff);
          replace_buff = tmp_buff;
          buff_size = required_buffer_size;
          MEMCPY(replace_buff + buff_pos, text, current_pos * sizeof(UChar));
        }
      } else {
        MEMCPY(replace_buff + buff_pos, text, current_pos * sizeof(UChar));
      }
      LOG_TRACE("succeed to append head", K(current_pos), K(text_length),
                                          K(buff_pos), K(buff_size));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
      LOG_WARN("failed to check icu regexp status", K(u_errorName(m_error_code)), K(ret));
    } else {
      buff_pos += current_pos;
      LOG_TRACE("succeed to append head", K(buff_pos), K(current_pos), K(buff_size));
    }
  }
  return ret;
}

int ObExprRegexContext::append_replace_str(ObExprStringBuf &string_buf,
                                           const UChar *u_replace,
                                           const int32_t u_replace_length,
                                           UChar *&replace_buff,
                                           int32_t &buff_size,
                                           int32_t &buff_pos) const
{
  int ret = OB_SUCCESS;
  int32_t capacity = buff_size - buff_pos;
  UErrorCode m_error_code = U_ZERO_ERROR;
  UChar *ptr = replace_buff + buff_pos;
  int32_t replace_size = uregex_appendReplacement(regexp_engine_,
                                                  u_replace,
                                                  u_replace_length,
                                                  &ptr,
                                                  &capacity,
                                                  &m_error_code);
  if (m_error_code == U_BUFFER_OVERFLOW_ERROR) {
    m_error_code = U_ZERO_ERROR;
    int32_t required_buffer_size = (buff_pos + replace_size) * 2;
    UChar *tmp_buff = NULL;
    if (OB_ISNULL(tmp_buff = static_cast<UChar *>(string_buf.alloc(required_buffer_size * sizeof(UChar))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed.", K(tmp_buff), K(required_buffer_size), K(ret));
    } else {
      MEMCPY(tmp_buff, replace_buff, buff_pos * sizeof(UChar));
      string_buf.free(replace_buff);
      replace_buff = tmp_buff;
      buff_size = required_buffer_size;
      capacity = buff_size - buff_pos;
      ptr = &(replace_buff[0]) + buff_pos;
      replace_size = uregex_appendReplacement(regexp_engine_,
                                              u_replace,
                                              u_replace_length,
                                              &ptr,
                                              &capacity,
                                              &m_error_code);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
    LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
  } else {
    buff_pos += replace_size;
    LOG_TRACE("succeed to append append replace", K(buff_pos), K(replace_size), K(buff_size));
  }
  return ret;
}

int ObExprRegexContext::append_tail(ObExprStringBuf &string_buf,
                                    UChar *&replace_buff,
                                    int32_t &buff_size,
                                    int32_t &buff_pos) const
{
  int ret = OB_SUCCESS;
  int32_t capacity = buff_size - buff_pos;
  UErrorCode m_error_code = U_ZERO_ERROR;
  UChar *ptr = replace_buff + buff_pos;
  int32_t tail_size = uregex_appendTail(regexp_engine_, &ptr, &capacity, &m_error_code);
  if (m_error_code == U_BUFFER_OVERFLOW_ERROR) {
    m_error_code = U_ZERO_ERROR;
    int32_t required_buffer_size = buff_pos + tail_size;
    UChar *tmp_buff = NULL;
    if (OB_ISNULL(tmp_buff = static_cast<UChar *>(string_buf.alloc(required_buffer_size * sizeof(UChar))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed.", K(tmp_buff), K(required_buffer_size), K(ret));
    } else {
      MEMCPY(tmp_buff, replace_buff, buff_pos * sizeof(UChar));
      string_buf.free(replace_buff);
      replace_buff = tmp_buff;
      buff_size = required_buffer_size;
      ptr = &(replace_buff[0]) + buff_pos;
      capacity = buff_size - buff_pos;
      tail_size = uregex_appendTail(regexp_engine_, &ptr, &capacity, &m_error_code);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
    LOG_WARN("failed to check icu regexp status", K(u_errorName(m_error_code)), K(ret));
  } else {
    buff_pos += tail_size;
    LOG_TRACE("succeed to append tail", K(buff_pos), K(tail_size), K(buff_size));
  }
  return ret;
}

int ObExprRegexContext::check_icu_regexp_status(UErrorCode u_error_code,
                                                const UParseError *parse_error/*=null*/) const
{
  int ret = OB_SUCCESS;
  //maybe we can break down all error types in the future, you can see UErrorCode in utypes.h file.
  if (U_SUCCESS(u_error_code)) {
    //do nothing
  } else {
    switch (u_error_code)
    {
    case U_REGEX_MISMATCHED_PAREN:
      ret = OB_ERR_REGEXP_EPAREN;
      LOG_WARN("unmatched parentheses in regular expression", K(ret));
      break;
    case U_REGEX_BAD_ESCAPE_SEQUENCE:
      ret = OB_ERR_REGEXP_EESCAPE;
      LOG_WARN("invalid escape \\ sequence in regular expression", K(ret));
      break;
    case U_REGEX_MISSING_CLOSE_BRACKET:
      ret = OB_ERR_REGEXP_EBRACK;
      LOG_WARN("nmatched bracket in regular expression", K(ret));
      break;
    case U_REGEX_RULE_SYNTAX:
      if (parse_error != NULL) {
        ObSqlString errmsg;
        if (OB_FAIL(errmsg.append_fmt("%s, Syntax error in regular expression on line %d, character %d.",
                                       u_errorName(u_error_code),
                                       parse_error->line,
                                       parse_error->offset))) {
          LOG_WARN("failed to append fmt", K(ret));
        } else {
          ret = OB_ERR_REGEXP_ERROR;
          LOG_WARN("Syntax error in regular expression", K(ret), K(u_errorName(u_error_code)));
          LOG_USER_ERROR(OB_ERR_REGEXP_ERROR, errmsg.ptr());
        }
      } else {
        ret = OB_ERR_REGEXP_ERROR;
        LOG_WARN("other error in icu regexp", K(ret), K(u_errorName(u_error_code)));
        LOG_USER_ERROR(OB_ERR_REGEXP_ERROR, u_errorName(u_error_code));
      }
      break;
    default:
      ret = OB_ERR_REGEXP_ERROR;
      LOG_WARN("other error in icu regexp", K(ret), K(u_errorName(u_error_code)));
      LOG_USER_ERROR(OB_ERR_REGEXP_ERROR, u_errorName(u_error_code));
      break;
    }
  }
  return ret;
}

//Oracle allow more, we consider optimizer following function
int ObExprRegexContext::preprocess_pattern(ObExprStringBuf &string_buf,
                                           const ObString &origin_pattern,
                                           ObString &pattern)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    pattern = origin_pattern;
  } else if (origin_pattern.length() / sizeof(UChar) > strlen("[^][:]")) {
    /*oracle mode allow:
    * regexp_substr('xxxx','[^][:]') <==> regexp_substr('xxxx','[^:]')
    */
    ObString const_str1(strlen("[^][:]"), "[^][:]");
    ObString u_const_str1;
    ObArenaAllocator alloc("ObExprRegexp");
    if (OB_FAIL(ObExprUtil::convert_string_collation(const_str1,
                                                     CS_TYPE_UTF8MB4_BIN,
                                                     u_const_str1,
                                                     CS_TYPE_UTF16_BIN,
                                                     alloc))) {
      LOG_WARN("convert charset failed", K(ret));
    } else {
      bool is_continued = true;
      bool need_transform = false;
      const char *origin_buf = origin_pattern.ptr();
      const int32_t origin_buf_len = origin_pattern.length();
      int32_t begin_idx = -1;
      for (int32_t i = 0; is_continued && i + u_const_str1.length() <= origin_buf_len; ++i) {
        ObString tmp_str(u_const_str1.length(), origin_buf + i);
        if (0 == tmp_str.compare(u_const_str1)) {
          if (!need_transform) {
            need_transform = true;
            begin_idx = i;
            i = i + u_const_str1.length() - 1;
          } else {
            need_transform = false;
            is_continued = false;
          }
        }
      }
      if (need_transform) {
        ObString const_str2(strlen("[^:]"), "[^:]");
        ObString u_const_str2;
        char *buf = NULL;
        if (OB_UNLIKELY(begin_idx < 0 || begin_idx > origin_buf_len - u_const_str1.length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(begin_idx), K(origin_buf_len), K(u_const_str1.length()));
        } else if (OB_FAIL(ObExprUtil::convert_string_collation(const_str2,
                                                                CS_TYPE_UTF8MB4_BIN,
                                                                u_const_str2,
                                                                CS_TYPE_UTF16_BIN,
                                                                alloc))) {
          LOG_WARN("convert charset failed", K(ret));
        } else if (OB_ISNULL(buf = static_cast<char *>(string_buf.alloc(
                                   origin_buf_len - u_const_str1.length() + u_const_str2.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(origin_pattern), K(ret));
        } else {
          int32_t buf_len = 0;
          MEMCPY(buf, origin_buf, begin_idx);
          buf_len += begin_idx;
          MEMCPY(buf + buf_len, u_const_str2.ptr(), u_const_str2.length());
          buf_len += u_const_str2.length();
          if (origin_buf_len - begin_idx - u_const_str1.length() > 0) {
            MEMCPY(buf + buf_len,
                   origin_buf + begin_idx + u_const_str1.length(),
                   origin_buf_len - begin_idx - u_const_str1.length());
            buf_len += origin_buf_len - begin_idx - u_const_str1.length();
          }
          pattern.assign_ptr(buf, buf_len);
          LOG_TRACE("succeed to preprocess pattern", K(buf), K(buf_len));
        }
      } else {
        pattern = origin_pattern;
        LOG_TRACE("succeed to preprocess pattern", K(origin_pattern), K(pattern));
      }
    }
  } else {
    pattern = origin_pattern;
    LOG_TRACE("succeed to preprocess pattern", K(origin_pattern), K(pattern));
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to preprocess pattern", K(origin_pattern), K(pattern));
  }
  return ret;
}

int ObExprRegexContext::get_regexp_flags(const ObString &match_param,
                                         const bool is_case_sensitive,
                                         const bool is_som_leftmost,
                                         const bool is_single_match,
                                         uint32_t& flags)
{
  UNUSEDx(is_som_leftmost, is_single_match);
  int ret = OB_SUCCESS;
  const char *ptr = match_param.ptr();
  int length = match_param.length();
  flags = is_case_sensitive ? 0 : UREGEX_CASE_INSENSITIVE;
  if (lib::is_oracle_mode()) {//compatible oracle
    flags |= UREGEX_UNIX_LINES;
  }
  for (int i = 0; OB_SUCC(ret) && i < length; i++) {
    char c = ptr[i];
    switch (c) {
      case 'c':
        flags &= ~UREGEX_CASE_INSENSITIVE;
        break;
      case 'i':
        flags |= UREGEX_CASE_INSENSITIVE;
        break;
      case 'm':
        flags |= UREGEX_MULTILINE;
        break;
      case 'n':
        flags |= UREGEX_DOTALL;
        break;
      case 'u':
        if (lib::is_mysql_mode()) {//compatible oracle
          flags |= UREGEX_UNIX_LINES;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid match param", K(match_param), K(c));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "use match param in regexp expression");
        }
        break;
      case 'x':
        if (lib::is_oracle_mode()) {//compatible oracle
          flags |= UREGEX_COMMENTS;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid match param", K(match_param), K(c));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "use match param in regexp expression");
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid match param", K(match_param), K(c));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "use match param in regexp expression");
        break;
    }
  }
  return ret;
}

int ObExprRegexContext::get_valid_unicode_string(ObExprStringBuf &string_buf,
                                                 const ObString &origin_str,
                                                 UChar *&u_str,
                                                 int32_t &u_str_len) const
{
  int ret = OB_SUCCESS;
  int32_t buf_len = origin_str.empty() ? sizeof(UChar) : origin_str.length();
  void *tmp_buf = NULL;
  if (OB_UNLIKELY(buf_len % sizeof(UChar) != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param, source text is null", K(ret), K(origin_str), K(origin_str.length()));
  } else if (OB_ISNULL(tmp_buf = string_buf.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(tmp_buf));
  } else {
    MEMSET(tmp_buf, 0, buf_len);
    MEMCPY(tmp_buf, origin_str.ptr(), origin_str.length());
    u_str = static_cast<UChar *>(tmp_buf);
    u_str_len = origin_str.length() / sizeof(UChar);
    for (int64_t i = 0; i < u_str_len; ++i) {
      u_str[i] = htons(static_cast<uint16_t>(u_str[i]));
    }
  }
  return ret;
}

int ObExprRegexContext::get_valid_replace_string(ObIAllocator &alloc,
                                                 const ObString &origin_replace,
                                                 UChar *&u_replace,
                                                 int32_t &u_replace_len) const
{
  int ret = OB_SUCCESS;
  u_replace_len = 0;
  u_replace = NULL;
  int32_t buf_len = origin_replace.empty() ? sizeof(UChar) : origin_replace.length() * 2;
  if (lib::is_mysql_mode()) {
    if (OB_FAIL(get_valid_unicode_string(alloc, origin_replace, u_replace, u_replace_len))) {
      LOG_WARN("failed to get valid unicode string", K(ret));
    } else {/*do nothing*/}
  } else if (OB_UNLIKELY(buf_len % sizeof(UChar) != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param, source text is null", K(ret), K(origin_replace),
                                                   K(origin_replace.length()));
  } else if (OB_ISNULL(u_replace = static_cast<UChar *>(alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(u_replace));
  } else if (origin_replace.empty()) {
    MEMSET(u_replace, 0, buf_len);
    u_replace_len = 0;
    LOG_TRACE("succeed to get valid replace string", K(u_replace_len));
  } else {
    //oracle mode replace string '\1' <==> '$1' in mysql mode, we need extra convert.
    UErrorCode m_error_code = U_ZERO_ERROR;
    int32_t group_count = uregex_groupCount(regexp_engine_, &m_error_code);
    MEMSET(u_replace, 0, buf_len);
    if (OB_FAIL(check_icu_regexp_status(m_error_code))) {
      LOG_WARN("failed to check icu regexp status", K(ret), K(u_errorName(m_error_code)));
    } else {
      const UChar *tmp_buf = static_cast<const UChar *>((void*)origin_replace.ptr());
      int32_t tmp_len = origin_replace.length() / sizeof(UChar);
      int32_t max_u_replace_len = buf_len / sizeof(UChar);
      int32_t backslash_cnt = 0;
      for (int64_t i = 0; i < tmp_len; ++i) {
        u_replace[u_replace_len++] = htons(static_cast<uint16_t>(tmp_buf[i]));
        if (static_cast<uint16_t>(u_replace[u_replace_len - 1]) == 0x5c) {//'\'
          bool is_continue = true;
          ++backslash_cnt;
          while (i < tmp_len - 1 && is_continue) {
            u_replace[u_replace_len++] = htons(static_cast<uint16_t>(tmp_buf[++i]));
            if (static_cast<uint16_t>(u_replace[u_replace_len - 1]) == 0x5c) {
              ++backslash_cnt;
            } else {
              is_continue = false;
            }
          }
          if (i < tmp_len && u_replace_len < max_u_replace_len) {
            if (backslash_cnt % 2 == 1 &&
                static_cast<uint16_t>(u_replace[u_replace_len - 1]) >= 0x31 &&
                static_cast<uint16_t>(u_replace[u_replace_len - 1]) <= 0x39) {//'\1'=>'$1'
              if (group_count > 0) {
                if (static_cast<uint16_t>(u_replace[u_replace_len - 1]) - 0x30 > group_count) {
                  //if the specify group num bigger than the total count, just skip, compatible Oracle.
                  u_replace_len = u_replace_len - 2;
                } else {
                  u_replace[u_replace_len - 2] = 0x24;
                }
              } else if (u_replace_len < max_u_replace_len) {
                uint16_t tmp_val = static_cast<uint16_t>(u_replace[u_replace_len - 1]);
                u_replace[u_replace_len - 1] = 0x5c;
                u_replace[u_replace_len++] = tmp_val;
              }
            } else if (backslash_cnt % 2 == 0 &&
                       static_cast<uint16_t>(u_replace[u_replace_len - 1]) == 0x24 &&
                       u_replace_len < max_u_replace_len) {//'\\$' =>'\\\$'
              u_replace[u_replace_len - 1] = 0x5c;
              u_replace[u_replace_len++] = 0x24;
            }
          }
          backslash_cnt = 0;
        } else if (static_cast<uint16_t>(u_replace[u_replace_len - 1]) == 0x24 &&
                   u_replace_len < max_u_replace_len) {//'$' ==>'\$'
          u_replace[u_replace_len - 1] = 0x5c;
          u_replace[u_replace_len++] = 0x24;
        } else {//reset
          backslash_cnt = 0;
        }
      }
      LOG_TRACE("succeed to get valid replace string", K(tmp_len), K(u_replace_len), K(group_count));
    }
  }
  return ret;
}

int ObExprRegexContext::check_need_utf8(ObRawExpr *expr, bool &need_utf8)
{
  int ret = OB_SUCCESS;
  need_utf8 = false;
  const ObRawExpr * real_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(expr, real_expr))) {
    LOG_WARN("fail to get real expr without cast", K(ret));
  } else if (OB_ISNULL(real_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("real expr is invalid", K(ret), K(real_expr));
  } else {
    need_utf8 = real_expr->get_result_type().is_nchar() ||
                real_expr->get_result_type().is_nvarchar2() ||
                real_expr->get_result_type().is_blob() ||
                real_expr->get_result_type().is_binary() ||
                real_expr->get_result_type().is_varbinary();
  }
  return ret;
}

int ObExprRegexContext::check_binary_compatible(const ObExprResType *types, int64_t num) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    int64_t binary_param_idx = -1;
    int64_t nobinary_param_idx = -1;
    for (int64_t i = 0; i < num; ++i) {
      if (ObExprRegexContext::is_binary_string(types[i])) {
        binary_param_idx = i;
      } else if (!ObExprRegexContext::is_binary_compatible(types[i])) {
        nobinary_param_idx = i;
      }
    }
    if (-1 != binary_param_idx && -1 != nobinary_param_idx) {
      const char *coll_name1 = ObCharset::collation_name(types[binary_param_idx].get_collation_type());
      const char *coll_name2 = ObCharset::collation_name(types[nobinary_param_idx].get_collation_type());
      ObString collation1 = ObString::make_string(coll_name1);
      ObString collation2 = ObString::make_string(coll_name2);
      ret = OB_ERR_MYSQL_CHARACTER_SET_MISMATCH;
      LOG_USER_ERROR(OB_ERR_MYSQL_CHARACTER_SET_MISMATCH, collation1.length(), collation1.ptr(), collation2.length(), collation2.ptr());
      LOG_WARN("If one of the params is binary string, all of the params should be implicitly castable to binary charset.", K(ret), K(*types));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObExprRegexpSessionVariables, regexp_stack_limit_, regexp_time_limit_);

#if defined(__x86_64__)
ObExprHsRegexCtx::ObExprHsRegexCtx()
  : ObExprOperatorCtx(),
    inited_(false),
    pattern_(),
    hs_flags_(0),
    hs_db_(nullptr),
    hs_scratch_(nullptr),
    hs_compile_err_(nullptr)
{
}

ObExprHsRegexCtx::~ObExprHsRegexCtx()
{
  destroy();
}

void ObExprHsRegexCtx::reset()
{
  destroy();
}

void ObExprHsRegexCtx::destroy()
{
  if (inited_) {
    inited_ = false;
    hs_flags_= 0;
    if (hs_scratch_ != nullptr) {
      hs_free_scratch(hs_scratch_);
      hs_scratch_ = nullptr;
    }
    if (hs_db_ != nullptr) {
      hs_free_database(hs_db_);
      hs_db_ = nullptr;
    }
  }
}

int ObExprHsRegexCtx::get_regexp_flags(const ObString &match_param,
                                       const bool is_case_sensitive,
                                       const bool is_som_leftmost,
                                       const bool is_single_match,
                                       uint32_t &flags) {
  int ret = OB_SUCCESS;
  flags = HS_FLAG_UTF8;
  if (is_single_match) {
    flags |= HS_FLAG_ALLOWEMPTY;
  }
  flags |= is_case_sensitive ? 0 : HS_FLAG_CASELESS;
  flags |= is_som_leftmost ? HS_FLAG_SOM_LEFTMOST : 0;
  flags |= is_single_match ? HS_FLAG_SINGLEMATCH : 0;
  const char *ptr = match_param.ptr();
  int length = match_param.length();
  for (int i = 0; OB_SUCC(ret) && i < length; i++) {
    char c = ptr[i];
    switch (c) {
      case 'c':
        flags &= ~HS_FLAG_CASELESS;
        break;
      case 'i':
        flags |= HS_FLAG_CASELESS;
        break;
      case 'm':
        flags |= HS_FLAG_MULTILINE;
        break;
      case 'n':
        flags |= HS_FLAG_DOTALL;
        break;
      case 'u':
        // hyperscan use unix-only ending by default, do nothing
        break;
      case 'x':
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hyperscan cann't handle flag 'u' or 'x', please use ICU.", K(match_param), K(c));
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "not supported match_param of hyperscan engine");
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid match param", K(match_param), K(c));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "use match param in regexp expression");
        break;
    }
  }
  return ret;
}

int ObExprHsRegexCtx::init(ObExprStringBuf &string_buf,
                           const ObExprRegexpSessionVariables &regex_vars,
                           const ObString &origin_pattern,
                           const uint32_t flags,
                           const bool reusable,
                           const ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  ObString pattern;
  ObString origin_pattern_utf8;
  UNUSED(regex_vars);
  if (OB_UNLIKELY(inited_ && !reusable)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret), K(this));
  } else if (OB_UNLIKELY(origin_pattern.length() < 0) ||
             (origin_pattern.length() > 0 && OB_ISNULL(origin_pattern.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pattern", K(ret), K(origin_pattern));
  } else if (CS_TYPE_UTF8MB4_BIN != cs_type && CS_TYPE_UTF8MB4_GENERAL_CI != cs_type) {
    // hyperscan cannot handle empty pattern as ICU can.
    // and empty pattern is not supported in mysql mode, it is supported in oracle mode.
    // so only in oracle mode, hyperscan and icu handle empty pattern differently.
    if (OB_FAIL(ObExprUtil::convert_string_collation(
          origin_pattern,
          cs_type,
          origin_pattern_utf8,
          ObCharset::is_bin_sort(cs_type) ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_UTF8MB4_GENERAL_CI,
          string_buf))) {
      LOG_WARN("convert charset failed", K(ret));
    }
  } else {
    origin_pattern_utf8 = origin_pattern;
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(pattern = origin_pattern_utf8)) { // TODO: preprocess pattern for oracle
  } else if (reusable &&
             inited_ &&
             pattern_ == ObString(0, pattern.length(), pattern.ptr()) &&
             hs_flags_ == flags) {
    // reuse the previous compile result.
  } else {
    if (inited_) { // reusable && pattern changed
      reset();
    }
    LOG_TRACE("init hs engine", K(origin_pattern), K(reusable), K(cs_type), K(pattern.length()),
              K(pattern), K(flags));
    char *pattern_save = static_cast<char *>(string_buf.alloc(pattern.length() + 1));
    if (OB_ISNULL(pattern_save)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), KP(pattern_save));
    } else {
      MEMCPY(pattern_save, pattern.ptr(), pattern.length());
      pattern_save[pattern.length()] = '\0';
      pattern_.assign(pattern_save, pattern.length());
      hs_flags_ = flags;
    }
    hs_error_t hs_ret = HS_SUCCESS;
    hs_compile_err_ = nullptr;
    if (OB_FAIL(ret)) {
    } else if ((hs_ret = hs_compile(pattern_save, hs_flags_ | HS_FLAG_ALLOWEMPTY, HS_MODE_BLOCK, nullptr, &hs_db_,
                                    &hs_compile_err_)) != HS_SUCCESS) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("compile pattern failed", K(ret), K(hs_flags_));
      if (hs_ret == HS_COMPILER_ERROR && hs_compile_err_ != nullptr) {

        LOG_USER_ERROR(OB_ERR_UNEXPECTED, hs_compile_err_->message);
      }
    } else if (OB_ISNULL(hs_db_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null hs database", K(ret));
    } else if (hs_alloc_scratch(hs_db_, &hs_scratch_) != HS_SUCCESS) {
      ret = OB_ERR_UNEXPECTED;
      ObString err_msg(hs_compile_err_->message);
      LOG_WARN("alloc hyper scan scratch failed", K(ret), K(err_msg));
      hs_free_database(hs_db_);
      hs_db_ = nullptr;
      hs_scratch_ = nullptr;
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObExprHsRegexCtx::match(ObExprStringBuf &string_buf,
                            const ObString &text,
                            const ObCollationType cs_type,
                            const int64_t start,
                            bool &result) const
{
  int ret = OB_SUCCESS;
  result = false;
  int64_t start_pos = -1;
  if (OB_UNLIKELY(!inited_ || OB_ISNULL(hs_db_) || OB_ISNULL(hs_scratch_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("hs regexp context not inited", K(ret), K(inited_), K(hs_scratch_), K(hs_db_));
  } else if (0 == text.length() || start >= text.length()) {
    //do nothing
  } else if (FALSE_IT(start_pos = ObCharset::charpos(cs_type, text.ptr(), text.length(), start))) {
  } else if (OB_UNLIKELY(start_pos >= text.length())) {
    // do nothing
  } else {
    MatchChain match_infos;
    hs_scratch_t *scratch = hs_scratch_;
    hs_error_t status;
    status = hs_scan(hs_db_, text.ptr() + start_pos, text.length() - start_pos, 0, scratch,
                     match_handler, &match_infos);

    if (OB_FAIL(check_hs_regexp_status(status))) {
      LOG_WARN("hs scan failed", K(ret), K(status));
    } else {
      result = (match_infos.count() > 0);
      LOG_DEBUG("Succeed to match", K(start), K(start_pos), K(text.length()), K(result));
    }
  }
  return ret;
}

int ObExprHsRegexCtx::find(ObExprStringBuf &string_buf,
                           const ObString &text,
                           const ObCollationType cs_type,
                           const int64_t start,
                           const int64_t occurrence,
                           const int64_t return_option,
                           const int64_t subexpr,
                           int64_t &result) const
{
  int ret = OB_SUCCESS;
  result = 0;
  int64_t start_pos = -1;
  UNUSED(subexpr);
  // compatible with mysql, any value less than 1, use occur = 1
  int64_t occur = (occurrence < 1 && lib::is_mysql_mode() ? 1 : occurrence);
  if (OB_UNLIKELY(!inited_ || OB_ISNULL(hs_db_) || OB_ISNULL(hs_scratch_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("hs regexp context not inited", K(ret), K(inited_), K(hs_scratch_), K(hs_db_));
  } else if (0 == text.length() || text.length() <= start) {
    //do nothing
  } else if (FALSE_IT(start_pos = ObCharset::charpos(cs_type, text.ptr(), text.length(), start))) {
  } else if (OB_UNLIKELY(start_pos >= text.length())) {
  } else {
    MatchChain match_infos;
    hs_scratch_t *scratch = hs_scratch_;
    hs_error_t status;
    status = hs_scan(hs_db_, text.ptr() + start_pos, text.length() - start_pos, 0, scratch, match_handler,
                     &match_infos);

    if (OB_FAIL(check_hs_regexp_status(status))) {
      LOG_WARN("scan failed", K(ret), K(status));
    } else if (match_infos.count() >= occur) {
      uint64_t begin = start_pos + match_infos.at(occur - 1).from_ + 1;
      uint64_t end = start_pos + match_infos.at(occur - 1).to_ + 1;
      result = return_option ? end : begin;
    }
    LOG_DEBUG("succeed to hs regexp substr", K(result), K(start), K(start_pos), K(occurrence),
              K(return_option), K(match_infos), K(match_infos.count()), K(text), K(text.length()));
  }
  return ret;
}

int ObExprHsRegexCtx::count(ObExprStringBuf &string_buf,
                            const ObString &text,
                            const ObCollationType cs_type,
                            const int32_t start,
                            int64_t &result) const
{
  int ret = OB_SUCCESS;
  result = 0;
  int64_t start_pos = -1;
  if (OB_UNLIKELY(!inited_ || OB_ISNULL(hs_db_) || OB_ISNULL(hs_scratch_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("hs regexp context not inited", K(ret), K(inited_), K(hs_scratch_), K(hs_db_));
  } else if (0 == text.length() || start >= text.length()) {
    //do nothing
  } else if (FALSE_IT(start_pos = ObCharset::charpos(cs_type, text.ptr(), text.length(), start))) {
  } else if (OB_UNLIKELY(start_pos >= text.length())) {
  } else {
    MatchChain match_infos;
    hs_scratch_t *scratch = hs_scratch_;
    hs_error_t status;
    status = hs_scan(hs_db_, text.ptr() + start_pos, text.length() - start_pos, 0, scratch, match_handler,
                     &match_infos);
    if (OB_FAIL(check_hs_regexp_status(status))) {
      LOG_WARN("scan failed", K(ret), K(status));
    } else {
      result = match_infos.count();
      LOG_DEBUG("succeed get hs regexp count", K(result), K(text), K(start), K(start_pos));
    }
  }
  return ret;
}

int ObExprHsRegexCtx::match_handler(unsigned int id, unsigned long long from, unsigned long long to,
                                    unsigned int flags, void *ctx)
{
  int ret = OB_SUCCESS;
  MatchChain *chain = static_cast<MatchChain *>(ctx);
  if (chain->empty()) {
    ret = chain->push_back(MatchInfo(from, to));
  } else if (chain->at(chain->count() - 1).from_ == from) {
    chain->at(chain->count() - 1).to_ = to;
  } else if (chain->at(chain->count() - 1).to_ <= from) {
    // exclude multiple matches. such as 'abc', pattern [a-z]{2} will match 'ab' and 'bc'.
    ret = chain->push_back(MatchInfo(from, to));
  }
  return ret;
}

int ObExprHsRegexCtx::substr(ObExprStringBuf &string_buf,
                             const ObString &text,
                             const ObCollationType cs_type,
                             const int64_t start,
                             const int64_t occurrence,
                             const int64_t subexpr,
                             ObString &result) const
{
  int ret = OB_SUCCESS;
  UNUSED(subexpr);
  result.reset();
  int64_t start_pos = -1;
  if (OB_UNLIKELY(!inited_ || OB_ISNULL(hs_db_) || OB_ISNULL(hs_scratch_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("hs regexp context not inited", K(ret), K(inited_), K(hs_scratch_), K(hs_db_));
  } else if (0 == text.length() || text.length() <= start) {
    // do nothing
  } else if (FALSE_IT(start_pos = ObCharset::charpos(cs_type, text.ptr(), text.length(), start))) {
  } else if (OB_UNLIKELY(start_pos >= text.length())) {
  } else {
    MatchChain match_infos;
    hs_scratch_t *scratch = hs_scratch_;
    hs_error_t status;
    status = hs_scan(hs_db_, text.ptr() + start_pos, text.length() - start_pos, 0, scratch, match_handler,
                     &match_infos);

    if (OB_FAIL(check_hs_regexp_status(status))) {
      LOG_WARN("scan failed", K(ret), K(status));
    } else if (match_infos.count() >= occurrence) {
      uint64_t begin_pos = start_pos + match_infos.at(occurrence - 1).from_;
      uint64_t end_pos = start_pos + match_infos.at(occurrence - 1).to_;
      char *res_buf = nullptr;
      int64_t res_len = end_pos - begin_pos;
      if (OB_ISNULL(res_buf = static_cast<char *>(string_buf.alloc(res_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(match_infos));
      } else {
        MEMCPY(res_buf, text.ptr() + begin_pos, res_len);
        result.assign_ptr(res_buf, res_len);
      }
    }
    LOG_DEBUG("succeed to hs regexp substr", K(result), K(start), K(start_pos), K(occurrence),
              K(match_infos), K(match_infos.count()), K(text), K(text.length()));
  }
  return ret;
}

// TODO: occurrency < 0
int ObExprHsRegexCtx::replace(ObExprStringBuf &string_buf,
                              const ObString &text_string,
                              const ObCollationType cs_type,
                              const ObString &replace_string,
                              const int64_t start,
                              const int64_t occurrence,
                              ObString &result) const
{
  int ret = OB_SUCCESS;
  result.reset();
  size_t start_pos = -1;
  if (OB_UNLIKELY(!inited_ || OB_ISNULL(hs_db_) || OB_ISNULL(hs_scratch_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("hs regexp context not inited", K(ret), K(inited_), K(hs_scratch_), K(hs_db_));
  } else if (0 == text_string.length() || text_string.length() <= start) {
    result = text_string;
  } else if (FALSE_IT(start_pos = ObCharset::charpos(cs_type, text_string.ptr(), text_string.length(), start))) {
  } else if (OB_UNLIKELY(start_pos >= text_string.length())) {
    result = text_string;
  } else {
    MatchChain match_infos;
    hs_scratch_t *scratch = hs_scratch_;
    hs_error_t status;
    status = hs_scan(hs_db_, text_string.ptr() + start_pos, text_string.length() - start_pos, 0, scratch,
                     match_handler, &match_infos);
    if (OB_FAIL(check_hs_regexp_status(status))) {
      LOG_WARN("scan failed", K(ret), K(status));
    } else if (match_infos.count() <= 0) {
      result = text_string;
    } else {
      // 1. calculate the length of result string
      // 2. modify from_ and to_ in match_infos according to start
      char *res_buf = nullptr;
      uint64_t res_len = 0;
      uint64_t last_to = 0;
      if (occurrence == 0) {
        for (int32_t i = 0; i < match_infos.count(); i++) {
          match_infos.at(i).from_ += start_pos;
          match_infos.at(i).to_ += start_pos;
          res_len += match_infos.at(i).from_ - last_to;
          res_len += replace_string.length();
          last_to = match_infos.at(i).to_;
        }
      } else if (occurrence <= match_infos.count()) {
          match_infos.at(occurrence - 1).from_ += start_pos;
          match_infos.at(occurrence - 1).to_ += start_pos;
          res_len += match_infos.at(occurrence - 1).from_;
          res_len += replace_string.length();
          last_to = match_infos.at(occurrence - 1).to_;
      }
      res_len += text_string.length() - last_to;
      // 3. replace string
      if (OB_ISNULL(res_buf = static_cast<char *>(string_buf.alloc(res_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(match_infos));
      } else {
        uint64_t last_to = 0, offset = 0;
        if (occurrence == 0) {
          for (int i = 0; i < match_infos.count(); i++) {
            MEMCPY(res_buf + offset,
                   text_string.ptr() + last_to,
                   match_infos.at(i).from_ - last_to);
            offset += match_infos.at(i).from_ - last_to;
            MEMCPY(res_buf + offset, replace_string.ptr(), replace_string.length());
            offset += replace_string.length();
            last_to = match_infos.at(i).to_;
          }
        } else if (occurrence <= match_infos.count()) {
          MEMCPY(res_buf, text_string.ptr(), match_infos.at(occurrence - 1).from_);
          offset += match_infos.at(occurrence - 1).from_;
          MEMCPY(res_buf + offset, replace_string.ptr(), replace_string.length());
          offset += replace_string.length();
          last_to = match_infos.at(occurrence - 1).to_;
        }
        MEMCPY(res_buf + offset, text_string.ptr() + last_to, text_string.length() - last_to);
        offset += text_string.length() - last_to;
        if (OB_UNLIKELY(offset != res_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected offset", K(ret), K(match_infos), K(text_string), K(offset));
        } else {
          result.assign_ptr(res_buf, offset);
        }
      }
    }
    LOG_DEBUG("succeed to regexp replace", K(result),
                                           K(match_infos),
                                           K(start),
                                           K(start_pos),
                                           K(occurrence),
                                           K(text_string),
                                           K(text_string.length()),
                                           K(replace_string),
                                           K(replace_string.length()));
  }
  return ret;
}

int ObExprHsRegexCtx::check_hs_regexp_status(hs_error_t status) const
{
  int ret = OB_SUCCESS;
  if (HS_SUCCESS == status) {
  } else {
    ret = OB_ERR_REGEXP_ERROR;
    LOG_WARN("other error in hyperscan regexp", K(ret), K(status));
  }
  return ret;
}
#endif

}
}

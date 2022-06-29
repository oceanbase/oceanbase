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
#include <stdlib.h>
#include <locale.h>
#include <cstring>
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/engine/expr/ob_expr_util.h"
namespace oceanbase {
using namespace common;
namespace sql {

void* ObInplaceAllocator::alloc(const int64_t size)
{
  void* mem = NULL;
  int ret = OB_SUCCESS;
  if (NULL == alloc_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prepare is needed", K(ret));
  } else {
    if (size < len_) {
      mem = mem_;
    } else {
      alloc_->free(mem_);
      len_ = next_pow2(size);
      mem_ = alloc_->alloc(len_);
      if (NULL == mem_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        mem = mem_;
        alloc_ = NULL;  // make sure allocate once for every prepare.
      }
    }
  }
  return mem;
}

ObExprRegexContext::ObExprRegexContext() : ObExprOperatorCtx(), inited_(false), reg_()
{}

ObExprRegexContext::~ObExprRegexContext()
{
  destroy();
}

void ObExprRegexContext::reset()
{
  destroy();
}

void ObExprRegexContext::reset_reg()
{
  reg_.re_magic = 0;
  reg_.re_nsub = 0;
  reg_.re_info = 0;
  reg_.re_csize = 0;
  reg_.re_endp = NULL;
  reg_.re_guts = NULL;
  reg_.re_fns = NULL;
}

int ObExprRegexContext::init(const ObString& pattern, int cflags, ObExprStringBuf& string_buf, const bool reusable)
{
  int ret = OB_SUCCESS;
  int regex_error_num = 0;
  if (OB_UNLIKELY(inited_ && !reusable)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret), K(this));
  } else if (pattern.length() < 0 || (pattern.length() > 0 && OB_ISNULL(pattern.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param pattern", K(ret), K(pattern));
  } else if (reusable && pattern_ == ObString(0, pattern.length(), pattern.ptr())) {
    // reuse the previous compile result.
  } else {
    if (inited_) {  // reusable && pattern changed
      ob_regfree(&reg_);
      reset_reg();
      inited_ = false;
    }
    pattern_allocator_.prepare(string_buf);
    pattern_wc_allocator_.prepare(string_buf);
    char* pattern_save = static_cast<char*>(pattern_allocator_.alloc(pattern.length()));
    if (NULL == pattern_save) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      MEMCPY(pattern_save, pattern.ptr(), pattern.length());
      pattern_.assign_ptr(pattern_save, pattern.length());
    }
    int64_t wc_pattern_length = 0;
    wchar_t* wc_pattern = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(getwc(pattern, wc_pattern, wc_pattern_length, string_buf))) {
      LOG_WARN("failed to getwc", K(ret));
    } else if (OB_ISNULL(wc_pattern)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("getwc function failed.", K(ret));
    } else {
      reg_.re_endp = wc_pattern + wc_pattern_length;
      regex_error_num = ob_re_wcomp(&reg_, wc_pattern, wc_pattern_length, (cflags | OB_REG_ADVANCED));
      if (OB_UNLIKELY(0 != regex_error_num)) {
        ret = convert_reg_err_code_to_ob_err_code(regex_error_num);
        LOG_WARN("regex compilation failed", K(ret));
        destroy();
      } else {
        inited_ = true;
      }
    }
  }
  return ret;
}

int ObExprRegexContext::match(
    const ObString& text, int64_t start_offset, bool& is_match, ObExprStringBuf& string_buf) const
{
  int ret = OB_SUCCESS;
  int regex_error_num = 0;
  is_match = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(this));
  } else if (text.length() < 0 || (text.length() > 0 && OB_ISNULL(text.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(text));
  } else {
    const static int64_t NMATCH = 1;
    ob_regmatch_t pmatch[NMATCH];
    wchar_t* wc_text = NULL;
    int64_t wc_length = 0;
    if (OB_FAIL(getwc(text, wc_text, wc_length, string_buf))) {
      LOG_WARN("failed to getwc", K(ret));
    } else if (OB_ISNULL(wc_text)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("getwc function failed.", K(ret));
    } else {
      pmatch[0].rm_so = start_offset;
      pmatch[0].rm_eo = wc_length;
      regex_error_num = ob_re_wexec((ob_regex_t*)&reg_, wc_text, wc_length, NULL, NMATCH, pmatch, 0);
      if (OB_UNLIKELY(0 != regex_error_num)) {
        if (OB_LIKELY(OB_REG_NOMATCH == regex_error_num)) {
          is_match = false;
          LOG_TRACE("regex not match", K(ret));
        } else {
          ret = convert_reg_err_code_to_ob_err_code(regex_error_num);
          LOG_WARN("regex match error", K(ret));
        }
      } else {
        is_match = true;
      }
    }
  }
  return ret;
}

int ObExprRegexContext::substr(const ObString& text, int64_t occurrence, int64_t subexpr, ObString& sub,
    ObExprStringBuf& string_buf, bool from_begin, bool from_end, ObSEArray<uint32_t, 4>& begin_locations) const
{
  int ret = OB_SUCCESS;
  sub.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(this));
  } else if (reg_.re_nsub >= subexpr) {
    size_t nsub = reg_.re_nsub;
    ob_regmatch_t pmatch[nsub + 1];
    int error = 0;
    int64_t tmp_start = 0;
    int64_t start = 0;
    wchar_t* wc_text = NULL;
    int64_t wc_length = 0;
    if (OB_FAIL(getwc(text, wc_text, wc_length, string_buf))) {
      LOG_WARN("failed to getwc", K(ret));
    } else if (OB_ISNULL(wc_text)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("getwc function failed.", K(ret));
    } else if (begin_locations.empty()) {
      /*do nothing*/
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < begin_locations.count() && occurrence > 0; ++idx) {
        start = begin_locations.at(idx);
        do {
          tmp_start = start;
          pmatch[0].rm_so = start;
          pmatch[0].rm_eo = wc_length;
          error = ob_re_wexec((ob_regex_t*)&reg_, wc_text, wc_length, NULL, nsub + 1, pmatch, 0);
          if (OB_UNLIKELY(0 != error)) {
            if (OB_LIKELY(OB_REG_NOMATCH == error)) {
              LOG_TRACE("regex not match, str=%.*s", K(wc_length));
            } else {
              ret = convert_reg_err_code_to_ob_err_code(error);
              ;
              LOG_WARN("regex match error", K(ret));
            }
          } else if (pmatch[0].rm_eo + tmp_start == start && start < wc_length) {
            start = start + 1;
          } else {
            start = static_cast<int64_t>(pmatch[0].rm_eo + tmp_start);
          }
        } while (OB_SUCC(ret) && --occurrence > 0 && 0 == error && tmp_start < wc_length && !from_begin && !from_end);
      }
      if (OB_SUCC(ret)) {
        if (0 == error && 0 == occurrence) {
          if (pmatch[subexpr].rm_so < 0 || pmatch[subexpr].rm_so > wc_length || pmatch[subexpr].rm_eo < 0 ||
              pmatch[subexpr].rm_eo > wc_length || OB_UNLIKELY(tmp_start + pmatch[subexpr].rm_so > wc_length)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("regexp library error, get invalid offset from pmatch", K(ret));
          } else {
            int64_t offset = static_cast<int64_t>(pmatch[subexpr].rm_eo - pmatch[subexpr].rm_so);
            if (offset > 0) {
              char* tmp_char = NULL;
              int64_t length_chr = 0;
              if (OB_FAIL(w2c(wc_text + pmatch[subexpr].rm_so + tmp_start, offset, tmp_char, length_chr, string_buf))) {
                LOG_WARN("failed to w2c.", K(ret));
              } else if (OB_ISNULL(tmp_char)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("w2c function failed.", K(ret));
              } else {
                sub.assign_ptr(tmp_char, length_chr);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexContext::count_match_str(const ObString& text, int64_t subexpr, int64_t& sub,
    ObExprStringBuf& string_buf, bool from_begin, bool from_end, ObSEArray<uint32_t, 4>& begin_locations) const
{
  int ret = OB_SUCCESS;
  sub = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(this));
  } else if (reg_.re_nsub >= subexpr) {
    size_t nsub = reg_.re_nsub;
    ob_regmatch_t pmatch[nsub + 1];
    int error = 0;
    int64_t count = 0;
    int64_t tmp_start = 0;
    int64_t start = 0;
    wchar_t* wc_text = NULL;
    int64_t wc_length = 0;
    if (OB_FAIL(getwc(text, wc_text, wc_length, string_buf))) {
      LOG_WARN("failed to getwc", K(ret));
    } else if (OB_ISNULL(wc_text)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("getwc function failed.", K(ret));
    } else if (begin_locations.empty()) {
      /*do nothing*/
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < begin_locations.count(); ++idx) {
        start = begin_locations.at(idx);
        do {
          tmp_start = start;
          pmatch[0].rm_so = start;
          pmatch[0].rm_eo = wc_length;
          error = ob_re_wexec((ob_regex_t*)&reg_, wc_text, wc_length, NULL, nsub + 1, pmatch, 0);
          if (OB_UNLIKELY(0 != error)) {
            if (OB_LIKELY(OB_REG_NOMATCH == error)) {
              LOG_TRACE("regex not match, ", K(wc_length));
            } else {
              ret = convert_reg_err_code_to_ob_err_code(error);
              LOG_WARN("regex match error", K(ret));
            }
          } else if (pmatch[0].rm_eo + tmp_start == start && start < wc_length) {
            start = start + 1;
            ++count;
          } else {
            start = static_cast<int32_t>(pmatch[0].rm_eo + tmp_start);
            ++count;
          }
        } while (OB_SUCC(ret) && 0 == error && tmp_start < wc_length && !from_begin && !from_end);
      }
      if (OB_SUCC(ret)) {
        if (0 == error) {
          if (pmatch[subexpr].rm_so < 0 || pmatch[subexpr].rm_so > wc_length || pmatch[subexpr].rm_eo < 0 ||
              pmatch[subexpr].rm_eo > wc_length) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("regexp library error, get invalid offset from pmatch", K(ret));
          }
        }
        sub = count;
      }
    }
  }
  return ret;
}

int ObExprRegexContext::instr(const ObString& text, int64_t occurrence, int64_t return_option, int64_t subexpr,
    int64_t& sub, ObExprStringBuf& string_buf, bool from_begin, bool from_end,
    ObSEArray<uint32_t, 4>& begin_locations) const
{
  int ret = OB_SUCCESS;
  sub = -1;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(this));
  } else if (reg_.re_nsub >= subexpr) {
    size_t nsub = reg_.re_nsub;
    ob_regmatch_t pmatch[nsub + 1];
    int error = 0;
    int64_t tmp_start = 0;
    int64_t start = 0;
    wchar_t* wc_text = NULL;
    int64_t wc_length = 0;
    if (OB_FAIL(getwc(text, wc_text, wc_length, string_buf))) {
      LOG_WARN("failed to getwc", K(ret));
    } else if (OB_ISNULL(wc_text)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("getwc function failed.", K(ret));
    } else if (begin_locations.empty()) {
      /*do nothing*/
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < begin_locations.count() && occurrence > 0; ++idx) {
        start = begin_locations.at(idx);
        do {
          tmp_start = start;
          pmatch[0].rm_so = start;
          pmatch[0].rm_eo = wc_length;
          error = ob_re_wexec((ob_regex_t*)&reg_, wc_text, wc_length, NULL, nsub + 1, pmatch, 0);
          if (OB_UNLIKELY(0 != error)) {
            if (OB_LIKELY(OB_REG_NOMATCH == error)) {
              LOG_TRACE("regex not match", K(ret));
            } else {
              ret = convert_reg_err_code_to_ob_err_code(error);
              LOG_WARN("regex match error", K(ret));
            }
          } else if (pmatch[0].rm_eo + tmp_start == start && start < wc_length) {
            start = start + 1;
          } else {
            start = static_cast<int64_t>(pmatch[0].rm_eo + tmp_start);
          }
        } while (OB_SUCC(ret) && --occurrence > 0 && 0 == error && tmp_start < wc_length && !from_begin && !from_end);
      }
      if (OB_SUCC(ret)) {
        if (0 == error && 0 == occurrence) {
          if (pmatch[subexpr].rm_so < 0 || pmatch[subexpr].rm_so > wc_length || pmatch[subexpr].rm_eo < 0 ||
              pmatch[subexpr].rm_eo > wc_length || OB_UNLIKELY(tmp_start + pmatch[subexpr].rm_so > wc_length)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("regexp library error, get invalid offset from pmatch", K(ret));
          } else {
            sub = return_option ? pmatch[subexpr].rm_eo + tmp_start : pmatch[subexpr].rm_so + tmp_start;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexContext::like(const ObString& text, int64_t occurrence, bool& sub, ObExprStringBuf& string_buf,
    bool from_begin, bool from_end, ObSEArray<uint32_t, 4>& begin_locations) const
{
  int ret = OB_SUCCESS;
  sub = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(this));
  } else if (text.length() < 0 || (text.length() > 0 && OB_ISNULL(text.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, source text is null", K(ret), K(text));
  } else {
    size_t nsub = reg_.re_nsub;
    ob_regmatch_t pmatch[nsub + 1];
    int error = 0;
    int64_t tmp_start = 0;
    int64_t start = 0;
    wchar_t* wc_text = NULL;
    int64_t wc_length = 0;
    if (OB_FAIL(getwc(text, wc_text, wc_length, string_buf))) {
      LOG_WARN("failed to getwc", K(ret));
    } else if (OB_ISNULL(wc_text)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("getwc function failed.", K(ret));
    } else if (begin_locations.empty()) {
      /*do nothing*/
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < begin_locations.count() && occurrence > 0; ++idx) {
        start = begin_locations.at(idx);
        do {
          tmp_start = start;
          pmatch[0].rm_so = start;
          pmatch[0].rm_eo = wc_length;
          error = ob_re_wexec((ob_regex_t*)&reg_, wc_text, wc_length, NULL, nsub + 1, pmatch, 0);
          if (OB_UNLIKELY(0 != error)) {
            if (OB_LIKELY(OB_REG_NOMATCH == error)) {
              LOG_TRACE("regex not match", K(ret));
            } else {
              ret = convert_reg_err_code_to_ob_err_code(error);
              LOG_WARN("regex match error", K(error));
            }
          } else if (pmatch[0].rm_eo + tmp_start == start && start < wc_length) {
            start = start + 1;
          } else {
            start = static_cast<int64_t>(pmatch[0].rm_eo + tmp_start);
          }
        } while (OB_SUCC(ret) && --occurrence > 0 && 0 == error && tmp_start < wc_length && !from_begin && !from_end);
      }
      if (OB_SUCC(ret)) {
        if (0 == error && 0 == occurrence) {
          if (pmatch[0].rm_so < 0 || pmatch[0].rm_so > wc_length || pmatch[0].rm_eo < 0 ||
              pmatch[0].rm_eo > wc_length) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("regexp library error, get invalid offset from pmatch", K(ret));
          } else {
            sub = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexContext::replace_substr(const ObString& text_string, const ObString& replace_string, int64_t occurrence,
    ObExprStringBuf& string_buf, ObIArray<size_t>& ch_position, ObString& sub, bool from_begin, bool from_end,
    ObSEArray<uint32_t, 4>& begin_locations) const
{
  const char* text = text_string.ptr();
  char* tmp = NULL;
  sub.reset();
  int ret = OB_SUCCESS;
  ObSEArray<uint32_t, 4> locations_and_length(
      common::ObModIds::OB_SQL_EXPR_REPLACE, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  int64_t tmp_start = 0;
  int64_t start = 0;
  ObSEArray<ObSEArray<ObString, 8>, 8> subexpr_arrays;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("regexp context not inited yet", K(ret), K(this));
  } else if (OB_ISNULL(text)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, source text is null", K(ret), K(text));
  } else if (begin_locations.empty()) {
    sub = text_string;
  } else if (reg_.re_nsub >= 0) {
    size_t nsub = reg_.re_nsub;
    int error = 0;
    wchar_t* wc_text = NULL;
    int64_t length = 0;
    if (OB_FAIL(getwc(text_string, wc_text, length, string_buf))) {
      LOG_WARN("failed to getwc", K(ret));
    } else if (OB_ISNULL(wc_text)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("getwc function failed.", K(ret));
    } else {
      if (0 != occurrence) {
        ob_regmatch_t pmatch[nsub + 1];
        ObSEArray<ObString, 8> subexpr_array;
        for (int64_t idx = 0; OB_SUCC(ret) && idx < begin_locations.count() && occurrence > 0; ++idx) {
          start = begin_locations.at(idx);
          do {
            tmp_start = start;
            pmatch[0].rm_so = start;
            pmatch[0].rm_eo = length;
            error = ob_re_wexec((ob_regex_t*)&reg_, wc_text, length, NULL, nsub + 1, pmatch, 0);
            if (OB_UNLIKELY(0 != error)) {
              if (OB_LIKELY(OB_REG_NOMATCH == error)) {
                LOG_TRACE("regex not match", K(length));
              } else {
                ret = convert_reg_err_code_to_ob_err_code(error);
                LOG_WARN("regex match error", K(ret));
              }
            } else if (pmatch[0].rm_eo + tmp_start == start && start < length) {
              start = start + 1;
            } else {
              start = static_cast<int64_t>(pmatch[0].rm_eo + tmp_start);
            }
          } while (OB_SUCC(ret) && --occurrence > 0 && 0 == error && tmp_start < length && !from_begin && !from_end);
        }
        if (OB_SUCC(ret)) {
          if (0 == error && 0 == occurrence) {
            if (pmatch[0].rm_so < 0 || pmatch[0].rm_so > length || pmatch[0].rm_eo < 0 || pmatch[0].rm_eo > length) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("regexp library error, get invalid offset from pmatch", K(ret));
            } else if (extract_subpre_string(wc_text, length, tmp_start, pmatch, nsub + 1, string_buf, subexpr_array)) {
              LOG_WARN("failed to extract subpre string", K(ret));
            } else if (OB_FAIL(subexpr_arrays.push_back(subexpr_array))) {
              LOG_WARN("failed to push back subexpr array", K(ret));
            } else if (pmatch[0].rm_eo == pmatch[0].rm_so) {
              if (OB_UNLIKELY(pmatch[0].rm_so + tmp_start >= ch_position.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("beyond array boundary", K(ret), K(pmatch[0].rm_so + tmp_start), K(ch_position.count()));
              } else if (OB_FAIL(locations_and_length.push_back(ch_position.at(pmatch[0].rm_so + tmp_start))) ||
                         OB_FAIL(locations_and_length.push_back(0))) {
                LOG_WARN("locations_and_length push_back error", K(ret));
              } else if (OB_FAIL(replace(
                             text_string, replace_string, locations_and_length, string_buf, subexpr_arrays, sub))) {
                LOG_WARN("replace function failed", K(ret));
              }
            } else {
              int64_t length_chr = 0;
              if (OB_FAIL(w2c(wc_text + pmatch[0].rm_so + tmp_start,
                      pmatch[0].rm_eo - pmatch[0].rm_so,
                      tmp,
                      length_chr,
                      string_buf))) {
                LOG_WARN("failed to w2c.", K(ret));
              } else if (OB_ISNULL(tmp)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("w2c function failed", K(ret));
              } else if (OB_UNLIKELY(pmatch[0].rm_so + tmp_start >= ch_position.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("beyond array boundary", K(ret), K(pmatch[0].rm_so + tmp_start), K(ch_position.count()));
              } else if (OB_FAIL(locations_and_length.push_back(ch_position.at(pmatch[0].rm_so + tmp_start))) ||
                         OB_FAIL(locations_and_length.push_back(length_chr))) {
                LOG_WARN("locations_and_length push_back error", K(ret));
              } else if (OB_FAIL(replace(
                             text_string, replace_string, locations_and_length, string_buf, subexpr_arrays, sub))) {
                LOG_WARN("replace function failed", K(ret));
              }
            }
          } else {
            sub = text_string;
          }
        } else {
          /*do nothing */
        }
      } else {
        for (int64_t idx = 0; OB_SUCC(ret) && idx < begin_locations.count(); ++idx) {
          start = begin_locations.at(idx);
          do {
            ob_regmatch_t pmatch[nsub + 1];
            tmp_start = start;
            pmatch[0].rm_so = start;
            pmatch[0].rm_eo = length;
            ObSEArray<ObString, 8> subexpr_array;
            error = ob_re_wexec((ob_regex_t*)&reg_, wc_text, length, NULL, nsub + 1, pmatch, 0);
            if (OB_UNLIKELY(0 != error)) {
              if (OB_LIKELY(OB_REG_NOMATCH == error)) {
                LOG_TRACE("regex not match", K(length), K(text + start));
              } else {
                ret = convert_reg_err_code_to_ob_err_code(error);
                LOG_WARN("regex match error", K(ret));
              }
            } else if (pmatch[0].rm_eo == pmatch[0].rm_so) {
              if (OB_UNLIKELY(pmatch[0].rm_so + tmp_start >= ch_position.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("beyond array boundary", K(ret), K(pmatch[0].rm_so + tmp_start), K(ch_position.count()));
              } else if (OB_FAIL(locations_and_length.push_back(ch_position.at(pmatch[0].rm_so + tmp_start))) ||
                         OB_FAIL(locations_and_length.push_back(0))) {
                LOG_WARN("locations_and_length push_back error", K(ret));
              } else if (extract_subpre_string(
                             wc_text, length, tmp_start, pmatch, nsub + 1, string_buf, subexpr_array)) {
                LOG_WARN("failed to extract subpre string", K(ret));
              } else if (OB_FAIL(subexpr_arrays.push_back(subexpr_array))) {
                LOG_WARN("failed to push back subexpr array", K(ret));
              } else if (pmatch[0].rm_eo + tmp_start == start && start < length) {
                start = start + 1;
              } else {
                start = static_cast<int64_t>(pmatch[0].rm_eo + tmp_start);
              }
            } else {
              int64_t length_chr = 0;
              if (OB_FAIL(w2c(wc_text + pmatch[0].rm_so + tmp_start,
                      pmatch[0].rm_eo - pmatch[0].rm_so,
                      tmp,
                      length_chr,
                      string_buf))) {
                LOG_WARN("failed to w2c.", K(ret));
              } else if (OB_ISNULL(tmp)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("w2c function failed", K(ret));
              } else if (OB_UNLIKELY(pmatch[0].rm_so + tmp_start >= ch_position.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("beyond array boundary", K(ret), K(pmatch[0].rm_so + tmp_start), K(ch_position.count()));
              } else if (extract_subpre_string(
                             wc_text, length, tmp_start, pmatch, nsub + 1, string_buf, subexpr_array)) {
                LOG_WARN("failed to extract subpre string", K(ret));
              } else if (OB_FAIL(subexpr_arrays.push_back(subexpr_array))) {
                LOG_WARN("failed to push back subexpr array", K(ret));
              } else if (OB_FAIL(locations_and_length.push_back(ch_position.at(pmatch[0].rm_so + tmp_start))) ||
                         OB_FAIL(locations_and_length.push_back(length_chr))) {
                LOG_WARN("locations_and_length push_back error", K(ret));
              } else if (pmatch[0].rm_eo + tmp_start == start && start < length) {
                start = start + 1;
              } else {
                start = static_cast<int64_t>(pmatch[0].rm_eo + tmp_start);
              }
            }
          } while (OB_SUCC(ret) && tmp_start < length && 0 == error && !from_begin && !from_end);
        }
        if (OB_SUCC(ret)) {
          if (!locations_and_length.empty()) {
            if (OB_FAIL(replace(text_string, replace_string, locations_and_length, string_buf, subexpr_arrays, sub))) {
              LOG_WARN("replace function failed", K(ret));
            }
          } else {
            sub = text_string;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRegexContext::replace(const ObString& text, const ObString& to, ObSEArray<uint32_t, 4>& locations_and_length,
    ObExprStringBuf& string_buf, ObIArray<ObSEArray<ObString, 8> >& subexpr_arrays, ObString& sub) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 8> to_strings;
  sub.reset();
  if (OB_UNLIKELY(text.length() <= 0 || to.length() < 0 || locations_and_length.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("text_string or to_string or locations_and_length is null or invalid", K(ret));
  } else if (OB_FAIL(pre_process_replace_str(text, to, string_buf, subexpr_arrays, to_strings))) {
    LOG_WARN("failed to pre process replace str", K(ret));
  } else {
    int64_t sum_length = 0;
    int64_t length_text = text.length();
    int64_t length_to = to.length();
    int64_t tot_length = 0;
    int64_t count_location = locations_and_length.count();
    for (int64_t i = 1; i < count_location; i = i + 2) {
      sum_length += locations_and_length.at(i);
    }
    if (OB_UNLIKELY(
            OB_MAX_VARCHAR_LENGTH < count_location / 2 * (length_to + length_text) + length_text - sum_length)) {
      ret = OB_ERR_VARCHAR_TOO_LONG;
      LOG_WARN("Result of replace_all_str() was larger than OB_MAX_VARCHAR_LENGTH.",
          K(length_text),
          K(length_to),
          K(OB_MAX_VARCHAR_LENGTH),
          K(ret));
    } else if (OB_UNLIKELY(
                   (tot_length = count_location / 2 * (length_to + length_text) + length_text - sum_length) < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the tot_length is invalid", K(length_text), K(tot_length), K(ret));
    } else if (tot_length != 0) {
      char* buf = static_cast<char*>(string_buf.alloc(tot_length));
      int64_t real_tot_length = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed.", K(tot_length), K(ret));
      } else {
        MEMSET(buf, 0, tot_length);
        int64_t pos = 0;
        const char* const text_ptr_start = text.ptr();
        const char* const text_ptr_end = text.ptr() + length_text;
        const char* text_ptr_lower = text.ptr();
        const char* text_ptr_upper = text.ptr();
        const char* const buf_ptr_end = buf + tot_length;
        char* tmp_buf = buf;
        for (int64_t i = 0, j = 0; i < count_location && j < to_strings.count(); i = i + 2, ++j) {
          const char* to_ptr = to_strings.at(j).ptr();
          pos = locations_and_length.at(i);
          text_ptr_upper = text_ptr_start + pos;
          if (OB_UNLIKELY(text_ptr_upper - text_ptr_lower < 0 || text_ptr_end - text_ptr_upper < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "get offset is invalid", K(text_ptr_upper - text_ptr_lower), K(text_ptr_end - text_ptr_upper), K(ret));
          } else {
            MEMCPY(tmp_buf, text_ptr_lower, text_ptr_upper - text_ptr_lower);
            tmp_buf += text_ptr_upper - text_ptr_lower;
            real_tot_length += text_ptr_upper - text_ptr_lower;
            text_ptr_lower = text_ptr_upper + locations_and_length.at(i + 1);
            if (OB_UNLIKELY(to_strings.at(j).length() < 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get offset is invalid", K(to_strings.at(j).length()), K(ret));
            } else {
              MEMCPY(tmp_buf, to_ptr, to_strings.at(j).length());
              tmp_buf += to_strings.at(j).length();
              real_tot_length += to_strings.at(j).length();
            }
          }
        }
        if (text_ptr_lower < text_ptr_end) {
          MEMCPY(tmp_buf, text_ptr_lower, text_ptr_end - text_ptr_lower);
          real_tot_length += text_ptr_end - text_ptr_lower;
        }
        if (OB_UNLIKELY(real_tot_length > tot_length)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid argument", K(real_tot_length), K(tot_length), K(ret));
        } else if (real_tot_length > 0) {
          char *real_buf = static_cast<char *>(string_buf.alloc(real_tot_length));
          if (OB_ISNULL(real_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed.", K(real_buf), K(ret));
          } else {
            MEMSET(real_buf, 0, real_tot_length);
            MEMCPY(real_buf, buf, real_tot_length);
            sub.assign_ptr(real_buf, static_cast<int64_t>(real_tot_length));
          }
        }
      }
    }
  }
  return ret;
}

// convert char to wchar_t
int ObExprRegexContext::getwc(const ObString& text, wchar_t*& wc, int64_t& wc_length, ObExprStringBuf& string_buf) const
{
  // %string_buf may be instance of ObInplaceAllocator, can only allocate once here.
  int ret = OB_SUCCESS;
  wc = NULL;
  wc_length = 0;
  if (text.length() < 0 || (text.length() > 0 && OB_ISNULL(text.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source text is null or length is invalid", K(ret), K(text));
  } else {
    int64_t length = text.length();
    ObSEArray<size_t, 4> byte_num;
    ObSEArray<size_t, 4> byte_offsets;
    wc = static_cast<wchar_t*>(string_buf.alloc((length + 1) * sizeof(wchar_t)));
    if (OB_ISNULL(wc)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed.", K(wc), K(ret));
    } else if (OB_FAIL(ObExprUtil::get_mb_str_info(
                   text, ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4), byte_num, byte_offsets))) {
      LOG_WARN("failed to get mb str info", K(ret));
    } else if (OB_UNLIKELY(byte_num.count() >= length + 1 ||
                           (byte_num.count() != 0 && byte_num.count() != byte_offsets.count() - 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(length + 1), K(byte_num.count()), K(byte_offsets.count()));
    } else {
      wmemset(wc, 0, length + 1);
      wc_length = byte_num.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < byte_num.count(); ++i) {
        int32_t wc_int = 0;
        int32_t unused_length = 0;
        if (OB_UNLIKELY(byte_offsets.at(i) + byte_num.at(i) > text.length())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(byte_offsets.at(i)), K(byte_num.at(i)), K(ret));
        } else if (OB_FAIL(ObCharset::mb_wc(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4),
                       text.ptr() + byte_offsets.at(i),
                       byte_num.at(i),
                       unused_length,
                       wc_int))) {
          LOG_WARN("failed to multi byte to wide char", K(ret));
        } else {
          wc[i] = static_cast<wchar_t>(wc_int);
        }
      }
    }
  }
  return ret;
}

// convert wchar_t to char
int ObExprRegexContext::w2c(
    const wchar_t* wc, int64_t length, char*& chr, int64_t& chr_length, ObExprStringBuf& string_buf) const
{
  int ret = OB_SUCCESS;
  chr_length = 0;
  chr = NULL;
  if (length < 0 || (length > 0 && OB_ISNULL(wc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source text is null or length is vaild", K(ret), K(wc), K(length));
  } else if (length > 0) {
    int32_t buff_len = sizeof(wchar_t);
    int64_t chr_len = (length + 1) * buff_len;
    char *buff = NULL;
    if (OB_ISNULL(buff = static_cast<char *>(string_buf.alloc(buff_len))) ||
        OB_ISNULL(chr = static_cast<char *>(string_buf.alloc(chr_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed.", K(buff), K(chr), K(ret));
    } else {
      char *tmp_chr = chr;
      MEMSET(chr, 0, chr_len);
      MEMSET(buff, 0, buff_len);
      for (int64_t i = 0; OB_SUCC(ret) && i < length; ++i) {
        int32_t wc_int = static_cast<int32_t>(wc[i]);
        int32_t real_length = 0;
        if (OB_FAIL(ObCharset::wc_mb(
                ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4), wc_int, buff, buff_len, real_length))) {
          LOG_WARN("failed to multi byte to wide char", K(ret));
        } else if (OB_UNLIKELY(chr_length + real_length > chr_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(chr_length), K(real_length), K(chr_len), K(ret));
        } else {
          MEMCPY(tmp_chr, buff, real_length);
          chr_length += real_length;
          tmp_chr = tmp_chr + real_length;
        }
      }
    }
  }
  return ret;
}

void ObExprRegexContext::destroy()
{
  if (inited_) {
    ob_regfree(&reg_);
    reset_reg();
    inited_ = false;
  }
}

int ObExprRegexContext::pre_process_replace_str(const ObString& text, const ObString& to, ObExprStringBuf& string_buf,
    ObIArray<ObSEArray<ObString, 8> >& subexpr_arrays, ObIArray<ObString>& to_strings) const
{
  int ret = OB_SUCCESS;
  int64_t length_text = text.length();
  int64_t length_to = to.length();
  int64_t tot_length = 0;
  if (OB_UNLIKELY(OB_MAX_VARCHAR_LENGTH < (tot_length = length_text + length_to))) {
    ret = OB_ERR_VARCHAR_TOO_LONG;
    LOG_WARN("Result of replace_all_str() was larger than OB_MAX_VARCHAR_LENGTH.",
        K(length_text),
        K(length_to),
        K(OB_MAX_VARCHAR_LENGTH),
        K(ret));
  } else if (tot_length != 0) {
    char* buf = static_cast<char*>(string_buf.alloc(tot_length));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed.", K(tot_length), K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < subexpr_arrays.count(); ++j) {
        ObString tmp_string;
        ObIArray<ObString>& subexpr_array = subexpr_arrays.at(j);
        MEMSET(buf, 0, tot_length);
        const char* const to_ptr = to.ptr();
        char* tmp_buf = buf;
        int64_t i = 0;
        int64_t real_tot_length = 0;
        bool need_copy_last = true;
        bool before_is_escape = false;
        if (length_to != 0) {
          for (; i < length_to - 1; ++i) {
            need_copy_last = true;
            if (to_ptr[i] == '\\') {
              if (to_ptr[i + 1] == '\\') {
                MEMCPY(tmp_buf, to_ptr + i, 1);
                tmp_buf += 1;
                ++real_tot_length;
                ++i;
                before_is_escape = true;
                need_copy_last = (length_to - 1 != i);
              } else if (!before_is_escape) {
                if (to_ptr[i + 1] >= '1' && to_ptr[i + 1] <= '9') {
                  int64_t idx = to_ptr[i + 1] - '1';
                  if (OB_UNLIKELY(idx < 0 || idx >= subexpr_array.count())) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("get unexpected error", K(ret), K(idx), K(subexpr_array));
                  } else {
                    MEMCPY(tmp_buf, subexpr_array.at(idx).ptr(), subexpr_array.at(idx).length());
                    tmp_buf += subexpr_array.at(idx).length();
                    real_tot_length += subexpr_array.at(idx).length();
                    ++i;
                    need_copy_last = (length_to - 1 != i);
                  }
                } else {
                  MEMCPY(tmp_buf, to_ptr + i, 1);
                  tmp_buf += 1;
                  ++real_tot_length;
                }
              } else {
                MEMCPY(tmp_buf, to_ptr + i, 1);
                tmp_buf += 1;
                ++real_tot_length;
                before_is_escape = false;
              }
            } else {
              before_is_escape = false;
              MEMCPY(tmp_buf, to_ptr + i, 1);
              tmp_buf += 1;
              ++real_tot_length;
            }
          }
          if (need_copy_last) {
            MEMCPY(tmp_buf, to_ptr + length_to - 1, 1);
            ++real_tot_length;
          }
          char *real_buf = static_cast<char *>(string_buf.alloc(real_tot_length));
          if (OB_ISNULL(real_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed.", K(real_buf), K(ret));
          } else {
            MEMSET(real_buf, 0, real_tot_length);
            MEMCPY(real_buf, buf, real_tot_length);
            tmp_string.assign_ptr(real_buf, static_cast<int64_t>(real_tot_length));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(to_strings.push_back(tmp_string))) {
          LOG_WARN("failed to push back string", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprRegexContext::extract_subpre_string(const wchar_t* wc_text, int64_t wc_length, int64_t start_pos,
    ob_regmatch_t pmatch[], uint64_t pmatch_size, ObExprStringBuf& string_buf, ObIArray<ObString>& subexpr_array) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wc_text) || OB_ISNULL(pmatch) || OB_UNLIKELY(wc_length < 0 || start_pos < 0 || pmatch_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected nullptr", K(ret), K(wc_text), K(wc_length), K(start_pos), K(pmatch), K(pmatch_size));
  } else {
    for (int64_t i = 1; OB_SUCC(ret) && i < pmatch_size; ++i) {
      ObString sub;
      if (OB_UNLIKELY(start_pos + pmatch[i].rm_so > wc_length)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("regexp library error, get invalid offset from pmatch", K(ret));
      } else {
        int64_t offset = static_cast<int64_t>(pmatch[i].rm_eo - pmatch[i].rm_so);
        if (offset > 0) {
          char* tmp_char = NULL;
          int64_t length = 0;
          if (OB_FAIL(w2c(wc_text + pmatch[i].rm_so + start_pos, offset, tmp_char, length, string_buf))) {
            LOG_WARN("failed to w2c.", K(ret));
          } else if (OB_ISNULL(tmp_char)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("w2c function failed.", K(ret));
          } else {
            sub.assign_ptr(tmp_char, length);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(subexpr_array.push_back(sub))) {
            LOG_WARN("failed to push back string", K(ret));
          }
        }
      }
    }
    for (int64_t i = pmatch_size; OB_SUCC(ret) && i < 10; ++i) {
      ObString sub;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(subexpr_array.push_back(sub))) {
          LOG_WARN("failed to push back string", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprRegexContext::convert_reg_err_code_to_ob_err_code(int reg_err) const
{
  int ret = OB_ERR_REGEXP_ERROR;
  switch (reg_err) {
    case OB_REG_NOMATCH:
      ret = OB_ERR_REGEXP_NOMATCH;
      break;
    case OB_REG_BADPAT:
      ret = OB_ERR_REGEXP_BADPAT;
      break;
    case OB_REG_ECOLLATE:
      ret = OB_ERR_REGEXP_ECOLLATE;
      break;
    case OB_REG_ECTYPE:
      ret = OB_ERR_REGEXP_ECTYPE;
      break;
    case OB_REG_EESCAPE:
      ret = OB_ERR_REGEXP_EESCAPE;
      break;
    case OB_REG_ESUBREG:
      ret = OB_ERR_REGEXP_ESUBREG;
      break;
    case OB_REG_EBRACK:
      ret = OB_ERR_REGEXP_EBRACK;
      break;
    case OB_REG_EPAREN:
      ret = OB_ERR_REGEXP_EPAREN;
      break;
    case OB_REG_EBRACE:
      ret = OB_ERR_REGEXP_EBRACE;
      break;
    case OB_REG_BADBR:
      ret = OB_ERR_REGEXP_BADBR;
      break;
    case OB_REG_ERANGE:
      ret = OB_ERR_REGEXP_ERANGE;
      break;
    case OB_REG_ESPACE:
      ret = OB_SIZE_OVERFLOW;
      break;
    case OB_REG_BADRPT:
      ret = OB_ERR_REGEXP_BADRPT;
      break;
    case OB_REG_ASSERT:
      ret = OB_ERR_REGEXP_ASSERT;
      break;
    case OB_REG_INVARG:
      ret = OB_ERR_REGEXP_INVARG;
      break;
    case OB_REG_MIXED:
      ret = OB_ERR_REGEXP_MIXED;
      break;
    case OB_REG_BADOPT:
      ret = OB_ERR_REGEXP_BADOPT;
      break;
    case OB_REG_ETOOBIG:
      ret = OB_ERR_REGEXP_ETOOBIG;
      break;
    default:
      break;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

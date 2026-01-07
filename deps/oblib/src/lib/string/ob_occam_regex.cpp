/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_occam_regex.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string_holder.h"
#include <cstring>
#include <regex.h>

namespace oceanbase
{
namespace common
{

struct CStringHolder {// RAII to ensure resource released
  CStringHolder() : c_str_(nullptr), len_(0) {}
  ~CStringHolder() {
    if (OB_NOT_NULL(c_str_)) {
      ob_free(c_str_);
    }
    new (this) CStringHolder();
  }
  int init(const ObString &obstring) {
    int ret = 0;
    if (OB_NOT_NULL(c_str_)) {
      ret = OB_INIT_TWICE;
      OCCAM_LOG(WARN, "has been inited", K(obstring), K_(c_str));
    } else if (OB_ISNULL(c_str_ = (char *)ob_malloc(obstring.length() + 1/*add '\0'*/, "CStrHolder"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OCCAM_LOG(WARN, "failed to alloc cstring buffer memory", K(obstring));
    } else {
      memcpy(c_str_, obstring.ptr(), obstring.length());
      c_str_[obstring.length()] = '\0';
      len_ = obstring.length() + 1;
    }
    return ret;
  }
  const char *c_str() const { return c_str_; }
  int64_t len() const { return len_; }
  char *c_str_;
  int64_t len_;
};

static int check_and_prepare_args(const ObString &input,
                                  const ObString &pattern,
                                  const ObIArray<ObStringHolder> &result,
                                  const int64_t nmatch,
                                  CStringHolder &input_cstr_holder,
                                  CStringHolder &pattern_cstr_holder,
                                  regmatch_t *pmatch)
{
  #define PRINT_WRAPPER K(ret), K(input), K(pattern), K(nmatch), K(result)
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; idx < nmatch; ++idx) {// C struct has no default construction method
    pmatch->rm_eo = -1;// described in man manual, not used value are setted -1 value
    pmatch->rm_so = -1;
  }
  if (input.empty() || pattern.empty() || !result.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OCCAM_LOG(WARN, "invalid input", PRINT_WRAPPER);
  } else if (OB_FAIL(input_cstr_holder.init(input))) {
    OCCAM_LOG(WARN, "failed to convert to cstring", PRINT_WRAPPER);
  } else if (OB_FAIL(pattern_cstr_holder.init(pattern))) {
    OCCAM_LOG(WARN, "failed to convert to cstring", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

static void print_detail_msg_to_log_if_meet_error(int sys_api_call_ret, regex_t &regex)
{
  int ret = OB_SUCCESS;
  if (sys_api_call_ret) {
    constexpr int64_t buffer_len = 256;
    char err_msg[buffer_len];
    regerror(sys_api_call_ret, &regex, err_msg, buffer_len);
    err_msg[buffer_len - 1] = '\0';
    OCCAM_LOG(WARN, "show system detail error message", K(err_msg), K(sys_api_call_ret));
  }
}

int regex_api_call(const int64_t input_len,
                   const char *input,
                   const char *regex_rule,
                   const ObOccamRegex::Flag flag,
                   const int64_t nmatch,
                   regmatch_t *pmatch)
{
  #define PRINT_WRAPPER K(ret), K(sys_api_call_ret), K(input), K(regex_rule), K(flag)
  int ret = OB_SUCCESS;
  int sys_api_call_ret = 0;
  regex_t regex;
  int regex_flag = REG_EXTENDED;
  // convert OccamRegex::Flag to POSIX API regex flag
  if (flag & ObOccamRegex::Flag::IgnoreCase) {
    regex_flag |= REG_ICASE;
  }
  if (0 != (sys_api_call_ret = regcomp(&regex, regex_rule, regex_flag))) {
    ret = OB_ERR_SYS;
    print_detail_msg_to_log_if_meet_error(sys_api_call_ret, regex);
    OCCAM_LOG(WARN, "failed to call regcomp()", PRINT_WRAPPER);
  } else {
    if (0 != (sys_api_call_ret = regexec(&regex, input, nmatch, pmatch, 0))) {
      if (REG_NOMATCH == sys_api_call_ret) {// not searched
        ret = OB_EMPTY_RESULT;
        OCCAM_LOG(WARN, "no match", PRINT_WRAPPER);
      } else {
        ret = OB_ERR_SYS;
        print_detail_msg_to_log_if_meet_error(sys_api_call_ret, regex);
        OCCAM_LOG(WARN, "failed to call regexec()", PRINT_WRAPPER);
      }
    }
    regfree(&regex);
  }
  return ret;
  #undef PRINT_WRAPPER
}

static int convert_pmatch_result_to_result_array(const CStringHolder &cstr_holder,
                                                 const int64_t nmatch,
                                                 const regmatch_t *pmatch,
                                                 ObIArray<ObStringHolder> &search_result)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && pmatch[idx].rm_so != -1 && idx < nmatch; ++idx) {
    if (OB_FAIL(search_result.push_back(ObStringHolder()))) {
      OCCAM_LOG(WARN, "failed to push back",
                      K(nmatch), K(idx), K(search_result), K(int(pmatch[idx].rm_so)), K(int(pmatch[idx].rm_so)));
    } else if (OB_FAIL(search_result.at(search_result.count() - 1).assign(ObString(pmatch[idx].rm_eo - pmatch[idx].rm_so,
                                                                                   cstr_holder.c_str() + pmatch[idx].rm_so)))) {
      OCCAM_LOG(WARN, "failed to construct ob string holder",
                      K(nmatch), K(idx), K(search_result), K(int(pmatch[idx].rm_so)), K(int(pmatch[idx].rm_so)));
    }
  }
  return ret;
}

int ObOccamRegex::regex_search(const ObString &input,
                               const ObString &pattern,
                               ObIArray<ObStringHolder> &result,
                               Flag flag)
{
  #define PRINT_WRAPPER K(ret), K(input), K(pattern), K(result), K(flag)
  int ret = OB_SUCCESS;
  regmatch_t pmatch[MAX_GROUP_SIZE];
  CStringHolder input_cstr_holder;
  CStringHolder pattern_cstr_holder;
  if (OB_FAIL(check_and_prepare_args(input, pattern, result, MAX_GROUP_SIZE, input_cstr_holder, pattern_cstr_holder, pmatch))) {
    OCCAM_LOG(WARN, "failed to preapre args", PRINT_WRAPPER);
  } else if (OB_FAIL(regex_api_call(input_cstr_holder.len(),
                                    input_cstr_holder.c_str(),
                                    pattern_cstr_holder.c_str(),
                                    flag,
                                    MAX_GROUP_SIZE,
                                    pmatch))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      OCCAM_LOG(WARN, "searched nothing", PRINT_WRAPPER);
    } else {
      OCCAM_LOG(WARN, "failed to call sys regex api", PRINT_WRAPPER);
    }
  } else if (OB_FAIL(convert_pmatch_result_to_result_array(input_cstr_holder, MAX_GROUP_SIZE, pmatch, result))) {
    OCCAM_LOG(WARN, "failed to fill result", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObOccamRegex::regex_search(const ObString &input,
                               const ObString &pattern,
                               bool &is_serached/*will consider of part match*/,
                               Flag flag)
{
  #define PRINT_WRAPPER K(ret), K(input), K(pattern), K(flag)
  int ret = OB_SUCCESS;
  regmatch_t pmatch[MAX_GROUP_SIZE];
  CStringHolder input_cstr_holder;
  CStringHolder pattern_cstr_holder;
  ObArray<ObStringHolder> _;
  if (OB_FAIL(check_and_prepare_args(input, pattern, _, MAX_GROUP_SIZE, input_cstr_holder, pattern_cstr_holder, pmatch))) {
    OCCAM_LOG(WARN, "failed to preapre args", PRINT_WRAPPER);
  } else if (OB_FAIL(regex_api_call(input_cstr_holder.len(),
                                    input_cstr_holder.c_str(),
                                    pattern_cstr_holder.c_str(),
                                    flag,
                                    MAX_GROUP_SIZE,
                                    pmatch))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      OCCAM_LOG(WARN, "searched nothing", PRINT_WRAPPER);
    } else {
      OCCAM_LOG(WARN, "failed to call sys regex api", PRINT_WRAPPER);
    }
  } else {
    is_serached = true;
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObOccamRegex::regex_match(const ObString &input,
                              const ObString &pattern,
                              bool &is_matched/*just consider of full match*/,
                              Flag flag)
{
  #define PRINT_WRAPPER K(ret), K(input), K(pattern), K(flag)
  int ret = OB_SUCCESS;
  regmatch_t pmatch[MAX_GROUP_SIZE];
  CStringHolder input_cstr_holder;
  CStringHolder pattern_cstr_holder;
  ObArray<ObStringHolder> _;
  if (OB_FAIL(check_and_prepare_args(input, pattern, _, MAX_GROUP_SIZE, input_cstr_holder, pattern_cstr_holder, pmatch))) {
    OCCAM_LOG(WARN, "failed to preapre args", PRINT_WRAPPER);
  } else if (OB_FAIL(regex_api_call(input_cstr_holder.len(),
                                    input_cstr_holder.c_str(),
                                    pattern_cstr_holder.c_str(),
                                    flag,
                                    MAX_GROUP_SIZE,
                                    pmatch))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      OCCAM_LOG(WARN, "searched nothing", PRINT_WRAPPER);
    } else {
      OCCAM_LOG(WARN, "failed to call sys regex api", PRINT_WRAPPER);
    }
  } else {// pmatch[0] must matchs all input str
    OCCAM_LOG(DEBUG, "success to get match results", PRINT_WRAPPER, K(pmatch[0].rm_so), K(pmatch[0].rm_eo));
    is_matched = (pmatch[0].rm_so == 0 && pmatch[0].rm_eo == input_cstr_holder.len() - 1/*consider of '\0'*/) ? true : false;
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObOccamRegex::regex_match(const ObString &input,
                              const ObString &pattern,
                              ObIArray<ObStringHolder> &result/*just consider of full match*/,
                              Flag flag)
{
  #define PRINT_WRAPPER K(ret), K(input), K(pattern), K(flag)
  int ret = OB_SUCCESS;
  regmatch_t pmatch[MAX_GROUP_SIZE];
  CStringHolder input_cstr_holder;
  CStringHolder pattern_cstr_holder;
  bool is_matched = false;
  if (OB_FAIL(check_and_prepare_args(input, pattern, result, MAX_GROUP_SIZE, input_cstr_holder, pattern_cstr_holder, pmatch))) {
    OCCAM_LOG(WARN, "failed to preapre args", PRINT_WRAPPER);
  } else if (OB_FAIL(regex_api_call(input_cstr_holder.len(),
                                    input_cstr_holder.c_str(),
                                    pattern_cstr_holder.c_str(),
                                    flag,
                                    MAX_GROUP_SIZE,
                                    pmatch))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      OCCAM_LOG(WARN, "searched nothing", PRINT_WRAPPER);
    } else {
      OCCAM_LOG(WARN, "failed to call sys regex api", PRINT_WRAPPER);
    }
  } else if (FALSE_IT(is_matched = (pmatch[0].rm_so == 0 && pmatch[0].rm_eo == input_cstr_holder.len() - 1/*consider of '\0'*/) ? true : false)) {// pmatch[0] must matchs all input str
  } else if (!is_matched) {
    OCCAM_LOG(INFO, "regex pattern not full matchs input", PRINT_WRAPPER);
  } else if (OB_FAIL(convert_pmatch_result_to_result_array(input_cstr_holder, MAX_GROUP_SIZE, pmatch, result))) {
    OCCAM_LOG(WARN, "failed to fill result", PRINT_WRAPPER);
  } else {
    OCCAM_LOG(DEBUG, "success to get match results", PRINT_WRAPPER, K(pmatch[0].rm_so), K(pmatch[0].rm_eo));
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
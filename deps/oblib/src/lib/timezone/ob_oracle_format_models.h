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

#ifndef OCEANBASE_ORACLE_FORMAT_MODELS_H_
#define OCEANBASE_ORACLE_FORMAT_MODELS_H_

#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_bit_set.h"
#include "lib/timezone/ob_time_convert.h"

// Note: DFM is abbr of datetime format models
// see oracle doc Format Models: https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements004.htm#i34924

namespace oceanbase {
namespace common {
struct ObTimeConstStr;

struct ObOracleTimeLimiter {
  int32_t MIN;
  int32_t MAX;
  int ERROR_CODE;
  inline int validate(int32_t value) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(value < MIN || value > MAX)) {
      ret = ERROR_CODE;
    }
    return ret;
  }
};

struct ObDFMLimit {
  static const ObOracleTimeLimiter YEAR;
  static const ObOracleTimeLimiter MONTH;
  static const ObOracleTimeLimiter MONTH_DAY;
  static const ObOracleTimeLimiter WEEK_DAY;
  static const ObOracleTimeLimiter YEAR_DAY;
  static const ObOracleTimeLimiter HOUR12;
  static const ObOracleTimeLimiter HOUR24;
  static const ObOracleTimeLimiter MINUTE;
  static const ObOracleTimeLimiter SECOND;
  static const ObOracleTimeLimiter SECS_PAST_MIDNIGHT;
  static const ObOracleTimeLimiter TIMEZONE_HOUR_ABS;
  static const ObOracleTimeLimiter TIMEZONE_MIN_ABS;
  static const ObOracleTimeLimiter JULIAN_DATE;
};

class ObDFMFlag {
public:
  // ElementFlag are defined according to oracle doc
  // see Format Models: https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements004.htm#i34924
  // Note: FF1-FF9 and FF should be together
  enum ElementFlag : int64_t {
    INVALID_FLAG = -1,
    AD = 0,
    AD2,  // A.D.
    BC,
    BC2,  // B.C.
    CC,
    SCC,
    D,
    DAY,
    DD,
    DDD,
    DY,
    FF1,
    FF2,
    FF3,
    FF4,
    FF5,
    FF6,
    FF7,
    FF8,
    FF9,
    FF,
    HH,
    HH24,
    HH12,
    IW,
    I,
    IY,
    IYY,
    IYYY,
    MI,
    MM,
    MONTH,
    MON,
    AM,
    AM2,  // A.M.
    PM,
    PM2,  // P.M.
    Q,
    RR,
    RRRR,
    SS,
    SSSSS,
    WW,
    W,
    YGYYY,
    YEAR,
    SYEAR,
    YYYY,
    SYYYY,
    YYY,
    YY,
    Y,
    DS,
    DL,
    TZH,
    TZM,
    TZD,
    TZR,
    X,
    J,
    ///<<< !!!add any flag before this line!!!
    // Please also add in ELEMENTFLAG_MAX_LEN[ObDFMFlag::MAX_FLAG_NUMBER]
    MAX_FLAG_NUMBER
  };

  // ElementGroup to handle conflict. Each group should only contain one element.
  enum ElementGroup : int64_t {
    RUNTIME_CONFLICT_SOLVE_GROUP = -2,
    NON_CONFLICT_GROUP = -1,
    ///<<< conflict in group, before this line, will be ignored
    NEVER_APPEAR_GROUP = 0,    // the element should never appear
    YEAR_GROUP,                // include : SYYYY YYYY YYY YY Y YGYYY RR RRRR
    MERIDIAN_INDICATOR_GROUP,  // include : AM PM
    WEEK_OF_DAY_GROUP,         // include : D Day Dy
    ERA_GROUP,                 // include : AD BC
    HOUR_GROUP,                // include : HH HH12 HH24
    MONTH_GROUP,               // include : MONTH MON MM
    DAY_OF_YEAR_GROUP,         // include : DDD, J
    ///<<< !!!add any flag before this line!!!
    MAX_CONFLICT_GROUP_NUMBER
  };

  // For matching format string, patterns of each flag are defined
  static const ObTimeConstStr PATTERN[MAX_FLAG_NUMBER];
  // conflict group of the elements.
  static constexpr int64_t CONFLICT_GROUP_MAP[MAX_FLAG_NUMBER] = {
      /**AD*/ ERA_GROUP,
      /**AD2*/ ERA_GROUP,
      /**BC*/ ERA_GROUP,
      /**BC2*/ ERA_GROUP,
      /**CC,*/ NEVER_APPEAR_GROUP,
      /**SCC*/ NEVER_APPEAR_GROUP,
      /**D*/ WEEK_OF_DAY_GROUP,
      /**DAY*/ WEEK_OF_DAY_GROUP,
      /**DD*/ NON_CONFLICT_GROUP,
      /**DDD*/ DAY_OF_YEAR_GROUP,
      /**DY*/ WEEK_OF_DAY_GROUP,
      /**FF1*/ NON_CONFLICT_GROUP,
      /**FF2*/ NON_CONFLICT_GROUP,
      /**FF3*/ NON_CONFLICT_GROUP,
      /**FF4*/ NON_CONFLICT_GROUP,
      /**FF5*/ NON_CONFLICT_GROUP,
      /**FF6*/ NON_CONFLICT_GROUP,
      /**FF7*/ NON_CONFLICT_GROUP,
      /**FF8*/ NON_CONFLICT_GROUP,
      /**FF9*/ NON_CONFLICT_GROUP,
      /**FF*/ NON_CONFLICT_GROUP,
      /**HH*/ HOUR_GROUP,
      /**HH24*/ HOUR_GROUP,
      /**HH12*/ HOUR_GROUP,
      /**IW*/ NEVER_APPEAR_GROUP,
      /**I*/ NEVER_APPEAR_GROUP,
      /**IY*/ NEVER_APPEAR_GROUP,
      /**IYY*/ NEVER_APPEAR_GROUP,
      /**IYYY*/ NEVER_APPEAR_GROUP,
      /**MI*/ NON_CONFLICT_GROUP,
      /**MM*/ MONTH_GROUP,
      /**MONTH*/ MONTH_GROUP,
      /**MON*/ MONTH_GROUP,
      /**AM*/ MERIDIAN_INDICATOR_GROUP,
      /**AM2*/ MERIDIAN_INDICATOR_GROUP,
      /**PM*/ MERIDIAN_INDICATOR_GROUP,
      /**PM2*/ MERIDIAN_INDICATOR_GROUP,
      /**Q*/ NON_CONFLICT_GROUP,
      /**RR*/ YEAR_GROUP,
      /**RRRR*/ YEAR_GROUP,
      /**SS*/ NON_CONFLICT_GROUP,
      /**SSSSS*/ RUNTIME_CONFLICT_SOLVE_GROUP,
      /**WW*/ NEVER_APPEAR_GROUP,
      /**W*/ NEVER_APPEAR_GROUP,
      /**YGYYY*/ YEAR_GROUP,
      /**YEAR*/ NEVER_APPEAR_GROUP,
      /**SYEAR*/ NEVER_APPEAR_GROUP,
      /**YYYY*/ YEAR_GROUP,
      /**SYYYY*/ YEAR_GROUP,
      /**YYY*/ YEAR_GROUP,
      /**YY*/ YEAR_GROUP,
      /**Y*/ YEAR_GROUP,
      /**DS*/ RUNTIME_CONFLICT_SOLVE_GROUP,
      /**DL*/ RUNTIME_CONFLICT_SOLVE_GROUP,
      /**TZH*/ RUNTIME_CONFLICT_SOLVE_GROUP,
      /**TZM*/ RUNTIME_CONFLICT_SOLVE_GROUP,
      /**TZD*/ RUNTIME_CONFLICT_SOLVE_GROUP,
      /**TZR*/ RUNTIME_CONFLICT_SOLVE_GROUP,
      /**X*/ NON_CONFLICT_GROUP,
      /**J*/ DAY_OF_YEAR_GROUP,
  };
  // the user error code returned, if conflict happend,.
  static constexpr int CONFLICT_GROUP_ERR[MAX_CONFLICT_GROUP_NUMBER] = {
      /*NEVER_APPEAR_GROUP*/ OB_ERR_FORMAT_CODE_CANNOT_APPEAR,  // ORA-01820: format code cannot appear in date input
                                                                // format
      /*YEAR_GROUP*/ OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE,    // ORA-01812: year may only be specified once
      /*MERIDIAN_INDICATOR_GROUP*/ OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT,  // ORA-01810: format code appears
                                                                                      // twice
      /*WEEK_OF_DAY_GROUP*/ OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE,  // ORA-01817: day of week may only be
                                                                          // specified once
      /*ERA_GROUP*/ OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT,      // ORA-01810: format code appears twice
      /*HOUR_GROUP*/ OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE,              // ORA-01813: hour may only be specified once
      /*MONTH_GROUP*/ OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE,            // ORA-01816: month may only be specified once
      /*DAY_OF_YEAR_GROUP*/ OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR  // ORA-1811: Julian date precludes use of
                                                                             // day of year
  };

  // max length for matching element
  static constexpr int64_t EXPECTED_MATCHING_LENGTH[MAX_FLAG_NUMBER] = {
      /**AD*/ 2,
      /**AD2*/ 4,
      /**BC*/ 2,
      /**BC2*/ 4,
      /**CC,*/ 0,  // never used
      /**SCC*/ 0,  // never used
      /**D*/ 1,
      /**DAY*/ 0,  // non-numeric, ignored
      /**DD*/ 2,
      /**DDD*/ 3,
      /**DY*/ 0,  // non-numeric, ignored
      /**FF1*/ 1,
      /**FF2*/ 2,
      /**FF3*/ 3,
      /**FF4*/ 4,
      /**FF5*/ 5,
      /**FF6*/ 6,
      /**FF7*/ 7,
      /**FF8*/ 8,
      /**FF9*/ 9,
      /**FF*/ 9,
      /**HH*/ 2,
      /**HH24*/ 2,
      /**HH12*/ 2,
      /**IW*/ 0,    // never used
      /**I*/ 0,     // never used
      /**IY*/ 0,    // never used
      /**IYY*/ 0,   // never used
      /**IYYY*/ 0,  // never used
      /**MI*/ 2,
      /**MM*/ 2,
      /**MONTH*/ 0,  // non-numeric, ignored
      /**MON*/ 0,    // non-numeric, ignored
      /**AM*/ 2,
      /**AM2*/ 4,
      /**PM*/ 2,
      /**PM2*/ 4,
      /**Q*/ 0,     // never used
      /**RR*/ 0,    // special case
      /**RRRR*/ 0,  // special case
      /**SS*/ 2,
      /**SSSSS*/ 5,
      /**WW*/ 0,  // never used
      /**W*/ 0,   // never used
      /**YGYYY*/ 5,
      /**YEAR*/ 0,   // never used
      /**SYEAR*/ 0,  // never used
      /**YYYY*/ 4,
      /**SYYYY*/ 4,
      /**YYY*/ 3,
      /**YY*/ 2,
      /**Y*/ 1,
      /**DS*/ 0,   // todo
      /**DL*/ 0,   // todo
      /**TZH*/ 2,  // todo
      /**TZM*/ 2,  // todo
      /**TZD*/ 0,  // non-numeric, ignored
      /**TZR*/ 0,  // non-numeric, ignored
      /**X*/ 0,    // non-numeric, ignored
      /**J*/ 7,
  };

  static inline bool is_flag_valid(int64_t flag)
  {
    return (flag > INVALID_FLAG && flag < MAX_FLAG_NUMBER);
  }
  static inline bool need_check_conflict(int64_t elem_group)
  {
    return elem_group >= 0;
  }
  static inline bool need_check_expected_length(ElementFlag flag)
  {
    return is_flag_valid(flag) && (EXPECTED_MATCHING_LENGTH[flag] > 0);
  }

private:
  static int64_t calc_max_len_of_patterns();
};

struct ObDFMParseCtx {
  explicit ObDFMParseCtx(const char* fmt_str, const int64_t fmt_len)
      : fmt_str_(fmt_str),
        cur_ch_(fmt_str),
        remain_len_(fmt_len),
        expected_elem_flag_(ObDFMFlag::INVALID_FLAG),
        is_matching_by_expected_len_(false)
  {}
  inline void update(const int64_t succ_len)
  {
    cur_ch_ += succ_len;
    remain_len_ -= succ_len;
  }
  inline bool is_valid()
  {
    return cur_ch_ != NULL && remain_len_ > 0;
  }
  inline int64_t get_parsed_len()
  {
    return static_cast<int64_t>(cur_ch_ - fmt_str_);
  }
  inline bool is_parse_finish()
  {
    return 0 == remain_len_;
  }
  inline void revert(const int64_t rev_len)
  {
    cur_ch_ -= rev_len;
    remain_len_ += rev_len;
  }

  void set_next_expected_elem(int64_t elem_flag, bool is_matching_by_expected_len)
  {
    expected_elem_flag_ = elem_flag;
    is_matching_by_expected_len_ = is_matching_by_expected_len;
  }

  const char* const fmt_str_;
  const char* cur_ch_;
  int64_t remain_len_;

  // the following values are only used in function str_to_ob_time_oracle_dfm
  int64_t expected_elem_flag_;
  bool is_matching_by_expected_len_;  // only used for match_int_value

  TO_STRING_KV("parsed len", static_cast<int64_t>(cur_ch_ - fmt_str_), "remain chars", ObString(remain_len_, cur_ch_),
      K_(expected_elem_flag), K_(is_matching_by_expected_len));
};

struct ObDFMElem {

  enum class UpperCaseMode { NON_CHARACTER, ONLY_FIRST_CHARACTER, ALL_CHARACTER };

  ObDFMElem()
      : elem_flag_(ObDFMFlag::INVALID_FLAG),
        offset_(OB_INVALID_INDEX_INT64),
        is_single_dot_before_(false),
        upper_case_mode_(UpperCaseMode::NON_CHARACTER)
  {}
  int64_t elem_flag_;          // flag from enum ObDFMFlag
  int64_t offset_;             // offset in origin format string
  bool is_single_dot_before_;  // for the dot before FF
  UpperCaseMode upper_case_mode_;
  ObString get_elem_name() const;
  TO_STRING_KV("elem_flag", get_elem_name(), K_(offset), K_(is_single_dot_before), K_(upper_case_mode));

  bool inline is_valid()
  {
    return ObDFMFlag::is_flag_valid(elem_flag_) && offset_ >= 0;
  }
};

typedef ObIArray<ObDFMElem> ObDFMElemArr;

class ObDFMUtil {
public:
  static const int64_t UNKNOWN_LENGTH_OF_ELEMENT = 20;
  static const int64_t COMMON_ELEMENT_NUMBER = 10;
  static int parse_datetime_format_string(const ObString& fmt_str, ObDFMElemArr& elements);
  static int check_semantic(
      const ObDFMElemArr& elements, ObBitSet<ObDFMFlag::MAX_FLAG_NUMBER>& flag_bitmap, uint64_t mode);
  static int parse_one_elem(ObDFMParseCtx& ctx, ObDFMElem& elem);
  static inline int64_t skip_separate_chars(
      ObDFMParseCtx& ctx, const int64_t limit = OB_MAX_VARCHAR_LENGTH, const int64_t stop_char = INT64_MAX);
  static inline int64_t skip_blank_chars(ObDFMParseCtx& ctx);
  static inline bool is_element_can_omit(const ObDFMElem& elem);
  // Explain padding: day or month name is padded with blanks to display in the same wide, please see oracle doc
  static int special_mode_sprintf(char* buf, const int64_t buf_len, int64_t& pos, const ObTimeConstStr& str,
      const ObDFMElem::UpperCaseMode mode, int64_t padding = -1);
  static int match_chars_until_space(ObDFMParseCtx& ctx, ObString& result, int64_t& value_len);
  static const char* find_first_separator(ObDFMParseCtx& ctx);
  static int match_int_value(
      ObDFMParseCtx& ctx, const int64_t expected_len, int64_t& value_len, int32_t& result, int32_t value_sign = 1);
  static int match_int_value_with_comma(
      ObDFMParseCtx& ctx, const int64_t expected_len, int64_t& value_len, int32_t& result);
  static int check_int_value_length(const ObDFMParseCtx& ctx, const int64_t expected_len, const int64_t real_data_len);
  static int check_ctx_valid(ObDFMParseCtx& ctx, int err_code);
  static int match_char(ObDFMParseCtx& ctx, const char c, const int err_code);

  static inline bool match_pattern_ignore_case(ObDFMParseCtx& ctx, const ObTimeConstStr& pattern)
  {
    bool ret_bool = false;
    if (ctx.remain_len_ >= pattern.len_) {
      ret_bool = (0 == strncasecmp(ctx.cur_ch_, pattern.ptr_, pattern.len_));
    } else {
      // false
    }
    return ret_bool;
  }
  static inline bool elem_has_meridian_indicator(ObBitSet<ObDFMFlag::MAX_FLAG_NUMBER>& flag_bitmap)
  {
    return flag_bitmap.has_member(ObDFMFlag::AM) || flag_bitmap.has_member(ObDFMFlag::PM) ||
           flag_bitmap.has_member(ObDFMFlag::AM2) || flag_bitmap.has_member(ObDFMFlag::PM2);
  }
  static bool is_split_char(const char ch);
  static inline bool is_sign_char(const char ch)
  {
    return '-' == ch || '+' == ch;
  }

private:
  static inline bool is_uppercase_char(const char ch)
  {
    return (0 == (ch & (1 << 5)));
  }
};

int64_t ObDFMUtil::skip_blank_chars(ObDFMParseCtx& ctx)
{
  int64_t blank_char_len = 0;
  while (blank_char_len < ctx.remain_len_ && ' ' == ctx.cur_ch_[blank_char_len]) {
    blank_char_len++;
  }
  ctx.update(blank_char_len);
  return blank_char_len;
}

int64_t ObDFMUtil::skip_separate_chars(
    ObDFMParseCtx& ctx, const int64_t limit /*= OB_MAX_VARCHAR_LENGTH*/, const int64_t stop_char /*= INT64_MAX*/)
{
  int64_t sep_len = 0;
  while (sep_len < ctx.remain_len_ && sep_len < limit && is_split_char(ctx.cur_ch_[sep_len]) &&
         static_cast<int64_t>(ctx.cur_ch_[sep_len]) != stop_char) {
    sep_len++;
  }
  ctx.update(sep_len);
  return sep_len;
}

inline bool ObDFMUtil::is_split_char(const char ch)
{
  int ret_bool = false;
  if (ch == '\n' || ch == '\t' ||
      ((ch >= 0x20 && ch <= 0x7E) &&
          !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')))) {
    ret_bool = true;
  }
  return ret_bool;
}
/*
 * if format elements contains TZR
 * hour minuts seconds and fracial second can not omit
 * Because, I guess, the daylight-saving time may be uncertain
 * if the time part is omitted.
 * The day
 */
inline bool ObDFMUtil::is_element_can_omit(const ObDFMElem& elem)
{
  int ret_bool = true;
  int64_t flag = elem.elem_flag_;
  int64_t conf_group = ObDFMFlag::CONFLICT_GROUP_MAP[flag];
  if (ObDFMFlag::YEAR_GROUP == conf_group || ObDFMFlag::WEEK_OF_DAY_GROUP == conf_group ||
      ObDFMFlag::MONTH_GROUP == conf_group || ObDFMFlag::DD == flag || ObDFMFlag::DS == flag || ObDFMFlag::DL == flag) {
    ret_bool = false;
  } else {
    // return true
  }
  return ret_bool;
}

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_ORACLE_FORMAT_MODELS_H_

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

//Note: DFM is abbr of datetime format models
//see oracle doc Format Models: https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements004.htm#i34924

namespace oceanbase
{
namespace common
{
struct ObTimeConstStr;

struct ObOracleTimeLimiter
{
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

struct ObDFMLimit
{
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
//ElementFlag are defined according to oracle doc
//see Format Models: https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements004.htm#i34924
//Note: FF1-FF9 and FF should be together
enum ElementFlag : int64_t
{
  INVALID_FLAG = -1,
  AD = 0,
  AD2,  //A.D.
  BC,
  BC2,  //B.C.
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
  AM2,  //A.M.
  PM,
  PM2,  //P.M.
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
  FM,
  LITERAL,
  ///<<< !!!add any flag before this line!!!
  // Please also add in ELEMENTFLAG_MAX_LEN[ObDFMFlag::MAX_FLAG_NUMBER]
    MAX_FLAG_NUMBER
  };

  //定义元素分组。有些相同语义的元素，他们属于同一个组，同组内的元素只能有一个，否则会报错
  enum ElementGroup : int64_t
  {
    RUNTIME_CONFLICT_SOLVE_GROUP = -2,
    NON_CONFLICT_GROUP = -1,
    ///<<< conflict in group, before this line, will be ignored
    NEVER_APPEAR_GROUP = 0,   //the element should never appear
    YEAR_GROUP,               //include : SYYYY YYYY YYY YY Y YGYYY RR RRRR
    MERIDIAN_INDICATOR_GROUP, //include : AM PM
    WEEK_OF_DAY_GROUP,        //include : D Day Dy
    ERA_GROUP,                //include : AD BC
    HOUR_GROUP,               //include : HH HH12 HH24
    MONTH_GROUP,              //include : MONTH MON MM
    DAY_OF_YEAR_GROUP,           //include : DDD, J
    ///<<< !!!add any flag before this line!!!
    MAX_CONFLICT_GROUP_NUMBER
  };

  //每一个flag对应一个PATTERN, 匹配format时, 通过遍历pattern数组, 返回匹配到element的flag
  static const ObTimeConstStr     PATTERN[MAX_FLAG_NUMBER];
  //定义每个元素属于哪一组
  static constexpr int64_t        CONFLICT_GROUP_MAP[MAX_FLAG_NUMBER] =
  {
    /**AD*/         ERA_GROUP,
    /**AD2*/        ERA_GROUP,
    /**BC*/         ERA_GROUP,
    /**BC2*/        ERA_GROUP,
    /**CC,*/        NEVER_APPEAR_GROUP,
    /**SCC*/        NEVER_APPEAR_GROUP,
    /**D*/          WEEK_OF_DAY_GROUP,
    /**DAY*/        WEEK_OF_DAY_GROUP,
    /**DD*/         NON_CONFLICT_GROUP,
    /**DDD*/        DAY_OF_YEAR_GROUP,
    /**DY*/         WEEK_OF_DAY_GROUP,
    /**FF1*/        NON_CONFLICT_GROUP,
    /**FF2*/        NON_CONFLICT_GROUP,
    /**FF3*/        NON_CONFLICT_GROUP,
    /**FF4*/        NON_CONFLICT_GROUP,
    /**FF5*/        NON_CONFLICT_GROUP,
    /**FF6*/        NON_CONFLICT_GROUP,
    /**FF7*/        NON_CONFLICT_GROUP,
    /**FF8*/        NON_CONFLICT_GROUP,
    /**FF9*/        NON_CONFLICT_GROUP,
    /**FF*/         NON_CONFLICT_GROUP,
    /**HH*/         HOUR_GROUP,
    /**HH24*/       HOUR_GROUP,
    /**HH12*/       HOUR_GROUP,
    /**IW*/         NEVER_APPEAR_GROUP,
    /**I*/          NEVER_APPEAR_GROUP,
    /**IY*/         NEVER_APPEAR_GROUP,
    /**IYY*/        NEVER_APPEAR_GROUP,
    /**IYYY*/       NEVER_APPEAR_GROUP,
    /**MI*/         NON_CONFLICT_GROUP,
    /**MM*/         MONTH_GROUP,
    /**MONTH*/      MONTH_GROUP,
    /**MON*/        MONTH_GROUP,
    /**AM*/         MERIDIAN_INDICATOR_GROUP,
    /**AM2*/        MERIDIAN_INDICATOR_GROUP,
    /**PM*/         MERIDIAN_INDICATOR_GROUP,
    /**PM2*/        MERIDIAN_INDICATOR_GROUP,
    /**Q*/          NON_CONFLICT_GROUP,
    /**RR*/         YEAR_GROUP,
    /**RRRR*/       YEAR_GROUP,
    /**SS*/         NON_CONFLICT_GROUP,
    /**SSSSS*/      RUNTIME_CONFLICT_SOLVE_GROUP,
    /**WW*/         NEVER_APPEAR_GROUP,
    /**W*/          NEVER_APPEAR_GROUP,
    /**YGYYY*/      YEAR_GROUP,
    /**YEAR*/       NEVER_APPEAR_GROUP,
    /**SYEAR*/      NEVER_APPEAR_GROUP,
    /**YYYY*/       YEAR_GROUP,
    /**SYYYY*/      YEAR_GROUP,
    /**YYY*/        YEAR_GROUP,
    /**YY*/         YEAR_GROUP,
    /**Y*/          YEAR_GROUP,
    /**DS*/         RUNTIME_CONFLICT_SOLVE_GROUP,
    /**DL*/         RUNTIME_CONFLICT_SOLVE_GROUP,
    /**TZH*/        RUNTIME_CONFLICT_SOLVE_GROUP,
    /**TZM*/        RUNTIME_CONFLICT_SOLVE_GROUP,
    /**TZD*/        RUNTIME_CONFLICT_SOLVE_GROUP,
    /**TZR*/        RUNTIME_CONFLICT_SOLVE_GROUP,
    /**X*/          NON_CONFLICT_GROUP,
    /**J*/          DAY_OF_YEAR_GROUP,
    /**LITERAL*/    NON_CONFLICT_GROUP,
  };

  static const int32_t MAX_WDAY_NAME_LENGTH_ORACLE = 36;
  static const int32_t MAX_WDAY_ABBR_NAME_LENGTH_ORACLE = 12;
  static const int32_t MAX_MON_NAME_LENGTH_ORACLE = 36;
  static const int32_t MAX_MON_ABBR_NAME_LENGTH_ORACLE = 12;
  static constexpr int64_t ELEMENTFLAG_MAX_LEN[MAX_FLAG_NUMBER] = {
    /*AD*/2, /*AD2*/4, /*BC*/2, /*BC2*/4, /*CC*/2, /*SCC*/3,
    /*D*/1, /*Day*/MAX_WDAY_NAME_LENGTH_ORACLE, /*DD*/2, /*DDD*/3, /*DY*/MAX_WDAY_ABBR_NAME_LENGTH_ORACLE, /*FF1*/9,
    /*FF2*/9, /*FF3*/9, /*FF4*/9, /*FF5*/9, /*FF6*/9, /*FF7*/9,
    /*FF8*/9, /*FF9*/9, /*FF*/9, /*HH*/2, /*HH24*/2, /*HH12*/2,
    /*IW*/2, /*I*/1, /*IY*/2, /*IYY*/3, /*IYYY*/4, /*MI*/2,
    /*MM*/2, /*MONTH*/MAX_MON_NAME_LENGTH_ORACLE, /*MON*/MAX_MON_ABBR_NAME_LENGTH_ORACLE, /*AM*/2, /*AM2*/3, /*PM*/2,
    /*PM2*/4, /*Q*/1, /*RR*/2, /*RRRR*/4, /*SS*/2, /*SSSSS*/5,
    /*WW*/2, /*W*/1, /*YGYYY*/5, /*YEAR*/0, /*SYEAR*/0, /*YYYY*/4,
    /*SYYYY*/5, /*YYY*/3, /*YY*/2, /*Y*/1, /*DS*/10, /*DL*/MAX_WDAY_NAME_LENGTH_ORACLE + 2 + MAX_MON_NAME_LENGTH_ORACLE + 1 + 2 + 3,
    /*TZH*/3, /*TZM*/2, /*TZD*/OB_MAX_TZ_ABBR_LEN, /*TZR*/MIN(OB_MAX_TZ_NAME_LEN, 6), /*X*/1, /*J*/7, /*LITERAL*/46
  };

  //定义组内冲突时，报什么错
  static constexpr int            CONFLICT_GROUP_ERR[MAX_CONFLICT_GROUP_NUMBER] =
  {
    /*NEVER_APPEAR_GROUP*/            OB_ERR_FORMAT_CODE_CANNOT_APPEAR,                 //ORA-01820: format code cannot appear in date input format
    /*YEAR_GROUP*/                    OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE,           //ORA-01812: year may only be specified once
    /*MERIDIAN_INDICATOR_GROUP*/      OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT, //ORA-01810: format code appears twice
    /*WEEK_OF_DAY_GROUP*/             OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE,      //ORA-01817: day of week may only be specified once
    /*ERA_GROUP*/                     OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT,  //ORA-01810: format code appears twice
    /*HOUR_GROUP*/                    OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE,           //ORA-01813: hour may only be specified once
    /*MONTH_GROUP*/                   OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE,          //ORA-01816: month may only be specified once
    /*DAY_OF_YEAR_GROUP*/             OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR   //ORA-1811: Julian date precludes use of day of year
  };

  //定义每个元素(数字类型)期望匹配长度，可以小于这个长度，不能超过
  static constexpr int64_t        EXPECTED_MATCHING_LENGTH[MAX_FLAG_NUMBER] =
  {
    /**AD*/         2,
    /**AD2*/        4,
    /**BC*/         2,
    /**BC2*/        4,
    /**CC,*/        0, //never used
    /**SCC*/        0, //never used
    /**D*/          1,
    /**DAY*/        0, //non-numeric, ignored
    /**DD*/         2,
    /**DDD*/        3,
    /**DY*/         0, //non-numeric, ignored
    /**FF1*/        1,
    /**FF2*/        2,
    /**FF3*/        3,
    /**FF4*/        4,
    /**FF5*/        5,
    /**FF6*/        6,
    /**FF7*/        7,
    /**FF8*/        8,
    /**FF9*/        9,
    /**FF*/         9,
    /**HH*/         2,
    /**HH24*/       2,
    /**HH12*/       2,
    /**IW*/         0, //never used
    /**I*/          0, //never used
    /**IY*/         0, //never used
    /**IYY*/        0, //never used
    /**IYYY*/       0, //never used
    /**MI*/         2,
    /**MM*/         2,
    /**MONTH*/      0, //non-numeric, ignored
    /**MON*/        0, //non-numeric, ignored
    /**AM*/         2,
    /**AM2*/        4,
    /**PM*/         2,
    /**PM2*/        4,
    /**Q*/          0, //never used
    /**RR*/         0, //special case
    /**RRRR*/       0, //special case
    /**SS*/         2,
    /**SSSSS*/      5,
    /**WW*/         0, //never used
    /**W*/          0, //never used
    /**YGYYY*/      5,
    /**YEAR*/       0, //never used
    /**SYEAR*/      0, //never used
    /**YYYY*/       4,
    /**SYYYY*/      4,
    /**YYY*/        3,
    /**YY*/         2,
    /**Y*/          1,
    /**DS*/         0, //todo
    /**DL*/         0, //todo
    /**TZH*/        2, //todo
    /**TZM*/        2, //todo
    /**TZD*/        0, //non-numeric, ignored
    /**TZR*/        0, //non-numeric, ignored
    /**X*/          0, //non-numeric, ignored
    /**J*/          7,
    /**LITERAL*/    46, // max length of format is 46.
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



struct ObDFMParseCtx
{
  explicit ObDFMParseCtx(const char* fmt_str, const int64_t fmt_len):
    fmt_str_(fmt_str),
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

  void set_next_expected_elem(int64_t elem_flag,
                              bool is_matching_by_expected_len)
  {
    expected_elem_flag_ = elem_flag;
    is_matching_by_expected_len_ = is_matching_by_expected_len;
  }

  const char* const fmt_str_;
  const char* cur_ch_;
  int64_t remain_len_;

  //the following values are only used in function str_to_ob_time_oracle_dfm
  int64_t expected_elem_flag_;
  bool is_matching_by_expected_len_;  //only used for match_int_value


  TO_STRING_KV("parsed len", static_cast<int64_t>(cur_ch_ - fmt_str_),
               "remain chars", ObString(remain_len_, cur_ch_),
               K_(expected_elem_flag),
               K_(is_matching_by_expected_len));

};

struct ObDFMElem
{

  enum class UpperCaseMode {
    NON_CHARACTER,
    ONLY_FIRST_CHARACTER,
    ALL_CHARACTER
  };

  ObDFMElem(): elem_flag_(ObDFMFlag::INVALID_FLAG),
    offset_(OB_INVALID_INDEX_INT64),
    len_(0),
    is_single_dot_before_(false),
    upper_case_mode_(UpperCaseMode::NON_CHARACTER)
  {}
  int64_t elem_flag_;             //flag from enum ObDFMFlag
  int64_t offset_;                //offset in origin format string
  int64_t len_;                   // length, only valid for LITERAL.
  bool is_single_dot_before_;   //for the dot before FF
  UpperCaseMode upper_case_mode_;
  ObString get_elem_name() const;
  TO_STRING_KV("elem_flag", get_elem_name(), K_(offset), K_(len), K_(is_single_dot_before),
    K_(upper_case_mode));

  bool inline is_valid() const
  {
    return ObDFMFlag::is_flag_valid(elem_flag_) && offset_ >= 0;
  }
};

typedef ObIArray<ObDFMElem> ObDFMElemArr;

class ObDFMUtil
{
public:
  static const int64_t UNKNOWN_LENGTH_OF_ELEMENT = 20;
  static const int64_t COMMON_ELEMENT_NUMBER = 10;
  static const int64_t MAX_FORMAT_LENGTH = 78;
  static int parse_datetime_format_string(const ObString &fmt_str, ObDFMElemArr &elements,
                                          const bool support_double_quotes = true);
  static int check_semantic(const ObDFMElemArr &elements,
                            ObFixedBitSet<OB_DEFAULT_BITSET_SIZE_FOR_DFM> &flag_bitmap, uint64_t mode);
  static int parse_one_elem(ObDFMParseCtx &ctx, ObDFMElem &elem, const bool support_double_quotes);
  static inline int64_t skip_separate_chars(ObDFMParseCtx &ctx,
                                            const int64_t limit = OB_MAX_VARCHAR_LENGTH,
                                            const int64_t stop_char = INT64_MAX);
  static inline int64_t skip_blank_chars(ObDFMParseCtx &ctx);
  static inline bool is_element_can_omit(const ObDFMElem &elem);
  //Explain padding: day or month name is padded with blanks to display in the same wide, please see oracle doc
  static int special_mode_sprintf(char *buf, const int64_t buf_len, int64_t &pos,
                                  const ObTimeConstStr &str, const ObDFMElem::UpperCaseMode mode, int64_t padding = -1);
  static int match_chars_until_space(ObDFMParseCtx &ctx, ObString &result, int64_t &value_len);
  static const char *find_first_separator(ObDFMParseCtx &ctx);
  static int match_int_value(ObDFMParseCtx &ctx,
                             const int64_t expected_len,
                             int64_t &value_len,
                             int32_t &result,
                             int32_t value_sign = 1);
  static int match_int_value_with_comma(ObDFMParseCtx &ctx, const int64_t expected_len, int64_t &value_len, int32_t &result);
  static int check_int_value_length(const ObDFMParseCtx &ctx,
                                    const int64_t expected_len,
                                    const int64_t real_data_len);
  static int check_ctx_valid(ObDFMParseCtx &ctx, int err_code);
  static int match_char(ObDFMParseCtx &ctx, const char c, const int err_code);
  static int validate_literal_elem(const ObDFMElem &elem, const ObString &format,
                                   ObString &literal);
  static int match_literal_ignore_case(ObDFMParseCtx &ctx, const ObDFMElem &elem,
                                       const ObString &format, bool &matched, int64_t &matched_len);
  static int print_literal(char *buf, const int64_t buf_len, int64_t &pos, const ObDFMElem &elem,
                           const ObString &format);
  // used for get length of element in the format.
  static inline int64_t get_element_len(const ObDFMElem elem)
  {
    return ObDFMFlag::LITERAL == elem.elem_flag_ ? elem.len_ : ObDFMFlag::PATTERN[elem.elem_flag_].len_;
  }
  // used for get max length of element in varchar. used when generate varchar according to format.
  static inline int64_t get_element_max_len(const ObDFMElem elem)
  {
    return ObDFMFlag::LITERAL == elem.elem_flag_ ? elem.len_ : ObDFMFlag::ELEMENTFLAG_MAX_LEN[elem.elem_flag_];
  }

  static inline bool match_pattern_ignore_case(ObDFMParseCtx &ctx, const ObTimeConstStr &pattern)
  {
    bool ret_bool = false;
    if (ctx.remain_len_ >= pattern.len_) {
      ret_bool = (0 == strncasecmp(ctx.cur_ch_, pattern.ptr_, pattern.len_));
    } else {
      //false
    }
    return ret_bool;
  }
  static inline bool elem_has_meridian_indicator(const ObFixedBitSet<OB_DEFAULT_BITSET_SIZE_FOR_DFM> &flag_bitmap)
  {
    return flag_bitmap.has_member(ObDFMFlag::AM) || flag_bitmap.has_member(ObDFMFlag::PM)
        || flag_bitmap.has_member(ObDFMFlag::AM2) || flag_bitmap.has_member(ObDFMFlag::PM2);
  }
  static bool is_split_char(const char ch);
  static inline bool is_sign_char(const char ch) {
    return '-' == ch || '+' == ch;
  }
  static bool is_valid_ending_char(const char ch) {
    return 0x0 == ch || isspace(ch);
  }
private:
  static inline bool is_uppercase_char(const char ch)
  {
    return (0 == (ch & (1 << 5)));
  }
};

int64_t ObDFMUtil::skip_blank_chars(ObDFMParseCtx &ctx)
{
  int64_t blank_char_len = 0;
  while (blank_char_len < ctx.remain_len_
         && isspace(ctx.cur_ch_[blank_char_len])) {
    blank_char_len++;
  }
  ctx.update(blank_char_len);
  return blank_char_len;
}

int64_t ObDFMUtil::skip_separate_chars(ObDFMParseCtx &ctx,
    const int64_t limit/*= OB_MAX_VARCHAR_LENGTH*/,
    const int64_t stop_char/*= INT64_MAX*/)
{
  int64_t sep_len = 0;
  while (sep_len < ctx.remain_len_ && sep_len < limit
         && is_split_char(ctx.cur_ch_[sep_len])
         && static_cast<int64_t>(ctx.cur_ch_[sep_len]) != stop_char) {
    sep_len++;
  }
  ctx.update(sep_len);
  return sep_len;
}

inline bool ObDFMUtil::is_split_char(const char ch)
{
  int ret_bool = false;
  if (ch <= 0x7F && ch >= 0x0 &&
      !((ch >= '0' && ch <= '9')
        || (ch >='a' && ch <= 'z')
        || (ch >= 'A' && ch <= 'Z'))) {
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
inline bool ObDFMUtil::is_element_can_omit(const ObDFMElem &elem)
{
  int ret_bool = true;
  int64_t flag = elem.elem_flag_;
  int64_t conf_group = ObDFMFlag::CONFLICT_GROUP_MAP[flag];
  if (ObDFMFlag::YEAR_GROUP == conf_group
      || ObDFMFlag::WEEK_OF_DAY_GROUP == conf_group
      || ObDFMFlag::MONTH_GROUP == conf_group
      || ObDFMFlag::DD == flag
      || ObDFMFlag::DS == flag
      || ObDFMFlag::DL == flag) {
    ret_bool = false;
  } else {
    //return true
  }
  return ret_bool;
}


}
}

#endif // OCEANBASE_ORACLE_FORMAT_MODELS_H_

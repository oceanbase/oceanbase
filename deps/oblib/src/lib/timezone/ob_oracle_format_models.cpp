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

#define USING_LOG_PREFIX  LIB_TIME

#include "lib/timezone/ob_oracle_format_models.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
namespace common
{

//do not define duplicate pattern
const ObTimeConstStr ObDFMFlag::PATTERN[ObDFMFlag::MAX_FLAG_NUMBER] =
{
  ObTimeConstStr("AD"),
  ObTimeConstStr("A.D."),
  ObTimeConstStr("BC"),
  ObTimeConstStr("B.C."),
  ObTimeConstStr("CC"),
  ObTimeConstStr("SCC"),
  ObTimeConstStr("D"),
  ObTimeConstStr("DAY"),
  ObTimeConstStr("DD"),
  ObTimeConstStr("DDD"),
  ObTimeConstStr("DY"),
  ObTimeConstStr("FF1"),
  ObTimeConstStr("FF2"),
  ObTimeConstStr("FF3"),
  ObTimeConstStr("FF4"),
  ObTimeConstStr("FF5"),
  ObTimeConstStr("FF6"),
  ObTimeConstStr("FF7"),
  ObTimeConstStr("FF8"),
  ObTimeConstStr("FF9"),
  ObTimeConstStr("FF"),
  ObTimeConstStr("HH"),
  ObTimeConstStr("HH24"),
  ObTimeConstStr("HH12"),
  ObTimeConstStr("IW"),
  ObTimeConstStr("I"),
  ObTimeConstStr("IY"),
  ObTimeConstStr("IYY"),
  ObTimeConstStr("IYYY"),
  ObTimeConstStr("MI"),
  ObTimeConstStr("MM"),
  ObTimeConstStr("MONTH"),
  ObTimeConstStr("MON"),
  ObTimeConstStr("AM"),
  ObTimeConstStr("A.M."),
  ObTimeConstStr("PM"),
  ObTimeConstStr("P.M."),
  ObTimeConstStr("Q"),
  ObTimeConstStr("RR"),
  ObTimeConstStr("RRRR"),
  ObTimeConstStr("SS"),
  ObTimeConstStr("SSSSS"),
  ObTimeConstStr("WW"),
  ObTimeConstStr("W"),
  ObTimeConstStr("Y,YYY"),
  ObTimeConstStr("YEAR"),
  ObTimeConstStr("SYEAR"),
  ObTimeConstStr("YYYY"),
  ObTimeConstStr("SYYYY"),
  ObTimeConstStr("YYY"),
  ObTimeConstStr("YY"),
  ObTimeConstStr("Y"),
  ObTimeConstStr("DS"),
  ObTimeConstStr("DL"),
  ObTimeConstStr("TZH"),
  ObTimeConstStr("TZM"),
  ObTimeConstStr("TZD"),
  ObTimeConstStr("TZR"),
  ObTimeConstStr("X"),
  ObTimeConstStr("J"),
  ObTimeConstStr("FM"),
  ObTimeConstStr(""),
};
constexpr int64_t ObDFMFlag::CONFLICT_GROUP_MAP[ObDFMFlag::MAX_FLAG_NUMBER];
constexpr int ObDFMFlag::CONFLICT_GROUP_ERR[ObDFMFlag::MAX_CONFLICT_GROUP_NUMBER];
constexpr int64_t ObDFMFlag::EXPECTED_MATCHING_LENGTH[ObDFMFlag::MAX_FLAG_NUMBER];
constexpr int64_t ObDFMFlag::ELEMENTFLAG_MAX_LEN[MAX_FLAG_NUMBER];

const ObOracleTimeLimiter ObDFMLimit::YEAR                       = {1, 9999,   OB_ERR_INVALID_YEAR_VALUE};
const ObOracleTimeLimiter ObDFMLimit::MONTH                      = {1, 12,     OB_ERR_INVALID_MONTH};
const ObOracleTimeLimiter ObDFMLimit::MONTH_DAY                  = {1, 31,     OB_ERR_DAY_OF_MONTH_RANGE};
const ObOracleTimeLimiter ObDFMLimit::WEEK_DAY                   = {1, 7,      OB_ERR_INVALID_DAY_OF_THE_WEEK};
const ObOracleTimeLimiter ObDFMLimit::YEAR_DAY                   = {1, 366,    OB_ERR_INVALID_DAY_OF_YEAR_VALUE};
const ObOracleTimeLimiter ObDFMLimit::HOUR12                     = {1, 12,     OB_ERR_INVALID_HOUR12_VALUE};
const ObOracleTimeLimiter ObDFMLimit::HOUR24                     = {0, 23,     OB_ERR_INVALID_HOUR24_VALUE};
const ObOracleTimeLimiter ObDFMLimit::MINUTE                     = {0, 59,     OB_ERR_INVALID_MINUTES_VALUE};
const ObOracleTimeLimiter ObDFMLimit::SECOND                     = {0, 59,     OB_ERR_INVALID_SECONDS_VALUE};
const ObOracleTimeLimiter ObDFMLimit::SECS_PAST_MIDNIGHT         = {0, 86399,  OB_ERR_INVALID_SECONDS_IN_DAY_VALUE};
const ObOracleTimeLimiter ObDFMLimit::TIMEZONE_HOUR_ABS          = {0, 15,     OB_INVALID_DATE_VALUE}; //ORA-01874: time zone hour must be between -15 and 15
const ObOracleTimeLimiter ObDFMLimit::TIMEZONE_MIN_ABS           = {0, 59,     OB_INVALID_DATE_VALUE}; //ORA-01875: time zone minute must be between -59 and 59
const ObOracleTimeLimiter ObDFMLimit::JULIAN_DATE                = {1, 5373484,OB_ERR_INVALID_JULIAN_DATE_VALUE}; // -4712-01-01 ~ 9999-12-31


int ObDFMUtil::match_int_value_with_comma(ObDFMParseCtx &ctx,
                                          const int64_t expected_len,
                                          int64_t &value_len,
                                          int32_t &result)
{
  int ret = OB_SUCCESS;
  int32_t temp_value = 0;
  int64_t real_data_len = 0;
  int64_t digits_len = 0;
  int64_t continuous_comma_count = 0;
  bool stop_flag = false;

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  while (OB_SUCC(ret) && !stop_flag
         && real_data_len < ctx.remain_len_ && digits_len < expected_len) { //look digits by # of value_len
    char cur_char = *(ctx.cur_ch_ + real_data_len);
    if (',' == cur_char) {
      continuous_comma_count++;
      if (continuous_comma_count == 2) {
        --real_data_len;
        stop_flag = true;
      } else {
        ++real_data_len;
      }
    } else {
      continuous_comma_count = 0;
      if (ObDFMUtil::is_split_char(cur_char)) {
        stop_flag = true;
      } else {
        if (OB_UNLIKELY(!isdigit(cur_char))) {
          ret = OB_ERR_NON_NUMERIC_CHARACTER_VALUE; //ORA-01858: a non-numeric character was found where a numeric was expected
          LOG_WARN("failed to match int value", K(ret));
        } else {
          temp_value *= 10;
          temp_value += cur_char - '0';
          ++real_data_len;
          ++digits_len;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_int_value_length(ctx, expected_len, real_data_len))) {
      LOG_WARN("int value length is not equal to expected len", K(ret), K(real_data_len), K(expected_len), K(ctx));
    } else {
      value_len = real_data_len;
      result = temp_value;
    }
  }
  return ret;
}

// use to parser hour+minute for TZR TZD
const char *ObDFMUtil::find_first_separator(ObDFMParseCtx &ctx)
{
  const char *result = nullptr;
  for (int64_t i = 0; nullptr == result && i < ctx.remain_len_; i++) {
    if (is_split_char(ctx.cur_ch_[i]) && ctx.cur_ch_[i] != 0x7f && ctx.cur_ch_[i] >= 0x20) {
      result = ctx.cur_ch_ + i;
    }
  }
  return result;
}

int ObDFMUtil::match_chars_until_space(ObDFMParseCtx &ctx, ObString &result, int64_t &value_len)
{
  int ret = OB_SUCCESS;
  int32_t str_len = 0;
  if (OB_UNLIKELY(ctx.is_parse_finish())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  while (OB_SUCC(ret) && str_len < ctx.remain_len_ && !isspace(ctx.cur_ch_[str_len])) {
    if (OB_UNLIKELY(str_len >= value_len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size over flow", K(ret));
    } else {
      ++str_len;
    }
  }
  if (OB_SUCC(ret)) {
    result.assign_ptr(ctx.cur_ch_, str_len);
    value_len = str_len;
  }
  return ret;
}

int ObDFMUtil::check_int_value_length(const ObDFMParseCtx &ctx,
                                      const int64_t expected_len,
                                      const int64_t real_data_len)
{
  int ret = OB_SUCCESS;
  /*
   * format need separate chars but input omit separate chars like:
   * to_date('20181225', 'YYYY-MM-DD')    input omit '-'
   * in this situation, the numeric value are matched in fixed length mode.
   * which means real_data_len should be equal to element expected length, or will return with an error
   */
  if (OB_SUCC(ret) && ctx.is_matching_by_expected_len_) {  //is true only in only in str_to_ob_time_oracle_dfm
    bool legal = true;
    if (ObDFMFlag::RR == ctx.expected_elem_flag_ || ObDFMFlag::RRRR == ctx.expected_elem_flag_) { //one special case
      legal = (2 == real_data_len || 4 == real_data_len);
    } else if (expected_len > 0) { //usual case, for numeric value
      legal = (real_data_len == expected_len);
    }

    if (OB_UNLIKELY(!legal)) {
      ret = OB_INVALID_DATE_VALUE;
    }
  }
  return ret;
}

int ObDFMUtil::match_int_value(ObDFMParseCtx &ctx,
                               const int64_t expected_len,
                               int64_t &value_len,
                               int32_t &result,
                               int32_t value_sign/* = 1*/)
{
  //only unsigned int
  int ret = OB_SUCCESS;
  int32_t temp_value = 0;
  int64_t real_data_len = 0;

  if (OB_UNLIKELY(!ctx.is_valid()) || OB_UNLIKELY(expected_len < 0)
      || OB_UNLIKELY(value_sign != -1 && value_sign != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(expected_len), K(value_sign));
  } else if (!isdigit(ctx.cur_ch_[0])) {  //check the first char
    ret = OB_ERR_NON_NUMERIC_CHARACTER_VALUE; //ORA-01858: a non-numeric character was found where a numeric was expected
  }

  int64_t date_max_len = std::min(ctx.remain_len_, expected_len);

  while (OB_SUCC(ret)
         && real_data_len < date_max_len
         && isdigit(ctx.cur_ch_[real_data_len])) {
    int32_t cur_digit = static_cast<int32_t>(ctx.cur_ch_[real_data_len] - '0');

    if (temp_value * 10LL > INT32_MAX - cur_digit) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("datetime part value is out of range", K(ret));
    } else {
      temp_value = temp_value * 10 + cur_digit;
      ++real_data_len;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_int_value_length(ctx, expected_len, real_data_len))) {
      LOG_WARN("int value length is not equal to expected len", K(ret), K(real_data_len), K(expected_len), K(ctx));
    } else {
      value_len = real_data_len;
      result = temp_value * value_sign;
    }
  }

  return ret;
}

int ObDFMUtil::parse_datetime_format_string(const ObString &fmt_str, ObDFMElemArr &elements,
                                            const bool support_double_quotes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fmt_str.length() > MAX_FORMAT_LENGTH)) {
    ret = OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER;
    LOG_WARN("datetime format too long", K(ret), K(fmt_str), K(fmt_str.length()));
  } else if (fmt_str.empty()) {
    //do nothing
  } else {
    ObDFMParseCtx parse_ctx(fmt_str.ptr(), fmt_str.length());
    int64_t skipped_len = 0;

    while (OB_SUCC(ret) && parse_ctx.remain_len_ > 0) {
      //skip separate chars
      skipped_len = skip_separate_chars(parse_ctx, OB_MAX_VARCHAR_LENGTH, '"');
      //parse one element from head
      if (OB_SUCC(ret) && parse_ctx.remain_len_ > 0) {
        ObDFMElem value_elem;

        if (parse_ctx.get_parsed_len() > 0) {
          value_elem.is_single_dot_before_ = (skipped_len == 1 && '.' == parse_ctx.cur_ch_[-1]);
        }

        if (OB_FAIL(parse_one_elem(parse_ctx, value_elem, support_double_quotes))) {
          LOG_WARN("failed to parse one element", K(ret));
        } else if (OB_FAIL(elements.push_back(value_elem))) {
          LOG_WARN("failed to push back elem", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("dmf parse format string result", K(fmt_str), K(elements));
    }
  }
  return ret;
}

/* search matched pattern */
int ObDFMUtil::parse_one_elem(ObDFMParseCtx &ctx, ObDFMElem &elem, const bool support_double_quotes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    int64_t winner_flag = ObDFMFlag::INVALID_FLAG;
    int64_t max_matched_len = 0;
    if ('"' == ctx.cur_ch_[0]) {
      if (!support_double_quotes) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support double-quotes in format when set variable", K(ret));
      } else {
        winner_flag = ObDFMFlag::LITERAL;
        max_matched_len = 1;
        while (max_matched_len < ctx.remain_len_ && ctx.cur_ch_[max_matched_len] != '"') {
          max_matched_len++;
        }
        if (max_matched_len < ctx.remain_len_ && '"' == ctx.cur_ch_[max_matched_len]) {
          max_matched_len++;
        }
      }
    } else {
      for (int64_t flag = 0; flag < ObDFMFlag::MAX_FLAG_NUMBER; ++flag) {
        const ObTimeConstStr &pattern = ObDFMFlag::PATTERN[flag];
        if (max_matched_len < pattern.len_ && ObDFMUtil::match_pattern_ignore_case(ctx, pattern)) {
          winner_flag = flag;
          max_matched_len = pattern.len_;
        }
      }
    }

    //uppercase adjust
    if (OB_SUCC(ret)) {
      if (OB_LIKELY(winner_flag != ObDFMFlag::INVALID_FLAG)) {
        elem.elem_flag_ = winner_flag;
        elem.offset_ = ctx.cur_ch_ - ctx.fmt_str_;
        elem.len_ = max_matched_len;
        switch (winner_flag) {
          case ObDFMFlag::MON:
          case ObDFMFlag::MONTH:
          case ObDFMFlag::DAY:
          case ObDFMFlag::DY:
          case ObDFMFlag::AM:
          case ObDFMFlag::PM:
          case ObDFMFlag::AD:
          case ObDFMFlag::BC: {
            if (OB_UNLIKELY(ctx.remain_len_ < 2)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("winner flag length must bigger than 2", K(ret), K(elem));
            } else if (isupper(ctx.cur_ch_[0]) && isupper(ctx.cur_ch_[1])) {
              elem.upper_case_mode_ = ObDFMElem::UpperCaseMode::ALL_CHARACTER;
            } else if (isupper(ctx.cur_ch_[0])) {
              elem.upper_case_mode_ = ObDFMElem::UpperCaseMode::ONLY_FIRST_CHARACTER;
            } else {
              elem.upper_case_mode_ = ObDFMElem::UpperCaseMode::NON_CHARACTER;
            }
            break;
          }

          case ObDFMFlag::AM2:
          case ObDFMFlag::PM2:
          case ObDFMFlag::AD2:
          case ObDFMFlag::BC2: {
            if (OB_UNLIKELY(ctx.remain_len_ < 4)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("winner flag length must bigger than 4", K(ret), K(elem));
            } else if (isupper(ctx.cur_ch_[0])) {
              elem.upper_case_mode_ = ObDFMElem::UpperCaseMode::ALL_CHARACTER;
            } else {
              elem.upper_case_mode_ = ObDFMElem::UpperCaseMode::NON_CHARACTER;
            }
          }
          default:
            //do nothing
            break;
        }
        ctx.update(max_matched_len);
      } else {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("date format is invalid", K(ret), K(ctx.remain_len_));
      }
    }
  }

  return ret;
}

//用于dfm_ele的打印，应该不需要修改，新flag不需要改，直接打印即可，因为不需要考虑大小写。
int ObDFMUtil::special_mode_sprintf(char *buf, const int64_t buf_len, int64_t &pos,
                                    const ObTimeConstStr &str, const ObDFMElem::UpperCaseMode mode, int64_t padding) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(str.len_ <= 0) || OB_ISNULL(str.ptr_) || OB_ISNULL(buf)
      || OB_UNLIKELY(padding > 0 && padding < str.len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str.len_), KP(str.ptr_), KP(buf));
  } else if (OB_UNLIKELY(pos + (padding > 0 ? padding : str.len_) >= buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is overflow", K(ret), K(buf_len), K(str.len_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < str.len_; ++i) {
      char cur_char = str.ptr_[i];
      if ((cur_char >= 'a' && cur_char <= 'z')
          || (cur_char >= 'A' && cur_char <= 'Z')) {
        switch (mode) {
          case ObDFMElem::UpperCaseMode::ALL_CHARACTER: {
              buf[pos++] = static_cast<char>(toupper(cur_char));
            break;
          }
          case ObDFMElem::UpperCaseMode::ONLY_FIRST_CHARACTER: {
            if (i == 0) {
              buf[pos++] = static_cast<char>(toupper(cur_char));
            } else {
              buf[pos++] = static_cast<char>(tolower(cur_char));
            }
            break;
          }
          case ObDFMElem::UpperCaseMode::NON_CHARACTER: {
            buf[pos++] = static_cast<char>(tolower(cur_char));
            break;
          }
          default: {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unknown dfm elem mode", K(ret), K(mode));
            break;
          }
        }
      } else {
        buf[pos++] = cur_char;
      }//end if
    }//end for
    for (int64_t i = str.len_; i < padding; ++i) {
      buf[pos++] = ' ';
    }
  }

  return ret;
}

//检查语义，新flag不需要修改，无冲突
int ObDFMUtil::check_semantic(const ObDFMElemArr &elements, ObFixedBitSet<OB_DEFAULT_BITSET_SIZE_FOR_DFM> &flag_bitmap, uint64_t mode)
{
  int ret = OB_SUCCESS;
  flag_bitmap.reset();
  int64_t conflict_group_bitset = 0;
  static_assert(ObDFMFlag::MAX_CONFLICT_GROUP_NUMBER < 20, "bitset will overflow, because conflict_group_bitset is type of int64_t");
  for (int64_t i = 0; OB_SUCC(ret) && i < elements.count(); ++i) {
    int64_t flag = elements.at(i).elem_flag_;
    if (ObDFMFlag::LITERAL == flag) {
      continue;
    }
    if (OB_UNLIKELY(!ObDFMFlag::is_flag_valid(flag))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid array value", K(ret), K(flag));
    }
    //The following datetime format elements can be used in timestamp and interval format models,
    //but not in the original DATE format model: FF, TZD, TZH, TZM, and TZR
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(flag >= ObDFMFlag::FF1 && flag <= ObDFMFlag::FF
                      && !HAS_TYPE_ORACLE(mode))) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("oracle date type can not have fractional seconds", K(ret), K(mode), K(ObDFMFlag::PATTERN[flag]));
      } else if (OB_UNLIKELY(!HAS_TYPE_TIMEZONE(mode) &&
                             (ObDFMFlag::TZD == flag || ObDFMFlag::TZR ==flag
                              || ObDFMFlag::TZH == flag || ObDFMFlag::TZM == flag))) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("oracle timestamp or timestamp with local timezone can not has timezone",
                 K(ret), K(mode), K(ObDFMFlag::PATTERN[flag]));
      }
    }

    //check no duplicate elem first
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(flag_bitmap.has_member(flag))) {
        ret = OB_ERR_FORMAT_CODE_APPEARS_TWICE; //ORA-01810: format code appears twice
        LOG_WARN("datetime format model check failed", K(ret), "flag", ObString(ObDFMFlag::PATTERN[flag].ptr_));
      } else if (OB_FAIL(flag_bitmap.add_member(flag))) {
        LOG_WARN("failed to add bitmap", K(ret), "flag", ObString(ObDFMFlag::PATTERN[flag].ptr_));
      }
    }
    //check conflict in group which the element belongs to
    if (OB_SUCC(ret)) {
      int64_t conf_group = ObDFMFlag::CONFLICT_GROUP_MAP[flag];
      if (ObDFMFlag::need_check_conflict(conf_group)) {
        if (OB_UNLIKELY(0 != (conflict_group_bitset & (1 << conf_group)))) {
          ret = ObDFMFlag::CONFLICT_GROUP_ERR[conf_group];
          LOG_WARN("invalid element in group conflict check", K(ret),
                   "elem offset", i,
                   "elem name", ObDFMFlag::PATTERN[flag].ptr_,
                   "conflict_group_id", conf_group);
        } else {
          conflict_group_bitset |= (1 << conf_group);
        }
      }
    }//end if
  }//end for

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(flag_bitmap.has_member(ObDFMFlag::TZM)
                    && !flag_bitmap.has_member(ObDFMFlag::TZH))) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("given TZM without TZH is forbidden", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("conflict summary", K(conflict_group_bitset));
  }
  return ret;
}

int64_t ObDFMFlag::calc_max_len_of_patterns()
{
  int64_t result = 0;
  for (int64_t flag = 0; flag < MAX_FLAG_NUMBER; ++flag) {
    if (result < PATTERN[flag].len_) {
      result = PATTERN[flag].len_;
    }
  }
  return result;
}

//需要适配新flag
ObString ObDFMElem::get_elem_name() const
{
  ObString result;
  if (ObDFMFlag::is_flag_valid(elem_flag_)) {
    result = ObDFMFlag::PATTERN[elem_flag_].to_obstring();
  } else {
    result = ObString("INVALID ELEMENT");
  }
  return result;
}

int ObDFMUtil::check_ctx_valid(ObDFMParseCtx &ctx, int err_code)
{
  return ctx.is_valid() ? OB_SUCCESS : err_code;
}

int ObDFMUtil::match_char(ObDFMParseCtx &ctx, const char c, const int err_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_ctx_valid(ctx, err_code))) {
    LOG_WARN("parsing finished", K(ret));
  } else if (OB_UNLIKELY(c != ctx.cur_ch_[0])) {
    ret = err_code;
  } else {
    ctx.update(1);
  }
  return ret;
}

int ObDFMUtil::validate_literal_elem(const ObDFMElem &elem, const ObString &format,
                                     ObString &literal)
{
  int ret = OB_SUCCESS;
  int64_t start = elem.offset_;
  int64_t len = elem.len_;
  const char *format_ptr = format.ptr();
  if (OB_UNLIKELY(start + len > format.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("literal out of range of format, maybe parser error", K(ret), K(format), K(elem));
  } else if (OB_UNLIKELY('"' != format_ptr[start])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("literal not start with double quote", K(ret), K(format), K(elem));
  } else {
    bool end_with_double_quote = '"' == format_ptr[start + len - 1];
    int64_t literal_len = len - 1 - (end_with_double_quote ? 1 : 0);
    literal.assign_ptr(format.ptr() + start + 1, literal_len);
  }
  return ret;
}

int ObDFMUtil::match_literal_ignore_case(ObDFMParseCtx &ctx, const ObDFMElem &elem,
                                         const ObString &format, bool &matched,
                                         int64_t &matched_len)
{
  int ret = OB_SUCCESS;
  ObString literal;
  matched = false;
  if (OB_FAIL(validate_literal_elem(elem, format, literal))) {
    LOG_WARN("validate literal elem failed", K(ret), K(format), K(elem));
  } else if (ctx.remain_len_ >= literal.length()) {
    matched = (0 == strncasecmp(ctx.cur_ch_, literal.ptr(), literal.length()));
    if (matched) {
      matched_len = literal.length();
    }
  }
  LOG_DEBUG("match literal ignore case", K(ctx), K(ctx.remain_len_), K(literal), K(matched),
    K(matched_len));
  return ret;
}

int ObDFMUtil::print_literal(char *buf, const int64_t buf_len, int64_t &pos, const ObDFMElem &elem,
                             const ObString &format)
{
  int ret = OB_SUCCESS;
  ObString literal;
  if (OB_FAIL(validate_literal_elem(elem, format, literal))) {
    LOG_WARN("validate literal elem failed", K(ret), K(format), K(elem));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s", literal.length(), literal.ptr()))) {
    LOG_WARN("databuff printf failed", K(ret), K(buf_len), K(pos), K(literal));
  } else {
    LOG_DEBUG("dfm print literal", K(ObString(pos, buf)), K(literal), K(elem), K(format));
  }
  return ret;
}

}
}

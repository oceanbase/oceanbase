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
#include "ob_fast_parser.h"
#include "sql/udr/ob_udr_struct.h"
#include "share/ob_define.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/worker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

#define CHECK_AND_PROCESS_HINT(str, size) \
do { \
  if (CHECK_EQ_STRNCASECMP(str, size)) { \
    raw_sql_.scan(size); \
    if (OB_FAIL(process_hint())) { \
      LOG_WARN("failed to process hint", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos)); \
    } \
  } \
} while (0)

int ObFastParser::parse(const common::ObString &stmt,
                        const FPContext &fp_ctx,
                        common::ObIAllocator &allocator,
                        char *&no_param_sql,
                        int64_t &no_param_sql_len,
                        ParamList *&param_list,
                        int64_t &param_num)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_parse);
  int ret = OB_SUCCESS;
  int64_t values_token_pos = 0;
  if (!lib::is_oracle_mode()) {
    ObFastParserMysql fp(allocator, fp_ctx);
    if (OB_FAIL(fp.parse(stmt, no_param_sql, no_param_sql_len, param_list, param_num, values_token_pos))) {
      LOG_WARN("failed to fast parser", K(stmt));
    }
  } else {
    ObFastParserOracle fp(allocator, fp_ctx);
    if (OB_FAIL(fp.parse(stmt, no_param_sql, no_param_sql_len, param_list, param_num, values_token_pos))) {
      LOG_WARN("failed to fast parser", K(stmt));
    }
  }
  return ret;
}

int ObFastParser::parse(const common::ObString &stmt,
                        const FPContext &fp_ctx,
                        common::ObIAllocator &allocator,
                        char *&no_param_sql,
                        int64_t &no_param_sql_len,
                        ParamList *&param_list,
                        int64_t &param_num,
                        ObFastParserResult &fp_result,
                        int64_t &values_token_pos)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_parse);
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode()) {
    ObFastParserMysql fp(allocator, fp_ctx);
    if (OB_FAIL(fp.parse(stmt, no_param_sql, no_param_sql_len, param_list, param_num, values_token_pos))) {
      LOG_WARN("failed to fast parser", K(stmt));
    } else {
      fp_result.question_mark_ctx_ = fp.get_question_mark_ctx();
      fp_result.values_tokens_.set_capacity(fp.get_values_tokens().count());
      for (int64_t i = 0; OB_SUCC(ret) && i < fp.get_values_tokens().count(); ++i) {
        if (OB_FAIL(fp_result.values_tokens_.push_back(ObValuesTokenPos(
                                                     fp.get_values_tokens().at(i).no_param_sql_pos_,
                                                     fp.get_values_tokens().at(i).param_idx_)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  } else {
    ObFastParserOracle fp(allocator, fp_ctx);
    if (OB_FAIL(fp.parse(stmt, no_param_sql, no_param_sql_len, param_list, param_num, values_token_pos))) {
      LOG_WARN("failed to fast parser", K(stmt));
    } else {
      fp_result.question_mark_ctx_ = fp.get_question_mark_ctx();
    }
  }
  return ret;
}

ObFastParserBase::ObFastParserBase(
  ObIAllocator &allocator,
  const FPContext fp_ctx) :
  no_param_sql_(nullptr), no_param_sql_len_(0),
  param_num_(0), is_oracle_mode_(false),
  is_batched_multi_stmt_split_on_(fp_ctx.enable_batched_multi_stmt_),
  is_udr_mode_(fp_ctx.is_udr_mode_),
  def_name_ctx_(fp_ctx.def_name_ctx_),
  cur_token_begin_pos_(0), copy_begin_pos_(0), copy_end_pos_(0),
  tmp_buf_(nullptr), tmp_buf_len_(0), last_escape_check_pos_(0),
  param_node_list_(nullptr), tail_param_node_(nullptr),
  cur_token_type_(INVALID_TOKEN), allocator_(allocator),
  found_insert_status_(NOT_FOUND_INSERT_TOKEN), values_token_pos_(0),
  parse_next_token_func_(nullptr), process_idf_func_(nullptr)
{
	question_mark_ctx_.count_ = 0;
  question_mark_ctx_.capacity_ = 0;
  question_mark_ctx_.by_ordinal_ = false;
  question_mark_ctx_.by_name_ = false;
  question_mark_ctx_.name_ = nullptr;
  charset_type_ = ObCharset::charset_type_by_coll(fp_ctx.charsets4parser_.string_collation_);
  charset_info_ = ObCharset::get_charset(fp_ctx.charsets4parser_.string_collation_);
}

int ObFastParserBase::parse(const ObString &stmt,
                            char *&no_param_sql,
                            int64_t &no_param_sql_len,
                            ParamList *&param_list,
                            int64_t &param_num,
                            int64_t &values_token_pos)
{
  int ret = OB_SUCCESS;
  int64_t len = stmt.length();
  if (OB_ISNULL(no_param_sql_ =
      static_cast<char *>(allocator_.alloc((len + 1))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(len));
  } else {
    no_param_sql_[0] = '\0';
    while (len > 0 && is_space(stmt[len - 1])) {
      --len;
    }
    // remove the ‘\0’ at the end
    if (len > 0 && '\0' == stmt[len - 1]) {
      --len;
    }
    while (len > 0 && is_space(stmt[len - 1])) {
      --len;
    }
    raw_sql_.init(stmt.ptr(), len);
    if (OB_LIKELY(parse_next_token_func_ != nullptr)) {
      if (OB_FAIL((this->*parse_next_token_func_)())) {
        LOG_WARN("failed to parse next token", K(ret), K(stmt), K(raw_sql_.cur_pos_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    no_param_sql = no_param_sql_;
    no_param_sql_len = no_param_sql_len_;
    param_list = param_node_list_;
    param_num = param_num_;
    if (found_insert_status_ == FOUND_INSERT_TOKEN_ONCE) {
      values_token_pos = values_token_pos_;
    }
  }
  return ret;
}

int ObFastParserBase::copy_trimed_data_buff(char *new_sql_buf,
                                            int64_t buf_len,
                                            int64_t &pos,
                                            const int64_t start_pos,
                                            const int64_t end_pos,
                                            ObRawSql &raw_sql)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("print copy_trimed_data_buff", K(start_pos), K(end_pos), K(raw_sql.to_string()));
  if (start_pos < end_pos) {
    if (OB_FAIL(databuff_memcpy(new_sql_buf, buf_len, pos, end_pos - start_pos, raw_sql.ptr(start_pos)))) {
      LOG_WARN("fail to do copy", K(ret), K(buf_len), K(pos), K(start_pos), K(end_pos));
    }
  }
  return ret;
}

int ObFastParserBase::do_trim_for_insert(char *new_sql_buf,
                                         int64_t buff_len,
                                         const ObString &no_trim_sql,
                                         ObString &after_trim_sql,
                                         bool &trimed_succ)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  trimed_succ = false;
  int64_t space_len = 0;
  after_trim_sql.reset();
  ObRawSql raw_sql;
  raw_sql.init(no_trim_sql.ptr(), no_trim_sql.length());
  int64_t start_pos = raw_sql.cur_pos_;
  int64_t end_pos = raw_sql.cur_pos_;
  TRIM_STATE trim_state = START_TRIM_STATE;
  while(trim_state != TRIM_FAILED && OB_SUCC(ret) && !raw_sql.is_search_end()) {
    char ch = raw_sql.char_at(raw_sql.cur_pos_);
    switch (trim_state) {
      case START_TRIM_STATE:
        if (IS_MULTI_SPACE_V2(raw_sql, raw_sql.cur_pos_, space_len)) {
          skip_space(raw_sql);
          start_pos = raw_sql.cur_pos_;
        } else if (is_split_character(raw_sql)) {
          trim_state = WITH_SPLII_CH;
        } else {
          raw_sql.scan();
          trim_state = WITH_OTHER_CH;
        }
        break;
      case WITH_SPLII_CH:
        if (IS_MULTI_SPACE_V2(raw_sql, raw_sql.cur_pos_, space_len)) {
          end_pos = raw_sql.cur_pos_;
          if (OB_FAIL(copy_trimed_data_buff(new_sql_buf, buff_len, pos, start_pos, end_pos, raw_sql))) {
            LOG_WARN("fail to do copy", K(ret), K(buff_len), K(pos), K(start_pos), K(end_pos));
          } else {
            skip_space(raw_sql);
            start_pos = raw_sql.cur_pos_;
            end_pos = start_pos;
          }
        } else if (is_split_character(raw_sql)) {
          // do nothing
        } else {
          raw_sql.scan();
          trim_state = WITH_OTHER_CH;
        }
        break;
      case WITH_OTHER_CH:
        if (IS_MULTI_SPACE_V2(raw_sql, raw_sql.cur_pos_, space_len)) {
          end_pos = raw_sql.cur_pos_;
          trim_state = WITH_SPACE_CH;
          if (OB_FAIL(copy_trimed_data_buff(new_sql_buf, buff_len, pos, start_pos, end_pos, raw_sql))) {
            LOG_WARN("fail to do copy", K(ret), K(buff_len), K(pos), K(start_pos), K(end_pos));
          } else {
            skip_space(raw_sql);
            start_pos = raw_sql.cur_pos_;
            end_pos = start_pos;
          }
        } else if (is_split_character(raw_sql)) {
          trim_state = WITH_SPLII_CH;
        } else {
          raw_sql.scan();
        }
        break;
      case WITH_SPACE_CH:
        if (IS_MULTI_SPACE_V2(raw_sql, raw_sql.cur_pos_, space_len)) {
          end_pos = raw_sql.cur_pos_;
          if (OB_FAIL(copy_trimed_data_buff(new_sql_buf, buff_len, pos, start_pos, end_pos, raw_sql))) {
            LOG_WARN("fail to do copy", K(ret), K(buff_len), K(pos), K(start_pos), K(end_pos));
          } else {
            skip_space(raw_sql);
            start_pos = raw_sql.cur_pos_;
            end_pos = start_pos;
          }
        } else if (is_split_character(raw_sql)) {
          trim_state = WITH_SPLII_CH;
        } else {
          trim_state = TRIM_FAILED;
        }
        break;
      case TRIM_FAILED:
        // do nothing
        break;
      }

    if (raw_sql.is_search_end()) {
      end_pos = raw_sql.cur_pos_;
      if (OB_FAIL(copy_trimed_data_buff(new_sql_buf, buff_len, pos, start_pos, end_pos, raw_sql))) {
        LOG_WARN("fail to do copy", K(ret));
      }
    }
  } // end while


  if (OB_SUCC(ret) && trim_state != TRIM_FAILED) {
    after_trim_sql.assign_ptr(new_sql_buf, pos);
    trimed_succ = true;
  }

  LOG_DEBUG("print after do_trim", K(buff_len), K(pos), K(no_trim_sql), K(after_trim_sql), K(trimed_succ));
  return ret;
}

int ObFastParserBase::parser_insert_str(common::ObIAllocator &allocator,
                                        int64_t values_token_pos,
                                        const ObString &old_no_param_sql,
                                        ObString &new_truncated_sql,
                                        bool &can_batch_opt,
                                        int64_t &params_count,
                                        int64_t &upd_params_count,
                                        int64_t &lenth_delta,
                                        int64_t &row_count)
{
  int ret = OB_SUCCESS;
  can_batch_opt = false;
  params_count = 0;
  row_count = 0;
  upd_params_count = 0;
  lenth_delta = 0;
  bool is_valid = false;
  bool is_insert_up = false;
  int64_t first_end_pos = 0;
  if (values_token_pos != 0) {
    ObRawSql raw_sql;
    raw_sql.init(old_no_param_sql.ptr(), old_no_param_sql.length());
    raw_sql.cur_pos_ = values_token_pos - 1;
    if (0 == raw_sql.strncasecmp(raw_sql.cur_pos_, "values", 6)) {
      bool is_first = true;
      is_valid = true;
      ObString first_str;
      ObString first_trimed_str;
      ObString cur_str;
      ObString cur_trimed_str;
      int64_t end_pos = 0;
      char *first_str_buf = nullptr;
      char *other_str_buf = nullptr;
      int64_t first_str_buf_len = 0;

      // scan the length of 'values'
      raw_sql.scan(6);
      while (OB_SUCC(ret) && is_valid && !raw_sql.is_search_end()) {
        cur_str.reset();
        if (is_insert_up) {
          is_valid = false;
        } else if (OB_FAIL(get_one_insert_row_str(raw_sql,
                                                  cur_str,
                                                  is_valid,
                                                  params_count,
                                                  is_insert_up,
                                                  upd_params_count,
                                                  end_pos))) {
          LOG_WARN("fail to get one insert row", K(ret));
        } else if (!is_valid) {
          LOG_WARN("get insert row failed", K(raw_sql.to_string()));
        } else if (is_first) {
          is_first = false;
          first_str.assign_ptr(cur_str.ptr(), cur_str.length());
          new_truncated_sql.assign_ptr(old_no_param_sql.ptr(), end_pos + 1);
          first_end_pos = end_pos;
          LOG_DEBUG("print first_str", K(first_str), K(is_valid), K(end_pos), K(old_no_param_sql), K(new_truncated_sql));
          row_count++;
        } else if (first_str != cur_str) {
          is_valid = false;
          row_count++;
          bool trimed_succ = false;
          if (first_str_buf == nullptr) {
            first_str_buf_len = first_str.length() + 1; // copy函数要求的最后边必须有一位填'\0'
            if (OB_ISNULL(first_str_buf = static_cast<char*>(allocator.alloc(first_str_buf_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory", K(ret), K(first_str_buf_len));
            } else if (OB_ISNULL(other_str_buf = static_cast<char*>(allocator.alloc(first_str_buf_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory", K(ret), K(first_str_buf_len));
            } else if (OB_FAIL(do_trim_for_insert(first_str_buf,
                                                  first_str_buf_len,
                                                  first_str,
                                                  first_trimed_str,
                                                  trimed_succ))) {
              LOG_WARN("fail to do trim", K(ret), K(first_str));
            } else if (!trimed_succ) {
              // trim failed
            } else if (OB_FAIL(do_trim_for_insert(other_str_buf,
                                                  first_str_buf_len,
                                                  cur_str,
                                                  cur_trimed_str,
                                                  trimed_succ))) {
              LOG_WARN("fail to do trim", K(ret), K(cur_str));
            } else if (!trimed_succ) {
              // trim failed
            } else if (first_trimed_str == cur_trimed_str) {
              is_valid = true;
            }
          } else if (FALSE_IT(cur_trimed_str.reset())) {
            // cur_trimed_str will be reused，need do reset, other_str_buf also will be reuse
          } else if (OB_ISNULL(other_str_buf)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null ptr", K(ret));
          } else if (OB_FAIL(do_trim_for_insert(other_str_buf,
                                                first_str_buf_len,
                                                cur_str,
                                                cur_trimed_str,
                                                trimed_succ))) {
            LOG_WARN("fail to do trim", K(ret), K(cur_str));
          } else if (!trimed_succ) {
            // trim failed
          } else if (first_trimed_str == cur_trimed_str) {
            is_valid = true;
          }
        } else {
          row_count++;
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (is_valid && is_insert_up) {
          char *new_sql_buf = NULL;
          int64_t pos = 0;
          int64_t on_duplicate_pos = end_pos + 1;
          int64_t on_duplicate_length = old_no_param_sql.length() - on_duplicate_pos;
          int64_t insert_length = new_truncated_sql.length();
          int64_t final_length = insert_length + on_duplicate_length + 1;
          lenth_delta = end_pos - first_end_pos;
          LOG_DEBUG("is insert_up print final length",
              K(on_duplicate_length), K(insert_length), K(end_pos), K(lenth_delta), K(final_length));
          if (OB_ISNULL(new_sql_buf = static_cast<char*>(allocator.alloc(final_length)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(final_length));
          } else if (OB_FAIL(databuff_memcpy(
              new_sql_buf, final_length, pos, insert_length, old_no_param_sql.ptr()))) {
            LOG_WARN("failed to deep copy insert string",
                K(ret), K(final_length), K(pos), K(new_truncated_sql));
          } else if (OB_FAIL(databuff_memcpy(
              new_sql_buf, final_length, pos, (on_duplicate_length), (old_no_param_sql.ptr() + on_duplicate_pos)))) {
            LOG_WARN("failed to deep copy on duplicate key string",
                K(ret), K(final_length), K(pos), K(old_no_param_sql.length()), K(end_pos),
                K(ObString(on_duplicate_length, old_no_param_sql.ptr() + on_duplicate_pos)));
          } else {
            new_truncated_sql.reset();
            new_truncated_sql.assign_ptr(new_sql_buf, pos);
            LOG_DEBUG("succ to deep copy on duplicate key string",
                K(ret), K(final_length), K(pos), K(old_no_param_sql), K(end_pos), K(new_truncated_sql));
          }
        }
      } // end while
      can_batch_opt = is_valid;
    }
  }
  LOG_DEBUG("after parser insert print curr_sql", K(old_no_param_sql), K(new_truncated_sql),
        K(can_batch_opt), K(params_count), K(row_count));
  return ret;
}

inline void ObFastParserBase::process_leading_space()
{
  int64_t space_len = 0;
  while (!raw_sql_.search_end_ && IS_MULTI_SPACE(raw_sql_.cur_pos_, space_len)) {
    cur_token_type_ = NORMAL_TOKEN;
    copy_end_pos_++;
    raw_sql_.scan(space_len);
  }
}

inline int64_t ObFastParserBase::is_identifier_flags(const int64_t pos)
{
  int64_t idf_pos = -1;
  char ch = raw_sql_.char_at(pos);
  if (is_identifier_char(ch)) {
    idf_pos = pos + 1;
  } else if (is_space(ch) || is_comma(ch)
            || is_left_parenthesis(ch) || is_right_parenthesis(ch)) {
    // Most of the time, if it is not an identifier character, it maybe a space,
    // comma, opening parenthesis, or closing parenthesis. This judgment logic is
    // added here to avoid the next judgment whether it is utf8 char or gbk char
  } else if (!is_oracle_mode_) {
    idf_pos = notascii_gb_char(pos);
  } else if (CHARSET_UTF8MB4 == charset_type_ || CHARSET_UTF16 == charset_type_) {
    idf_pos = is_utf8_char(pos);
  } else if (ObCharset::is_gb_charset(charset_type_)) {
    idf_pos = is_gbk_char(pos);
  } else if (CHARSET_LATIN1 == charset_type_) {
    idf_pos = is_latin1_char(pos);
  }
  return idf_pos;
}

/**
 * Used to parse {space}*{int_num}{space}*
 * @param [in] : pos the position of the first character
 * Return the next position of the position that meets the condition
 * and return -1 if it is not satisfied
 */
int64_t ObFastParserBase::is_digit_with_space(int64_t pos)
{
  int64_t end_pos = -1;
  int64_t space_len = 0;
  char ch = raw_sql_.char_at(pos);
  while (IS_MULTI_SPACE(pos, space_len)) {
    pos += space_len;
  }
  ch = raw_sql_.char_at(pos);
  if (is_digit(ch)) {
    ch = raw_sql_.char_at(++pos);
    while (is_digit(ch)) {
      ch = raw_sql_.char_at(++pos);
    }
    while (IS_MULTI_SPACE(pos, space_len)) {
      pos += space_len;
    }
    end_pos = pos;
  }
  return end_pos;
}

/**
 * Used to parse {space}*\({space}*{int_num}{space}*\)
 * @param [in] : pos the position of the first character
 * Return the next position of the position that meets the condition
 * and return -1 if it is not satisfied
 */
int64_t ObFastParserBase::is_interval_pricision(int64_t pos)
{
  int64_t interval_end_pos = -1;
  int64_t byte_len = 0;
  while (IS_MULTI_SPACE(pos, byte_len)) {
    pos += byte_len;
  }
  if (IS_MULTI_LEFT_PARENTHESIS(pos, byte_len)) {
    pos += byte_len;
    int next_pos = is_digit_with_space(pos);
    if (-1 != next_pos) {
      pos = next_pos;
    }
    if (IS_MULTI_RIGHT_PARENTHESIS(pos, byte_len)) {
      pos += byte_len;
      interval_end_pos = pos;
    }
  }
  return interval_end_pos;
}

// \({space}*{int_num}{space}*,{space}*{int_num}{space}*\)
// eg: second(123, 568)
inline int64_t ObFastParserBase::is_2num_second(int64_t pos)
{
#define IS_SPACE_DIGIT_SPACE() \
  do { \
    ch = raw_sql_.char_at(pos); \
    while (IS_MULTI_SPACE(pos, byte_len)) { \
      pos += byte_len; \
    } \
    ch = raw_sql_.char_at(pos); \
    if (is_digit(ch)) { \
      ch = raw_sql_.char_at(++pos); \
      while (is_digit(ch)) { \
        ch = raw_sql_.char_at(++pos); \
      } \
      while (IS_MULTI_SPACE(pos, byte_len)) { \
        pos += byte_len; \
        ch = raw_sql_.char_at(pos); \
      } \
      is_space_digit_space = true; \
    } \
  } while (0)

  int64_t end_pos = -1;
  int64_t byte_len = 0;
  char ch = raw_sql_.char_at(pos);
  bool is_space_digit_space = false;
  IS_SPACE_DIGIT_SPACE();
  if (is_space_digit_space && IS_MULTI_COMMA(pos, byte_len)) {
    pos += byte_len;
    IS_SPACE_DIGIT_SPACE();
    if (is_space_digit_space && IS_MULTI_RIGHT_PARENTHESIS(pos, byte_len)) {
      pos += byte_len;
      end_pos = pos;
    }
  }
  return end_pos;
}

// to{space}+(day|hour|minute|second{interval_pricision}?)
int64_t ObFastParserBase::is_interval_ds(int64_t pos)
{
  int64_t end_pos = -1;
  int64_t space_len = 0;
  if (0 == raw_sql_.strncasecmp(pos, "to", 2)) {
    pos += 2;
    if (IS_MULTI_SPACE(pos, space_len)) {
      pos += space_len;
      while (IS_MULTI_SPACE(pos, space_len)) {
        pos += space_len;
      }
      if (0 == raw_sql_.strncasecmp(pos, "day", 3)) {
        pos += 3;
        end_pos = pos;
      } else if (0 == raw_sql_.strncasecmp(pos, "hour", 4)) {
        pos += 4;
        end_pos = pos;
      } else if (0 == raw_sql_.strncasecmp(pos, "minute", 6)) {
        pos += 6;
        end_pos = pos;
      } else if (0 == raw_sql_.strncasecmp(pos, "second", 6)) {
        pos += 6;
        end_pos = pos;
        int64_t next_pos = is_interval_pricision(pos);
        if (-1 != next_pos) {
          end_pos = next_pos;
        }
      }
    }
  }
  return end_pos;
}

// to{space}+(year|month)
inline int64_t ObFastParserBase::is_interval_ym(int64_t pos)
{
  int64_t end_pos = -1;
  int64_t space_len = 0;
  if (0 == raw_sql_.strncasecmp(pos, "to", 2)) {
    pos += 2;
    if (IS_MULTI_SPACE(pos, space_len)) {
      pos += space_len;
      while (IS_MULTI_SPACE(pos, space_len)) {
        pos += space_len;
      }
      if (0 == raw_sql_.strncasecmp(pos, "year", 4)) {
        pos += 4;
        end_pos = pos;
      } else if (0 == raw_sql_.strncasecmp(pos, "month", 5)) {
        pos += 5;
        end_pos = pos;
      } 
    }
  }
  return end_pos;
}

/**
 * Used to parse ({interval_pricision}{space}*|{space}+)to{space}+
 * @param [in] : pos the position of the first character
 * Return the next position of the position that meets the condition
 * and return -1 if it is not satisfied
 */
int64_t ObFastParserBase::is_interval_pricision_with_space(int64_t pos)
{
  int64_t end_pos = -1;
  int64_t space_len = 0;
  // deal with ({interval_pricision}{space}*|{space}+)to{space}+(year|month)
  if (IS_MULTI_SPACE(pos, space_len)) { // {space}+
    end_pos = pos;
    pos += space_len;
  }
  int next_pos = is_interval_pricision(pos);
  if (-1 != next_pos) {
    // {interval_pricision}, this part does not need to be rolled back, so update cur_pos_
    raw_sql_.cur_pos_ = next_pos;
    // The regular expression that satisfies the part of ({interval_pricision}{space}*|{space}+)
    end_pos = next_pos;
  }
  return end_pos;
}

/**
 * Used to parse the following interval-related tokens compatible with oracle
 * Interval{whitespace}?'[^']*'{space}*(year|month){interval_pricision}?
 * Interval{whitespace}?'[^']*'{space}*(year|month)({interval_pricision}{space}*|
 * {space}+)to{space}+(year|month)
 * Interval{whitespace}?'[^']*'{space}*second{space}*\({space}*{int_num}{space}*,
 * {space}*{int_num}{space}*\)
 * Interval{whitespace}?'[^']*'{space}*(day|hour|minute|second){interval_pricision}?
 * Interval{whitespace}?'[^']*'{space}*(day|hour|minute|second)({interval_pricision}{space}*|
 * {space}+)to{space}+(day|hour|minute|second{interval_pricision}?)
 */
int ObFastParserBase::process_interval()
{
#define CHECK_AND_PROCESS_ROLLBACK(type, is_second) \
  do { \
    /* ({interval_pricision}{space}*|{space}+)*/ \
    int back_pos = raw_sql_.cur_pos_; \
    int next_pos = is_interval_pricision_with_space(raw_sql_.cur_pos_); \
    if (-1 != next_pos) { \
      raw_sql_.cur_pos_ = next_pos; \
      back_pos = raw_sql_.cur_pos_; \
      ch = raw_sql_.char_at(raw_sql_.cur_pos_); \
      while (IS_MULTI_SPACE(raw_sql_.cur_pos_, byte_len)) { \
        ch = raw_sql_.scan(byte_len); \
      } \
      if (is_second) { \
        char prev_char = raw_sql_.char_at(next_pos - 1); \
        /* \({space}*{int_num}{space}*,{space}*{int_num}{space}*\) */ \
        if (!IS_MULTI_RIGHT_PARENTHESIS(next_pos - 1, byte_len) \
          && IS_MULTI_LEFT_PARENTHESIS(raw_sql_.cur_pos_, byte_len)) { \
          ch = raw_sql_.scan(byte_len); \
          next_pos = is_2num_second(raw_sql_.cur_pos_); \
        } else { \
          next_pos = is_2num_second(raw_sql_.cur_pos_); \
        } \
      } else { \
        if (T_INTERVAL_DS == type) { \
          next_pos = is_interval_ds(raw_sql_.cur_pos_); \
        } else { \
          next_pos = is_interval_ym(raw_sql_.cur_pos_); \
        } \
      } \
    } else if (is_second && IS_MULTI_LEFT_PARENTHESIS(raw_sql_.cur_pos_, byte_len)) { \
      /* There is no space, followed by'('. Used to deal with \({space}*{int_num}{space}*,*/ \
      /*{space}*{int_num}{space}*\) */ \
      ch = raw_sql_.scan(byte_len); \
      next_pos = is_2num_second(raw_sql_.cur_pos_); \
    } \
    if (-1 != next_pos) { \
      raw_sql_.cur_pos_ = next_pos; \
    } else { \
      raw_sql_.cur_pos_ = back_pos; \
    } \
  } while (0)

#define CHECK_EQ_AND_PROCESS_ROLLBACK(str, size, type, is_second) \
  do { \
    if (CHECK_EQ_STRNCASECMP(str, size)) { \
      ch = raw_sql_.scan(size); \
      param_type = type; \
      cur_token_type_ = PARAM_TOKEN; \
      CHECK_AND_PROCESS_ROLLBACK(type, is_second); \
    } \
  } while (0) 

  int ret = OB_SUCCESS;
  int64_t byte_len = 0;
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  tmp_buf_len_ = 0;
  ObItemType param_type = T_INVALID;
  if (nullptr == tmp_buf_ &&
      OB_ISNULL(tmp_buf_ = static_cast<char *>(allocator_.alloc(raw_sql_.raw_sql_len_ + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(raw_sql_.raw_sql_len_));
  } else {
    // deal with '[^']*'
    while ('\'' != ch && !raw_sql_.is_search_end()) {
      tmp_buf_[tmp_buf_len_++] = ch;
      ch = raw_sql_.scan();
    }
    if ('\'' == ch) {
      ch = raw_sql_.scan();
      // deal with {space}*
      while (IS_MULTI_SPACE(raw_sql_.cur_pos_, byte_len)) {
        ch = raw_sql_.scan(byte_len);
      }
      // hit Interval{whitespace}?'[^']*'{space}*(year|month){interval_pricision}?
      CHECK_EQ_AND_PROCESS_ROLLBACK("year", 4, T_INTERVAL_YM, false);
      CHECK_EQ_AND_PROCESS_ROLLBACK("month", 5, T_INTERVAL_YM, false);
      CHECK_EQ_AND_PROCESS_ROLLBACK("minute", 6, T_INTERVAL_DS, false);
      CHECK_EQ_AND_PROCESS_ROLLBACK("day", 3, T_INTERVAL_DS, false);
      CHECK_EQ_AND_PROCESS_ROLLBACK("hour", 4, T_INTERVAL_DS, false);
      CHECK_EQ_AND_PROCESS_ROLLBACK("second", 6, T_INTERVAL_DS, true);
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
    }
  }
  if (OB_SUCC(ret) && PARAM_TOKEN == cur_token_type_) {
    char *buf = nullptr;
    int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
    int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
    need_mem_size += text_len + 1; // '\0'
    int64_t str_len = tmp_buf_len_;
    need_mem_size += str_len + 1; // '\0'
    // allocate all the memory needed at once
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
    } else {
      ParseNode *node = new_node(buf, param_type);
      node->str_len_ = str_len;
      node->raw_text_ = parse_strdup_with_replace_multi_byte_char(
      raw_sql_.ptr(cur_token_begin_pos_), text_len, buf, node->text_len_);
      // buf points to the beginning of the next available memory
      buf += text_len + 1;
      node->str_value_ = parse_strndup(tmp_buf_, tmp_buf_len_, buf);
      // buf points to the beginning of the next available memory
      buf += str_len + 1;
      node->raw_sql_offset_ = cur_token_begin_pos_;
      lex_store_param(node, buf);
    }
  }
  return ret;
}

int ObFastParserBase::process_insert_or_replace(const char *str, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (CHECK_EQ_STRNCASECMP(str, size)) {
    raw_sql_.scan(size);
    if (OB_FAIL(process_hint())) {
      LOG_WARN("failed to process hint", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
    } else if (found_insert_status_ == NOT_FOUND_INSERT_TOKEN) {
      // 说明是insert token
      found_insert_status_ = FOUND_INSERT_TOKEN_ONCE;
    } else if (found_insert_status_ == FOUND_INSERT_TOKEN_ONCE) {
      found_insert_status_ = INVALID_TOKEN_STATUS;
    }
  }
  return ret;
}

bool ObFastParserBase::skip_space(ObRawSql &raw_sql)
{
  bool b_ret = false;
  int64_t space_len = 0;
  while (!raw_sql.search_end_ && IS_MULTI_SPACE_V2(raw_sql, raw_sql.cur_pos_, space_len)) {
    raw_sql.scan(space_len);
    b_ret = true;
  }
  return b_ret;
}

bool ObFastParserBase::is_split_character(ObRawSql &raw_sql)
{
  bool bret = false;
  char ch = raw_sql.char_at(raw_sql.cur_pos_);
  if (is_comma(ch) || is_left_parenthesis(ch) || is_right_parenthesis(ch)) {
    raw_sql.scan();
    bret = true;
  }
  return bret;
}

int ObFastParserBase::check_is_on_duplicate_key(ObRawSql &raw_sql, bool &is_on_duplicate_key)
{
  int ret = OB_SUCCESS;
  is_on_duplicate_key = false;
  if (0 == raw_sql.strncasecmp("duplicate", 9)) {
    raw_sql.scan(9);
    bool has_space = skip_space(raw_sql);
    if (has_space && 0 == raw_sql.strncasecmp("key", 3)) {
      raw_sql.scan(3);
      has_space = skip_space(raw_sql);
      if (has_space && 0 == raw_sql.strncasecmp("update", 6)) {
        raw_sql.scan(6);
        is_on_duplicate_key = true;
      }
    }
  }
  return ret;
}

int ObFastParserBase::get_one_insert_row_str(ObRawSql &raw_sql,
                                             ObString &str,
                                             bool &is_valid,
                                             int64_t &ins_params_count,
                                             bool &is_insert_up,
                                             int64_t &on_duplicate_params,
                                             int64_t &end_pos)
{
  int ret = OB_SUCCESS;
  int left_count = 0;
  int64_t start = 0;
  int64_t end = 0;
  bool need_break = false;
  int64_t curr_pos = 0;
  is_valid = false;
  is_insert_up = false;
  on_duplicate_params = 0;
  ins_params_count = 0;
  end_pos = 0;
  ROW_STATE row_state = START_STATE;

  while (!need_break && !raw_sql.is_search_end()) {
    skip_space(raw_sql);
    char ch = raw_sql.char_at(raw_sql.cur_pos_);
    curr_pos = raw_sql.cur_pos_;
    raw_sql.scan();
    switch (row_state) {
      case START_STATE:
        if ('(' == ch) {
          start = curr_pos;
          row_state = LEFT_PAR_STATE;
          left_count++;
        } else {
          row_state = UNEXPECTED_STATE;
        }
        break;
      case LEFT_PAR_STATE:
        if (')' == ch) {
          left_count--;
          if (0 == left_count) {
            row_state = PARS_MATCH;
            end = curr_pos;
          }
        } else if ('(' == ch) {
          left_count++;
        } else if ('?' == ch) {
          ins_params_count++;
        }
        break;
      case PARS_MATCH:
        if (',' == ch) {
          need_break = true;
        } else if (';' == ch) {
          raw_sql.search_end_ = true;
          need_break = true;
        } else if ('o' == ch || 'O' == ch) {
          bool is_duplicate = false;
          if (0 == raw_sql.strncasecmp("n", 1)) {
            raw_sql.scan();
            bool has_space = skip_space(raw_sql);
            bool is_duplicate = false;
            if (has_space) {
              check_is_on_duplicate_key(raw_sql, is_duplicate);
              if (is_duplicate) {
                row_state = ON_DUPLICATE_KEY;
              } else {
                row_state = UNEXPECTED_STATE;
              }
            }
          }
        } else {
          row_state = UNEXPECTED_STATE;
        }
        break;
      case ON_DUPLICATE_KEY:
        if ('?' == ch) {
          on_duplicate_params++;
        }
        break;
      case UNEXPECTED_STATE:
        need_break = true;
        break;
      default:
        break;
    }
  }

  if (PARS_MATCH == row_state || ON_DUPLICATE_KEY == row_state) {
    if (ON_DUPLICATE_KEY == row_state) {
      is_insert_up = true;
    }
    if (start > 0 && end > 0) {
      str.assign_ptr(raw_sql.ptr(start), (end - start) + 1);
      end_pos = end;
      is_valid = true;
    }
  }
  LOG_TRACE("after get one insert row str", K(raw_sql.cur_pos_), K(str), K(row_state),
      K(on_duplicate_params), K(is_valid), K(start), K(end));
  return ret;
}

inline int64_t ObFastParserBase::notascii_gb_char(const int64_t pos)
{
  int64_t idf_pos = -1;
  if (notascii(raw_sql_.char_at(pos))) {
    idf_pos = pos + 1;
  } else {
    idf_pos = is_gbk_char(pos);
  }
  return idf_pos;
}

inline int64_t ObFastParserBase::is_latin1_char(const int64_t pos)
{
  int64_t idf_pos = -1;
  if (is_latin1(raw_sql_.char_at(pos))) {
    idf_pos = pos + 1;
  }
  return idf_pos;
}

// ({U_2}{U}|{U_3}{U}{U}|{U_4}{U}{U}{U}
inline int64_t ObFastParserBase::is_utf8_char(const int64_t pos)
{
  int64_t idf_pos = -1;
  if (is_oracle_mode_ &&
     pos + 3 < raw_sql_.raw_sql_len_ &&
     (-1 != is_utf8_multi_byte_space(raw_sql_.raw_sql_, pos) ||
      -1 != is_utf8_multi_byte_comma(raw_sql_.raw_sql_, pos) ||
      -1 != is_utf8_multi_byte_left_parenthesis(raw_sql_.raw_sql_, pos) ||
      -1 != is_utf8_multi_byte_right_parenthesis(raw_sql_.raw_sql_, pos))) {
    raw_sql_.scan(3);
  } else {
    bool is_idf = true;
    if (is_u2(raw_sql_.char_at(pos))) {
      for (int64_t i = 1; i <= 1; i++) {
        if (!is_u(raw_sql_.char_at(pos + i))) {
          is_idf = false;
          break;
        }
      }
      if (is_idf) {
        idf_pos = pos + 2;
      }
    } else if (is_u3(raw_sql_.char_at(pos))) {
      for (int64_t i = 1; i <= 2; i++) {
        if (!is_u(raw_sql_.char_at(pos + i))) {
          is_idf = false;
          break;
        }
      }
      if (is_idf) {
        idf_pos = pos + 3;
      }
    } else if (is_u4(pos)) {
      for (int64_t i = 1; i <= 3; i++) {
        if (!is_u(raw_sql_.char_at(pos + i))) {
          is_idf = false;
          break;
        }
      }
      if (is_idf) {
        idf_pos = pos + 4;
      }
    }
  }
  return idf_pos;
}

// ([\\\xe3\][\\\x80\][\\\x80])
inline int64_t ObFastParserBase::is_utf8_multi_byte_space(const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xe3 == static_cast<uint8_t>(str[pos]) &&
      0x80 == static_cast<uint8_t>(str[pos + 1]) &&
      0x80 == static_cast<uint8_t>(str[pos + 2])) {
    idf_pos = pos + 3;
  }
  return idf_pos;
}

// [0-9]{n}
inline bool ObFastParserBase::is_n_continuous_digits(const char *str, const int64_t pos, const int64_t len, const int64_t n)
{
  bool res = false;
  if (pos + n < len) {
    int64_t i = 1;
    for ( ; i <= n; i++) {
      if (str[pos + i] < '0' || str[pos + i] > '9') {
        break;
      }
    }
    if (i > n) {
      res = true;
    }
  }
  return res;
}

// ([\\\xef\][\\\xbc\][\\\x8c])
inline int64_t ObFastParserBase::is_utf8_multi_byte_comma(const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xef == static_cast<uint8_t>(str[pos]) &&
      0xbc == static_cast<uint8_t>(str[pos + 1]) &&
      0x8c == static_cast<uint8_t>(str[pos + 2])) {
    idf_pos = pos + 3;
  }
  return idf_pos;
}

// ([\\\xef\][\\\xbc\][\\\x88])
inline int64_t ObFastParserBase::is_utf8_multi_byte_left_parenthesis(
                                 const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xef == static_cast<uint8_t>(str[pos]) &&
      0xbc == static_cast<uint8_t>(str[pos + 1]) &&
      0x88 == static_cast<uint8_t>(str[pos + 2])) {
    idf_pos = pos + 3;
  }
  return idf_pos;
}

// ([\\\xef\][\\\xbc\][\\\x89])
inline int64_t ObFastParserBase::is_utf8_multi_byte_right_parenthesis(
                                 const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xef == static_cast<uint8_t>(str[pos]) &&
      0xbc == static_cast<uint8_t>(str[pos + 1]) &&
      0x89 == static_cast<uint8_t>(str[pos + 2])) {
    idf_pos = pos + 3;
  }
  return idf_pos;
}

// ([\\\xa1][\\\xa1])
inline int64_t ObFastParserBase::is_gbk_multi_byte_space(const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xa1 == static_cast<uint8_t>(str[pos]) &&
      0xa1 == static_cast<uint8_t>(str[pos + 1])) {
    idf_pos = pos + 2;
  }
  return idf_pos;
}

// ([\\\xa3][\\\xac])
inline int64_t ObFastParserBase::is_gbk_multi_byte_comma(const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xa3 == static_cast<uint8_t>(str[pos]) &&
      0xac == static_cast<uint8_t>(str[pos + 1])) {
    idf_pos = pos + 2;
  }
  return idf_pos;
}

// ([\\\xa3][\\\xa8])
inline int64_t ObFastParserBase::is_gbk_multi_byte_left_parenthesis(
                                 const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xa3 == static_cast<uint8_t>(str[pos]) &&
      0xa8 == static_cast<uint8_t>(str[pos + 1])) {
    idf_pos = pos + 2;
  }
  return idf_pos;
}

// ([\\\xa3][\\\xa9])
inline int64_t ObFastParserBase::is_gbk_multi_byte_right_parenthesis(
                                 const char *str, const int64_t pos)
{
  int64_t idf_pos = -1;
  if (0xa3 == static_cast<uint8_t>(str[pos]) &&
      0xa9 == static_cast<uint8_t>(str[pos + 1])) {
    idf_pos = pos + 2;
  }
  return idf_pos;
}

// {GB_1}{GB_2}
inline int64_t ObFastParserBase::is_gbk_char(const int64_t pos)
{
  int64_t idf_pos = -1;
  if (is_oracle_mode_ &&
     pos + 2 < raw_sql_.raw_sql_len_ &&
     (-1 != is_gbk_multi_byte_space(raw_sql_.raw_sql_, pos) ||
      -1 != is_gbk_multi_byte_comma(raw_sql_.raw_sql_, pos) ||
      -1 != is_gbk_multi_byte_left_parenthesis(raw_sql_.raw_sql_, pos) ||
      -1 != is_gbk_multi_byte_right_parenthesis(raw_sql_.raw_sql_, pos))) {
    raw_sql_.scan(2);
  } else if (is_gb1(raw_sql_.char_at(pos)) && is_gb2(raw_sql_.char_at(pos + 1))) {
    idf_pos = pos + 2;
  }
  return idf_pos;
}

int64_t ObFastParserBase::is_whitespace(int64_t pos)
{
  int64_t ws_end_pos = -1;
  int64_t space_len = 0;
  char ch = raw_sql_.char_at(pos);
  if (IS_MULTI_SPACE(pos, space_len)) { // {space}+
    pos += space_len;
    while (IS_MULTI_SPACE(pos, space_len)) {
      pos += space_len;
    }
    ws_end_pos = pos;
  } else if ('#' == ch) { // #{non_newline}*
    ch = raw_sql_.char_at(++pos);
    while (is_non_newline(ch)) {
      ch = raw_sql_.char_at(++pos);
    }
    ws_end_pos = pos;
  } else if ('-' == ch) { // "--"{space}+{non_newline}*
    ch = raw_sql_.char_at(++pos);
    if ('-' == ch) {
      ch = raw_sql_.char_at(++pos);
      if (IS_MULTI_SPACE(pos, space_len)) {
        pos += space_len;
        while (IS_MULTI_SPACE(pos, space_len)) {
          pos += space_len;
        }
        ch = raw_sql_.char_at(pos);
        while (is_non_newline(ch)) {
          ch = raw_sql_.char_at(++pos);
        }
        ws_end_pos = pos;
      }
    }
  }
  return ws_end_pos;
}

// ({space}*(\/\*([^+*]|\*+[^*\/])*\*+\/{space}*)*(\/\*\+({space}*hint{space}+)?))
// eg: select /* comment */ /*+ hint */
int64_t ObFastParserBase::is_hint_begin(int64_t pos)
{
  int ret = OB_SUCCESS;
  int64_t space_len = 0;
  int64_t hint_begin_pos = -1;
  while (IS_MULTI_SPACE(pos, space_len)) {
    pos += space_len;
  }
  char ch = raw_sql_.char_at(pos);
  char next_ch = raw_sql_.char_at(++pos);
  while ('/' == ch && '*' == next_ch) {
    ch = raw_sql_.char_at(++pos);
    if ('+' == ch) { // hint
      while (IS_MULTI_SPACE(pos, space_len)) {
        pos += space_len;
      }
      if (CHECK_EQ_STRNCASECMP("hint", 4)) {
        pos += 4;
        while (IS_MULTI_SPACE(pos, space_len)) {
          pos += space_len;
        }
        hint_begin_pos = pos;
        break;
      }
      hint_begin_pos = pos;
      break;
    } else if (raw_sql_.is_search_end()) {
      // does not meet the requirements of hint
      break;
    }
    ch = raw_sql_.char_at(pos);
    next_ch = raw_sql_.char_at(++pos);
    // check and ignore comment
    while (ch != '*' && next_ch != '/' && !raw_sql_.is_search_end(pos)) {
      ch = raw_sql_.char_at(pos);
      next_ch = raw_sql_.char_at(++pos);
    }
    // "*/" appears, the end of the comment
    ch = raw_sql_.char_at(++pos);
    while (IS_MULTI_SPACE(pos, space_len)) {
      pos += space_len;
    }
    ch = raw_sql_.char_at(pos);
    next_ch = raw_sql_.char_at(++pos);
  }
  return hint_begin_pos;
}

int ObFastParserBase::process_hint()
{
  int ret = OB_SUCCESS;
  int64_t space_len = 0;
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  if (IS_MULTI_SPACE(raw_sql_.cur_pos_, space_len) || '/' == ch) {
    int64_t hint_begin_pos = is_hint_begin(raw_sql_.cur_pos_);
    if (-1 != hint_begin_pos) {
      // all the contents in the hint remain unchanged
      raw_sql_.cur_pos_ = hint_begin_pos;
      char next_ch = raw_sql_.peek();
      ch = raw_sql_.char_at(hint_begin_pos);
      while (('*' != ch || '/' != next_ch) && !raw_sql_.is_search_end()) {
        ch = raw_sql_.scan();
        next_ch = raw_sql_.peek();
      }
      if (!raw_sql_.is_search_end()) {
        cur_token_type_ = NORMAL_TOKEN;
        raw_sql_.scan(); // scan '\/'
        raw_sql_.scan(); // scan the first character of the new token
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
      }
    }
  }
  return ret;
}

inline void ObFastParserBase::reset_parser_node(ParseNode *node)
{
  node->type_ = T_INVALID;
  node->num_child_ = 0;
  node->param_num_ = 0;
  node->is_neg_ = 0;
  node->is_hidden_const_ = 0;
  node->is_tree_not_param_ = 0;
  node->length_semantics_  = 0;
  node->is_val_paramed_item_idx_ = 0;
  node->is_copy_raw_text_ = 0;
  node->is_column_varchar_ = 0;
  node->is_trans_from_minus_ = 0;
  node->is_assigned_from_child_ = 0;
  node->is_num_must_be_pos_ = 0;
  node->is_date_unit_ = 0;
  node->value_ = INT64_MAX;
  node->str_value_ = nullptr;
  node->str_len_ = 0;
  node->pl_str_off_ = 0;
  node->raw_text_ = nullptr;
  node->text_len_ = 0;
  node->pos_ = 0;
  node->children_ = nullptr;
  node->raw_param_idx_ = 0;
}

inline ParseNode *ObFastParserBase::new_node(char *&buf, ObItemType type)
{
  ParseNode *node = reinterpret_cast<ParseNode *>(buf);
  reset_parser_node(node);
  node->type_ = type;
  // buf points to the beginning of the next available memory
  buf += PARSER_NODE_SIZE;
  return node;
}

int64_t ObFastParserBase::get_question_mark(ObQuestionMarkCtx *ctx,
                                            void *malloc_pool,
                                            const char *name,
                                            const int64_t name_len,
                                            char *buf)
{
  int64_t idx = -1;
  if (OB_UNLIKELY(NULL == ctx || NULL == name)) {
    (void)fprintf(stderr, "ERROR question mark ctx or name is NULL\n");
  } else {
    if (NULL == ctx->name_ && 0 == ctx->capacity_) {
      ctx->capacity_ = MAX_QUESTION_MARK;
      ctx->name_ = (char **)parse_malloc(sizeof(char*) * MAX_QUESTION_MARK, malloc_pool);
    }
    if (ctx->name_ != NULL) {
      bool valid_name = true;
      for (int64_t i = 0; valid_name && -1 == idx && i < ctx->count_; ++i) {
        if (NULL == ctx->name_[i]) {
          (void)fprintf(stderr, "ERROR name_ in question mark ctx is null\n");
          valid_name = false;
        } else if (0 == STRNCASECMP(ctx->name_[i], name, name_len)) {
          idx = i;
        }
      }
      if (-1 == idx && valid_name) {
        if (ctx->count_ >= ctx->capacity_) {
          void *buf = parse_malloc(sizeof(char*) * (ctx->capacity_ * 2), malloc_pool);
          if (OB_UNLIKELY(NULL == buf)) {
            ctx->name_ = NULL;
            (void)printf("ERROR malloc memory failed\n");
          } else {
            MEMCPY(buf, ctx->name_, sizeof(char*) * ctx->capacity_);
            ctx->capacity_ *= 2;
            ctx->name_ = (char **)buf;
          }
        }
        if (ctx->name_ != NULL) {
          ctx->name_[ctx->count_] = parse_strndup(name, name_len, buf);
          idx = ctx->count_++;
        }
      }
    } else {
      (void)fprintf(stderr, "ERROR question mark name buffer is null\n");
    }
  }
  return idx;
}

int64_t ObFastParserBase::get_question_mark_by_defined_name(QuestionMarkDefNameCtx *ctx,
                                                            const char *name,
                                                            const int64_t name_len)
{
  int64_t idx = -1;
  if (OB_UNLIKELY(NULL == ctx || NULL == name)) {
    (void)fprintf(stderr, "ERROR question mark ctx or name is NULL\n");
  } else if (ctx->name_ != NULL) {
    for (int64_t i = 0; -1 == idx && i < ctx->count_; ++i) {
      if (NULL == ctx->name_[i]) {
        (void)fprintf(stderr, "ERROR name_ in question mark ctx is null\n");
      } else if (0 == STRNCASECMP(ctx->name_[i], name, name_len)) {
        idx = i;
      }
    }
  }
  return idx;
}

inline char* ObFastParserBase::parse_strndup(const char *str, size_t nbyte, char *buf)
{
  MEMMOVE(buf, str, nbyte);
  buf[nbyte] = '\0';
  return buf;
}

char *ObFastParserBase::parse_strdup_with_replace_multi_byte_char(
	                      const char *str, const size_t dup_len, char *out_str, int64_t &out_len)
{
  out_len = 0;
  int64_t len = 0;
  for (int64_t i = 0; i < dup_len; ++i) {
    if (CHARSET_UTF8MB4 == charset_type_ || CHARSET_UTF16 == charset_type_) {
     if (i + 2 < dup_len) {
        if (str[i] == (char)0xe3 && str[i+1] == (char)0x80 && str[i+2] == (char)0x80) {
          //utf8 multi byte space
          out_str[len++] = ' ';
          i = i + 2;
        } else if (str[i] == (char)0xef && str[i+1] == (char)0xbc && str[i+2] == (char)0x88) {
          //utf8 multi byte left parenthesis
          out_str[len++] = '(';
          i = i + 2;
        } else if (str[i] == (char)0xef && str[i+1] == (char)0xbc && str[i+2] == (char)0x89) {
          //utf8 multi byte right parenthesis
          out_str[len++] = ')';
          i = i + 2;
        } else {
          out_str[len++] = str[i];
        }
      } else {
        out_str[len++] = str[i];
      }
    } else if (ObCharset::is_gb_charset(charset_type_)) {
      if (i + 1 < dup_len) {
        if (str[i] == (char)0xa1 && str[i+1] == (char)0xa1) {//gbk multi byte space
          out_str[len++] = ' ';
          ++i;
        } else if (str[i] == (char)0xa3 && str[i+1] == (char)0xa8) {
          //gbk multi byte left parenthesis
          out_str[len++] = '(';
          ++i;
        } else if (str[i] == (char)0xa3 && str[i+1] == (char)0xa9) {
          //gbk multi byte right parenthesis
          out_str[len++] = ')';
          ++i;
        } else {
          out_str[len++] = str[i];
        }
      } else {
        out_str[len++] = str[i];
      }
    } else {
      out_str[len++] = str[i];
    }
  }
  if (len > 0) {
    out_str[len] = '\0';
    out_len = len;
  }
  return out_str;
}

inline void ObFastParserBase::lex_store_param(ParseNode *node, char *buf)
{
  ParamList *param = reinterpret_cast<ParamList *>(buf);
  param->node_ = node;
  param->next_ = NULL;
  if (nullptr == param_node_list_) {
    param_node_list_ = param;
  } else {
    tail_param_node_->next_ = param;
  }
  tail_param_node_ = param;
  param_num_++;
}

/**
 * The hexadecimal number in mysql mode has the following two representations:
 * x'([0-9A-F])*' or 0x([0-9A-F])+
 * @param [in] : when is_quote is true, it means the first one. when "\`" does not appear
 * as a pair, only an 'x' is reserved
 */
int ObFastParserBase::process_hex_number(bool is_quote)
{
  int ret = OB_SUCCESS;
  int64_t pos = raw_sql_.cur_pos_;
  char next_ch = raw_sql_.scan();
  if (is_quote) {
    // X'([0-9A-F])*'
    while (is_hex(next_ch)) {
      next_ch = raw_sql_.scan();
    }
    if ('\'' == next_ch) {
      cur_token_type_ = PARAM_TOKEN;
      next_ch = raw_sql_.scan();
    } else if (raw_sql_.is_search_end()) {
      // missing'\'', all positions starting from quote will be ignored
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
    } else {
      // it is possible that the next token is a string and needs to fall back to
      // the position of quote
      raw_sql_.cur_pos_ = pos;
      cur_token_type_ = NORMAL_TOKEN;
    }
  } else {
    // 0X([0-9A-F])+
    while (is_hex(next_ch)) {
      next_ch = raw_sql_.scan();
    }
    int64_t next_idf_pos = is_first_identifier_flags(raw_sql_.cur_pos_);
    if (-1 != next_idf_pos) {
      // it is possible that the next token is a string and needs to fall back to
      // the position of quote
      raw_sql_.cur_pos_ = pos;
      cur_token_type_ = NORMAL_TOKEN;
    } else {
      cur_token_type_ = PARAM_TOKEN;
    }
  }
  if (OB_SUCC(ret) && PARAM_TOKEN == cur_token_type_) {
    char *buf = nullptr;
    int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
    int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
    int64_t str_len = text_len - 2;
    int64_t dst_str_len = 0;
    if ('\'' == raw_sql_.char_at(raw_sql_.cur_pos_ - 1)) {
      // Values written using X'val' notation
      --str_len;
      if (0 != str_len % 2) {
        /*
         * https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
         * Values written using X'val' notation must contain an even number of digits or a syntax error occurs. To correct the problem, pad the value with a leading zero.
         * Values written using 0xval notation that contain an odd number of digits are treated as having an extra leading 0. For example, 0xaaa is interpreted as 0x0aaa.
         */
        LOG_WARN("parser syntax error",
                 K(ret), K(str_len), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
        return OB_ERR_PARSER_SYNTAX;
      }
    } else {
      // Values written using 0xval notation NOTE: 0Xval (use upper case 'X') notation is illegal in MySQL
      if (!is_oracle_mode_ && raw_sql_.char_at(cur_token_begin_pos_ + 1) == 'X') {
        LOG_WARN("parser syntax error", K(ret));
        return OB_ERR_PARSER_SYNTAX;
      }
    }
    if (str_len > 0) {
      dst_str_len = ob_parse_binary_len(str_len);
      need_mem_size += dst_str_len;
    }
    // allocate all the memory needed at once
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
    } else {
      ParseNode *node = new_node(buf, T_HEX_STRING);
      node->text_len_ = text_len;
      if (str_len > 0) {
        // skip x' or 0x
        ob_parse_binary(raw_sql_.ptr(cur_token_begin_pos_ + 2), str_len, buf);
        node->str_value_ = buf;
        node->str_len_ = dst_str_len;
        // buf points to the beginning of the next available memory
        buf += dst_str_len;
      } else {
        node->str_value_ = NULL;
        node->str_len_ = 0;
      }
      node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
      node->raw_sql_offset_ = cur_token_begin_pos_;
      node->is_copy_raw_text_ = 1;
      lex_store_param(node, buf);
    }
  }
  return ret;
}

/**
 * The binary in mysql mode has the following two representations:
 * b'([01])*' or 0b([01])+
 * @param [in] : when is_quote is true, it means the first one. when "\`" does not appear
 * as a pair, only an 'b' is reserved
 */
int ObFastParserBase::process_binary(bool is_quote)
{
  int ret = OB_SUCCESS;
  int64_t pos = raw_sql_.cur_pos_;
  char ch = raw_sql_.scan();
  if (is_quote) {
    // B'([01])*'
    while (is_binary(ch)) {
      ch = raw_sql_.scan();
    }
    if ('\'' == ch) {
      cur_token_type_ = PARAM_TOKEN;
      ch = raw_sql_.scan();
    } else if (raw_sql_.is_search_end()) {
      // missing'\'', all positions starting from quote will be ignored
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
    } else {
      // it is possible that the next token is a string and needs to fall back to
      // the position of quote
      raw_sql_.cur_pos_ = pos;
      cur_token_type_ = NORMAL_TOKEN;
    }
  } else {
    // 0B([01])+
    cur_token_type_ = PARAM_TOKEN;
    while (is_binary(ch)) {
      ch = raw_sql_.scan();
    }
  }
  if (OB_SUCC(ret) && PARAM_TOKEN == cur_token_type_) {
    char *buf = nullptr;
    int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
    int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
    int64_t str_len = text_len - 2;
    int64_t dst_str_len = 0;
    if ('\'' == raw_sql_.char_at(raw_sql_.cur_pos_ - 1)) {
      --str_len;
    } else {
      // Values written using 0bval notation NOTE: 0Bval (use upper case 'B') notation is illegal in MySQL
      if (!is_oracle_mode_ && raw_sql_.char_at(cur_token_begin_pos_ + 1) == 'B') {
        LOG_WARN("parser syntax error", K(ret));
        return OB_ERR_PARSER_SYNTAX;
      }
    }
    if (str_len > 0) {
      dst_str_len = ob_parse_bit_string_len(str_len);
      need_mem_size += dst_str_len;
    }
    // allocate all the memory needed at once
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
    } else {
      ParseNode *node = new_node(buf, T_HEX_STRING);
      node->text_len_ = text_len;
      if (str_len > 0) {
        // skip B' or 0B
        ob_parse_bit_string(raw_sql_.ptr(cur_token_begin_pos_ + 2), str_len, buf);
        node->str_value_ = buf;
        node->str_len_ = dst_str_len;
        // buf points to the beginning of the next available memory
        buf += dst_str_len;
      } else {
        node->str_value_ = NULL;
        node->str_len_ = 0;
      }
      node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
      node->raw_sql_offset_ = cur_token_begin_pos_;
      node->is_copy_raw_text_ = 1;
      lex_store_param(node, buf);
    }
  }
  return ret;
}

inline int64_t ObFastParserBase::is_first_identifier_flags(const int64_t pos)
{
  int64_t idf_pos = -1;
  char ch = raw_sql_.char_at(pos);
  if (is_first_identifier_char(ch)) {
    idf_pos = pos + 1;
  } else if (is_space(ch) || is_comma(ch) || is_left_parenthesis(ch) || is_right_parenthesis(ch)) {
    // Most of the time, if it is not an identifier character, it maybe a space,
    // comma, opening parenthesis, or closing parenthesis. This judgment logic is
    // added here to avoid the next judgment whether it is utf8 char or gbk char
  } else if (!is_oracle_mode_) {
    idf_pos = notascii_gb_char(pos);
  } else if (CHARSET_UTF8MB4 == charset_type_ || CHARSET_UTF16 == charset_type_) {
    idf_pos = is_utf8_char(pos);
  } else if (ObCharset::is_gb_charset(charset_type_)) {
    idf_pos = is_gbk_char(pos);
  } else if (CHARSET_LATIN1 == charset_type_) {
    idf_pos = is_latin1_char(pos);
  }
  return idf_pos;
}

// eg: Timestamp '2006-07-23 20:33:28.048719'
// eg: DATE '2010-01-01'
// eg: TIME '30 24:00:00'
int ObFastParserBase::process_time_relate_type(bool &need_process_ws, ObItemType type)
{
  int ret = OB_SUCCESS;
  int64_t ws_end_pos = is_whitespace(raw_sql_.cur_pos_);
  if (-1 != ws_end_pos) {
    // deal with {whitespace}?
    need_process_ws = false;
    raw_sql_.cur_pos_ = ws_end_pos;
  }
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  int64_t idf_end_pos = raw_sql_.cur_pos_;
  // deal with the'[^']*' part, the part after quote may be parameterized or ignored
  if ('\'' == ch || (!is_oracle_mode_ && '\"' == ch)) {
    if (T_TIME == type || T_DATE == type || T_TIMESTAMP_TZ == type ||
        T_DATETIME == type || T_TIMESTAMP == type) {
      OZ (process_date_related_type(ch, type));
    } else if (is_oracle_mode_) {
      raw_sql_.scan();
      OZ (process_interval());
    }
    if (OB_SUCC(ret) && !is_valid_token()) {
      raw_sql_.cur_pos_ = idf_end_pos;
      copy_end_pos_ = idf_end_pos;
    }
  }
  return ret;
}

int ObFastParserBase::process_date_related_type(const char quote, ObItemType item_type)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t quote_begin_pos = raw_sql_.cur_pos_;
  char ch = raw_sql_.scan();
  while (quote != ch && !raw_sql_.is_search_end()) {
    ch = raw_sql_.scan();
  }
  if (raw_sql_.is_search_end()) {
    // not match another quote
    copy_end_pos_ = quote_begin_pos;
    cur_token_type_ = IGNORE_TOKEN;
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
  } else {
    raw_sql_.scan();
    cur_token_type_ = PARAM_TOKEN;
    int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
    int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
    need_mem_size += text_len + 1; // '\0'
    int64_t str_len = raw_sql_.cur_pos_ - quote_begin_pos - 2;
    // allocate all the memory needed at once
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
    } else {
      ParseNode *node = new_node(buf, item_type);
      node->str_len_ = str_len;
      node->raw_text_ = parse_strdup_with_replace_multi_byte_char(
      raw_sql_.ptr(cur_token_begin_pos_), text_len, buf, node->text_len_);
      // buf points to the beginning of the next available memory
      buf += text_len + 1;
      node->str_value_ = raw_sql_.ptr(quote_begin_pos + 1);
      node->is_copy_raw_text_ = 1;
      node->raw_sql_offset_ = cur_token_begin_pos_;
      lex_store_param(node, buf);
    }
  }
  return ret;
}

int ObFastParserBase::add_bool_type_node(bool is_true)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
  int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
  // allocate all the memory needed at once
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
  } else {
    ParseNode *node = new_node(buf, T_BOOL);
    node->text_len_ = text_len;
    node->value_ = is_true ? 1 : 0;
    node->raw_sql_offset_ = cur_token_begin_pos_;
    node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
    lex_store_param(node, buf);
  }
  return ret;
}

int ObFastParserBase::add_null_type_node()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
  int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
  // allocate all the memory needed at once
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
  } else {
    ParseNode *node = new_node(buf, T_NULL);
    node->text_len_ = text_len;
    node->raw_sql_offset_ = cur_token_begin_pos_;
    node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
    lex_store_param(node, buf);
  }
  return ret;
}

int ObFastParserBase::add_nowait_type_node()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
  int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
  int64_t str_len = 1;
  need_mem_size += str_len + 1; // '\0'
  // allocate all the memory needed at once
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
  } else {
    ParseNode *node = new_node(buf, T_INT);
    node->text_len_ = text_len;
    node->str_len_ = str_len;
    node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
    node->str_value_ = parse_strndup("0", str_len, buf);
    // buf points to the beginning of the next available memory
    buf += str_len + 1;
    node->value_ = 0;
    node->raw_sql_offset_ = cur_token_begin_pos_;
    lex_store_param(node, buf);
  }
  return ret;
}

void ObFastParserBase::process_escape_string(char *str_buf, int64_t &str_buf_len)
{
  // read the next character after the escape character
  char ch = raw_sql_.scan();
  if (!raw_sql_.is_search_end()) {
    switch (ch) {
      case 'n':
        str_buf[str_buf_len++] = '\n';
        break;
      case 't':
        str_buf[str_buf_len++] = '\t';
        break;
      case 'r':
        str_buf[str_buf_len++] = '\r';
        break;
      case 'b':
        str_buf[str_buf_len++] = '\b';
        break;
      case '0':
        str_buf[str_buf_len++] = '\0';
        break;
      case 'Z': // ctrl + Z
        str_buf[str_buf_len++] = '\032';
        break;
      case '_':
      case '%':
        str_buf[str_buf_len++] = '\\';
        str_buf[str_buf_len++] = ch;
        break;
      default:
        str_buf[str_buf_len++] = ch;
        break;
    }
  }
}

int ObFastParserBase::process_question_mark()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  raw_sql_.scan();
  cur_token_type_ = PARAM_TOKEN;
  int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
  int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
  if (question_mark_ctx_.by_name_) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
    // allocate all the memory needed at once
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
  } else {
    ParseNode *node = new_node(buf, T_QUESTIONMARK);
    node->value_ = question_mark_ctx_.count_++;
    question_mark_ctx_.by_ordinal_ = true;
    node->text_len_ = text_len;
    node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
    node->raw_sql_offset_ = cur_token_begin_pos_;
    lex_store_param(node, buf);
  }
  return ret;
}

int ObFastParserBase::process_ps_statement()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  cur_token_type_ = PARAM_TOKEN;
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  bool is_num = is_digit(ch) ? true : false;
  if (is_num) { // ":"{int_num}
    ch = raw_sql_.scan();
    while (is_digit(ch)) {
      ch = raw_sql_.scan();
    }
  } else {
    int64_t next_idf_pos = raw_sql_.cur_pos_;
    while (-1 != (next_idf_pos = is_identifier_flags(next_idf_pos))) {
      raw_sql_.cur_pos_ = next_idf_pos;
    }
  }
  int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
  int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
  need_mem_size += (text_len + 1);
  if (question_mark_ctx_.by_ordinal_) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
    // allocate all the memory needed at once
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
  } else {
    ParseNode *node = new_node(buf, T_QUESTIONMARK);
    node->text_len_ = text_len;
    node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
    if (is_num) {
      if (is_udr_mode_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "question mark by number");
        LOG_WARN("question mark by number not supported", K(ret));
      } else {
        node->value_ = strtoll(&node->raw_text_[1], NULL, 10);
      }
    } else {
      int64_t ind = -1;
      if (is_udr_mode_ && nullptr != def_name_ctx_) {
        ind = get_question_mark_by_defined_name(def_name_ctx_, node->raw_text_, text_len);
      } else {
        ind = get_question_mark(&question_mark_ctx_, &allocator_,
                                node->raw_text_, text_len, buf);
      }
      node->value_ = ind;
      // buf points to the beginning of the next available memory
      buf += text_len + 1;
      question_mark_ctx_.by_name_ = true;
    }
    if (OB_SUCC(ret)) {
      if (node->value_ < 0) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
      } else {
        node->raw_sql_offset_ = cur_token_begin_pos_;
        lex_store_param(node, buf);
      }
    }
  }
  return ret;
}

// Used to process '`' and keep all characters before the next '`'
int ObFastParserBase::process_backtick()
{
  int ret = OB_SUCCESS;
  cur_token_type_ = NORMAL_TOKEN;
  char ch = raw_sql_.scan();
  while (!raw_sql_.is_search_end() && '`' != ch) {
    ch = raw_sql_.scan();
  }
  if ('`' != ch) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
  } else {
    // read an extra character for the next parsing
    raw_sql_.scan();
  }
  return ret;
}

// Used to process '\"' and keep all characters before the next '\"'
int ObFastParserBase::process_double_quote()
{
  int ret = OB_SUCCESS;
  char ch = raw_sql_.scan();
  cur_token_type_ = NORMAL_TOKEN;
  while (!raw_sql_.is_search_end() && '\"' != ch) {
    ch = raw_sql_.scan();
  }
  if ('\"' != ch) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
  } else {
    ch = raw_sql_.scan();
  }
  return ret;
}

// Until "*/" appears, all characters before it should be ignored
int ObFastParserBase::process_comment_content(bool is_mysql_comment)
{
  int ret = OB_SUCCESS;
  // if is in /*! xxx */ the token type should be normal
  cur_token_type_ = is_mysql_comment ? NORMAL_TOKEN : IGNORE_TOKEN;
  bool is_match = false;
  char ch = raw_sql_.scan();
  while (!raw_sql_.is_search_end()) {
    if (is_mysql_comment && '/' == ch && '*' == raw_sql_.peek()) {
      raw_sql_.scan();
      if (OB_FAIL(process_comment_content())) {
        LOG_WARN("failed to process comment content", K(ret));
      } else {
        cur_token_type_ = NORMAL_TOKEN;
      }
    } else if ('*' == ch && '/' == raw_sql_.peek()) {
      // scan '\/'
      raw_sql_.scan();
      is_match = true;
      break;;
    } else {
      ch = raw_sql_.scan();
    }
  }
  if (!is_match) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
  } else {
    // read an extra character for the next parsing
    raw_sql_.scan();
  }
  return ret;
}

/**
 * Used to check the escape character encountered in the string
 * Character sets marked with escape_with_backslash_is_dangerous, such as big5, cp932, gbk, sjis
 * The escape character (0x5C) may be part of a multi-byte character and requires special judgment
 */
inline void ObFastParserBase::check_real_escape(bool &is_real_escape)
{
  if (OB_NOT_NULL(charset_info_) && charset_info_->escape_with_backslash_is_dangerous) {
    char *cur_pos = tmp_buf_ + tmp_buf_len_;
    char *last_check_pos = tmp_buf_ + last_escape_check_pos_;
    int error = 0;
    int expected_well_formed_len = cur_pos - last_check_pos;

    while (last_check_pos < cur_pos) {
      size_t real_well_formed_len = charset_info_->cset->well_formed_len(
                  charset_info_, last_check_pos, cur_pos, UINT64_MAX, &error);
      last_check_pos += (real_well_formed_len + ((error != 0) ? 1 : 0));
    }

    if (error != 0) { //the final well-formed result
      *cur_pos = '\\';
      if (charset_info_->cset->ismbchar(charset_info_, cur_pos - 1, cur_pos + 1)) {
        is_real_escape = false;
      }
    }
  }
}

// [A-Za-z0-9_]
inline void ObFastParserBase::process_system_variable(bool is_contain_quote)
{
  cur_token_type_ = NORMAL_TOKEN;
  raw_sql_.scan();
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  if (is_contain_quote) {
    while (is_sys_var_char(ch) || ch == '`') {
      ch = raw_sql_.scan();
    }
  } else {
    while (is_sys_var_char(ch)) {
      ch = raw_sql_.scan();
    }
  }
}

void ObFastParserBase::parse_integer(ParseNode *node)
{
  int err_no = 0;
  if ('-' == node->str_value_[0]) {
    int pos = 1;
    int64_t space_len = 0;
    char *copied_str = const_cast<char *>(node->str_value_);
    while (pos < node->str_len_ && is_multi_byte_space(copied_str, node->str_len_, pos, space_len)) {
      pos += space_len;
    }
    copied_str[--pos] = '-';
    node->value_ = ob_strntoll(copied_str + pos, node->str_len_ - pos, 10, NULL, &err_no);
    if (ERANGE == err_no) {
      node->type_ = T_NUMBER;
    }
  } else {
    uint64_t value = 0;
    if (is_oracle_mode_) {
      value = ob_strntoll(node->str_value_, node->str_len_, 10, NULL, &err_no);
    } else {
      value = ob_strntoull(node->str_value_, node->str_len_, 10, NULL, &err_no);
    }
    node->value_ = value;
    if (ERANGE == err_no) {
      node->type_ = T_NUMBER;
    } else if (!is_oracle_mode_ && value > INT64_MAX) {
      node->type_ = T_UINT64;
    }
  }
}

inline void ObFastParserBase::process_user_variable(bool is_contain_quote)
{
  cur_token_type_ = NORMAL_TOKEN;
  raw_sql_.scan();
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  if (is_contain_quote) { // @[`'\"][`'\"A-Za-z0-9_\.$/%]*
    while (is_user_var_char(ch)) {
      ch = raw_sql_.scan();
    }
  } else { // [A-Za-z0-9_\.$]*
    while (is_user_var_char_without_quota(ch)) {
      ch = raw_sql_.scan();
    }
  }
}

int ObFastParserBase::process_negative()
{
  int ret = OB_SUCCESS;
  int64_t space_len = 0;
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  while (IS_MULTI_SPACE(raw_sql_.cur_pos_, space_len)) {
    ch = raw_sql_.scan(space_len);
  }
  char next_char = raw_sql_.peek();
  if (is_digit(ch)) {
    if (!is_oracle_mode_ &&
       ('x' == next_char || 'X' == next_char || 'b' == next_char || 'B' == next_char)) {
      cur_token_type_ = NORMAL_TOKEN;
    } else if (OB_FAIL(process_number(true/*has_minus*/))) {
      LOG_WARN("failed to handle number", K(ret));
    }
  } else if ('.' == ch && isdigit(next_char)) {
    if (OB_FAIL(process_number(true/*has_minus*/))) {
      LOG_WARN("failed to handle number", K(ret));
    }
  } else {
    cur_token_type_ = NORMAL_TOKEN;
  }
  return ret;
}

inline void ObFastParserBase::process_token()
{
  if (NORMAL_TOKEN == cur_token_type_) {
    copy_end_pos_ = raw_sql_.cur_pos_;
  } else {
    if (copy_end_pos_ > copy_begin_pos_) {
      append_no_param_sql();
    }
    if (PARAM_TOKEN == cur_token_type_) {
      // add'?' to the result string, the parameter part has been saved
      tail_param_node_->node_->pos_ = no_param_sql_len_;
      no_param_sql_[no_param_sql_len_++] = '?';
      no_param_sql_[no_param_sql_len_] = '\0';
    }
    // update the position of copy_begin_pos_ and copy_end_pos_
    copy_begin_pos_ = raw_sql_.cur_pos_;
    copy_end_pos_ = raw_sql_.cur_pos_;
  }
}

int ObFastParserBase::process_identifier_begin_with_l(bool &need_process_ws)
{
  int ret = OB_SUCCESS;
  int64_t space_len = 0;
  if (CHECK_EQ_STRNCASECMP("oad", 3)) {
    raw_sql_.scan(3);
    if (IS_MULTI_SPACE(raw_sql_.cur_pos_, space_len)) {
      need_process_ws = false;
      raw_sql_.scan(space_len);
      while (IS_MULTI_SPACE(raw_sql_.cur_pos_, space_len)) {
        raw_sql_.scan(space_len);
      }
      if (CHECK_EQ_STRNCASECMP("data", 4)) {
        raw_sql_.scan(4);
        need_process_ws = false;
        OZ (process_hint());
      }
    }
  }
  return ret;
}

int ObFastParserBase::process_identifier_begin_with_t(bool &need_process_ws)
{
  int ret = OB_SUCCESS;
  if (CHECK_EQ_STRNCASECMP("rue", 3)) {
    raw_sql_.scan(3);
    if (-1 == is_identifier_flags(raw_sql_.cur_pos_)) {
      cur_token_type_ = PARAM_TOKEN;
      OZ (add_bool_type_node(true/*is_true*/));
    }
  } else {
    ObItemType item_type = T_INVALID;
    if (CHECK_EQ_STRNCASECMP("imestamp", 8)) {
      raw_sql_.scan(8);
      item_type = is_oracle_mode_ ? T_TIMESTAMP_TZ : T_TIMESTAMP;
    } else if (!is_oracle_mode_ && CHECK_EQ_STRNCASECMP("ime", 3)) {
      raw_sql_.scan(3);
      item_type = T_TIME;
    }
    // deal with Timestamp{whitespace}?'[^']*'
    if (T_INVALID != item_type) {
      OZ (process_time_relate_type(need_process_ws, item_type));
    }
  }
  return ret;
}

int ObFastParserBase::process_number(bool has_minus)
{
#define ADD_PARAMETERIC_NODE(type) \
  do { \
    is_double = true; \
    need_parameterized = true; \
    param_type = type; \
    cur_token_type_ = PARAM_TOKEN; \
  } while (0)

#define CHECK_AND_PROCESS_NUMBER(default_type) \
  do {  \
    if (is_oracle_mode_) { \
      if ('D' == ch || 'd' == ch) { \
        raw_sql_.scan(); \
        ADD_PARAMETERIC_NODE(T_DOUBLE); \
      } else if ('f' == ch || 'F' == ch) { \
        raw_sql_.scan(); \
        ADD_PARAMETERIC_NODE(T_FLOAT); \
      } else { \
        ADD_PARAMETERIC_NODE(default_type); \
      } \
    } else { \
      int64_t next_idf_pos = is_identifier_flags(raw_sql_.cur_pos_); \
      if (-1 != next_idf_pos && !has_flag_after_euler && !has_dot) { \
        if (has_minus) { \
          copy_end_pos_ = num_begin_pos; \
          cur_token_begin_pos_ = num_begin_pos; \
        } \
        raw_sql_.cur_pos_ = next_idf_pos; \
        if (OB_LIKELY(process_idf_func_ != nullptr)) { \
          OZ ((this->*process_idf_func_)(true)); \
        } \
      } else { \
        ADD_PARAMETERIC_NODE(default_type); \
      } \
    } \
  } while (0)
  

  int ret = OB_SUCCESS;
  int64_t num_begin_pos = raw_sql_.cur_pos_;
  bool is_digit_first = false;
  bool need_parameterized = false;
  ObItemType param_type = T_INVALID;
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  while (is_digit(ch)) {
    is_digit_first = true;
    ch = raw_sql_.scan();
  }
  bool is_double = false;
  bool has_dot = false;
  if ('.' == ch) {
    is_double = true;
    has_dot = true;
    ch = raw_sql_.scan();
    while (is_digit(ch)) {
      ch = raw_sql_.scan();
    }
  }
  // If there is no digit, the content after the character 'e' does not need to be matched,
  // it is not part of the number
  bool has_digit_after_euler = false;
  bool has_flag_after_euler = false;
  if ('e' == ch || 'E' == ch) {
    ch = raw_sql_.scan();
    if ('+' == ch || '-' == ch) {
      has_flag_after_euler = true;
      ch = raw_sql_.scan();
    }
    while (is_digit(ch)) {
      has_digit_after_euler = true;
      ch = raw_sql_.scan();
    }
    // no digit after euler
    if (!has_digit_after_euler) {
      if (is_digit_first) {
        // consider at this time: digit first identifier or double two cases
        if (!has_dot) {
          // if there is a minus sign after euler.
          // eg: 011e-
          if (has_flag_after_euler) {
            raw_sql_.reverse_scan();
          }
          if (!is_oracle_mode_) {
            if (has_minus) {
              copy_end_pos_ = num_begin_pos;
              cur_token_begin_pos_ = num_begin_pos;
            }
            if (OB_LIKELY(process_idf_func_ != nullptr)) {
              OZ ((this->*process_idf_func_)(true));
            }
          } else {
            // after reverse scan, cur_ch == 'e'
            raw_sql_.reverse_scan();
            ADD_PARAMETERIC_NODE(T_INT);
          }
        } else {
          // If has_dot is true, it has a "." in front of it. It belongs to the double type, and if
          // there is a +/- sign behind it, it needs to be reverse scan
          if (has_flag_after_euler) {
            raw_sql_.reverse_scan();
          }
          // after reverse scan, cur_ch == 'e'
          raw_sql_.reverse_scan();
          ADD_PARAMETERIC_NODE(T_NUMBER);
        }
      } else {
        // if is_digit_first is false, it must be a double type starting with .[0-9]
        if (has_flag_after_euler) {
          raw_sql_.reverse_scan();
        }
        // after reverse scan, cur_ch == 'e'
        raw_sql_.reverse_scan();
        ADD_PARAMETERIC_NODE(T_NUMBER);
      }
    } else { // has number after euler
      if (is_oracle_mode_) {
        CHECK_AND_PROCESS_NUMBER(T_NUMBER);
      } else {
        CHECK_AND_PROCESS_NUMBER(T_DOUBLE);
      }
    }
  } else {
    // not 'e' end, eg: 1.a, 1.1a, .1a
    if (has_dot) {
      CHECK_AND_PROCESS_NUMBER(T_NUMBER);
    } else {
      CHECK_AND_PROCESS_NUMBER(T_INT);
    }
  }
  if (OB_SUCC(ret) && need_parameterized) {
    char *buf = nullptr;
    int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
    int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
    int64_t str_len = text_len;
    if (!is_double) {
      param_type = T_INT;
    }
    // The reason for doing this here is to avoid applying for memory when judging its type
    // when the number is a negative number. see the implementation of parse_integer for details
    if (T_INT == param_type && has_minus) {
      need_mem_size += str_len + 1; // '\0'
    }
    // allocate all the memory needed at once
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
    } else {
      ParseNode *node = new_node(buf, param_type);
      node->text_len_ = text_len;
      node->str_len_ = text_len;
      node->raw_sql_offset_ = cur_token_begin_pos_;
      node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
      if (T_INT == param_type && has_minus) {
        node->str_value_ = parse_strndup(raw_sql_.ptr(cur_token_begin_pos_), str_len, buf);
      } else {
        node->str_value_ = raw_sql_.ptr(cur_token_begin_pos_);
      }
      if (T_INT == param_type) {
        parse_integer(node);
      }
      node->str_value_ = raw_sql_.ptr(cur_token_begin_pos_);
      lex_store_param(node, buf);
    }
  }
  return ret;
}

inline void ObFastParserBase::remove_multi_stmt_end_space()
{
  // insert into t values (1);
  //                          |
  //                       cur_pos
  int end_pos = raw_sql_.cur_pos_ - 2;
  for (; end_pos >= 0 && is_space(raw_sql_.char_at(end_pos)); --end_pos)
    ;
  copy_end_pos_ = end_pos + 1;
  if (copy_end_pos_ > copy_begin_pos_) {
    append_no_param_sql();
  }
  copy_begin_pos_ = raw_sql_.cur_pos_;
  copy_end_pos_ = raw_sql_.cur_pos_;
}

inline void ObFastParserBase::append_no_param_sql()
{
  MEMCPY(no_param_sql_ + no_param_sql_len_, raw_sql_.ptr(copy_begin_pos_),
  copy_end_pos_ - copy_begin_pos_);
  no_param_sql_len_ += copy_end_pos_ - copy_begin_pos_;
  no_param_sql_[no_param_sql_len_] = '\0';
}

int ObFastParserMysql::process_zero_identifier()
{
  int ret = OB_SUCCESS;
  char next_ch = raw_sql_.peek();
  if ('x' == next_ch || 'X' == next_ch) {
    raw_sql_.scan();
    next_ch = raw_sql_.peek();
    if (is_hex(next_ch)) {
      raw_sql_.scan();
      OZ (process_hex_number(false/*is_quote*/));
    } else {
      if (OB_LIKELY(process_idf_func_ != nullptr)) {
        OZ ((this->*process_idf_func_)(true));
      }
    }
  } else if ('b' == next_ch || 'B' == next_ch) {
    raw_sql_.scan();
    next_ch = raw_sql_.peek();
    if (is_binary(next_ch)) {
      raw_sql_.scan();
      OZ (process_binary(false/*is_quote*/));
    } else {
      if (OB_LIKELY(process_idf_func_ != nullptr)) {
        OZ ((this->*process_idf_func_)(true));
      }
    }
  } else {
    OZ (process_number(false/*has_minus*/));
  }
  return ret;
}

int ObFastParserMysql::process_string(const char quote)
{
  int ret = OB_SUCCESS;
  bool is_quote_end = false;
  ParseNode **child_node = NULL;
  char ch = INVALID_CHAR;
  tmp_buf_len_ = 0;
  last_escape_check_pos_ = 0;
  if (nullptr == tmp_buf_ &&
      OB_ISNULL(tmp_buf_ = static_cast<char *>(allocator_.alloc(raw_sql_.raw_sql_len_ + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(raw_sql_.raw_sql_len_));
  } else {
    while (OB_SUCC(ret) && !raw_sql_.is_search_end()) {
      ch = raw_sql_.scan();
      int64_t copy_begin_pos = raw_sql_.cur_pos_;
      while (!raw_sql_.is_search_end() && '\\' != ch && quote != ch) {
        ch = raw_sql_.scan();
      }
      int64_t len = raw_sql_.cur_pos_ - copy_begin_pos;
      if (len > 0) {
        MEMCPY(tmp_buf_ + tmp_buf_len_, raw_sql_.ptr(copy_begin_pos), len);
        tmp_buf_len_ += len;
      }
      if (!is_valid_char(ch)) {
        break;
      } else if ('\\' == ch) {
        bool is_real_escape = true;
        bool is_no_backslash_escapes = false;
        check_real_escape(is_real_escape);
        IS_NO_BACKSLASH_ESCAPES(sql_mode_, is_no_backslash_escapes);
        if (!is_real_escape || is_no_backslash_escapes) {
          tmp_buf_[tmp_buf_len_++] = '\\';
        } else {
          process_escape_string(tmp_buf_, tmp_buf_len_);
        }
        last_escape_check_pos_ = tmp_buf_len_;
      } else if (quote == ch) {
        if (quote == raw_sql_.peek()) { // double quote
          ch = raw_sql_.scan();
          tmp_buf_[tmp_buf_len_++] = quote;
        } else {
          // deal with sqnewline({quote}{whitespace}{quote})
          int64_t ws_end_pos = is_whitespace(raw_sql_.cur_pos_ + 1);
          if (quote != raw_sql_.char_at(ws_end_pos)) {
            is_quote_end = true;
            break;
          }
          // cur_pos_ points to the position of a quote after sqnewline
          // continue processing the string
          raw_sql_.cur_pos_ = ws_end_pos;
          if (OB_ISNULL(child_node)) {
            char *buf = nullptr;
            int64_t need_mem_size = sizeof(ParseNode *) + PARSER_NODE_SIZE + tmp_buf_len_ + 1;
            if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
            } else {
              child_node = reinterpret_cast<ParseNode **>(buf);
              ParseNode *node = *child_node;
              // buf points to the beginning of the next available memory
              buf += sizeof(ParseNode *);
              node = new_node(buf, T_CONCAT_STRING);
              node->str_len_ = tmp_buf_len_;
              if (node->str_len_ > 0) {
                node->str_value_ = parse_strndup(tmp_buf_, tmp_buf_len_, buf);
              }
              child_node[0] = node;
            }
          }
        }
      }
    } // end while
    if (OB_SUCC(ret)) {
      // in ansi_quotes sql_mode, the "" is treated as `, shouldn't parameterize it.
      bool is_ansi_quotes = false;
      IS_ANSI_QUOTES(sql_mode_, is_ansi_quotes);
      raw_sql_.scan();
      if (!is_quote_end) {
        cur_token_type_ = IGNORE_TOKEN;
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
      } else if (is_ansi_quotes && quote == '"') {
        cur_token_type_ = NORMAL_TOKEN;
      } else {
        char *buf = nullptr;
        cur_token_type_ = PARAM_TOKEN;
        int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
        int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
        int64_t str_len = tmp_buf_len_;
        need_mem_size += str_len + 1; // '\0'
        // allocate all the memory needed at once
        if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
        } else {
          ObItemType param_type = T_VARCHAR;
          if ('n' == raw_sql_.char_at(cur_token_begin_pos_) ||
              'N' == raw_sql_.char_at(cur_token_begin_pos_)) {
            param_type = T_NCHAR;
          }
          ParseNode *node = new_node(buf, param_type);
          if (NULL != child_node) {
            node->num_child_ = 1;
            node->children_ = child_node;
          }
          node->text_len_ = text_len;
          node->str_len_ = str_len;
          node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
          if (node->str_len_ > 0) {
            node->str_value_ = parse_strndup(tmp_buf_, tmp_buf_len_, buf);
          }
          // buf points to the beginning of the next available memory
          buf += str_len + 1;
          node->raw_sql_offset_ = cur_token_begin_pos_;
          lex_store_param(node, buf);
        }
      }
    }
  }
  return ret;
}

int ObFastParserMysql::process_identifier_begin_with_n()
{
  int ret = OB_SUCCESS;
  if (CHECK_EQ_STRNCASECMP("ull", 3)) {
    raw_sql_.scan(3);
    if (-1 == is_identifier_flags(raw_sql_.cur_pos_)) {
      cur_token_type_ = PARAM_TOKEN;
      OZ (add_null_type_node());
    }
  } else if ('\'' == raw_sql_.char_at(raw_sql_.cur_pos_)) {
    OZ (process_string('\''));
  } else {
  }
  return ret;
}

int ObFastParserMysql::process_identifier_begin_with_backslash()
{
  int ret = OB_SUCCESS;
  if (raw_sql_.char_at(raw_sql_.cur_pos_) == 'N') {
    raw_sql_.scan(1);
    if (-1 == is_identifier_flags(raw_sql_.cur_pos_)) {
      cur_token_type_ = PARAM_TOKEN;
      OZ (add_null_type_node());
    }
  } else {
  }
  return ret;
}

int ObFastParserMysql::process_values(const char *str)
{
  int ret = OB_SUCCESS;
  if (found_insert_status_ == FOUND_INSERT_TOKEN_ONCE) {
    if (!is_oracle_mode_) {
      // mysql support: insert ... values / value (xx, ...);
      if (CHECK_EQ_STRNCASECMP("alues", 5)) {
        if (OB_FAIL(values_tokens_.push_back(ObValuesTokenPos(no_param_sql_len_ +
                    cur_token_begin_pos_ - copy_begin_pos_, param_num_)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          values_token_pos_ = raw_sql_.cur_pos_;
          raw_sql_.scan(5);
        }
      } else if (CHECK_EQ_STRNCASECMP("alue", 4)) {
        values_token_pos_ = raw_sql_.cur_pos_;
        raw_sql_.scan(4);
      } else {
        // do nothing
      }
    }
  } else {
    if (!is_oracle_mode_) {
      if (CHECK_EQ_STRNCASECMP("alues", 5)) {
        if (OB_FAIL(values_tokens_.push_back(ObValuesTokenPos(no_param_sql_len_ +
                    cur_token_begin_pos_ - copy_begin_pos_, param_num_)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          raw_sql_.scan(5);
        }
      }
    }
  }
  return ret;
}

int ObFastParserMysql::process_identifier(bool is_number_begin)
{
  int ret = OB_SUCCESS;
  bool need_process_ws = true;
  cur_token_type_ = INVALID_TOKEN;
  char ch = INVALID_CHAR;
  if (!is_number_begin) {
    char prev_ch = raw_sql_.char_at(cur_token_begin_pos_);
    switch (prev_ch) {
      case 't': // true, time, timestamp
      case 'T': {
        OZ (process_identifier_begin_with_t(need_process_ws));
        break;
      }
      case 'f': // false
      case 'F': {
        if (CHECK_EQ_STRNCASECMP("alse", 4)) {
          raw_sql_.scan(4);
          if (-1 == is_identifier_flags(raw_sql_.cur_pos_)) {
            cur_token_type_ = PARAM_TOKEN;
            OZ (add_bool_type_node(false/*is_true*/));
          }
        }
        break;
      }
      case 'n': // null, nowait, no_wait
      case 'N': {
        OZ (process_identifier_begin_with_n());
        break;
      }
      case 'd': // date, delete
      case 'D': {
        if (CHECK_EQ_STRNCASECMP("ate", 3)) {
          raw_sql_.scan(3);
          OZ (process_time_relate_type(need_process_ws, T_DATE));
        } else {
          CHECK_AND_PROCESS_HINT("elete", 5);
        }
        break;
      }
      case 's': // select
      case 'S': {
        CHECK_AND_PROCESS_HINT("elect", 5);
        break;
      }
      case 'u': // update
      case 'U': {
        CHECK_AND_PROCESS_HINT("pdate", 5);
        break;
      }
      case 'i': // insert or interval
      case 'I': {
        OZ (process_insert_or_replace("nsert", 5));
        break;
      }
      // check whether is 'values' token
      case 'v':
      case 'V':
        // 是不是values;
        OZ (process_values("alues"));
        break;

      case 'r': // replace
      case 'R': {
        OZ (process_insert_or_replace("eplace", 6));
        break;
      }
      case 'h': // hint
      case 'H': {
        CHECK_AND_PROCESS_HINT("int", 3);
        break;
      }
      case 'l': // load{space}+data
      case 'L': {
        OZ (process_identifier_begin_with_l(need_process_ws));
        break;
      }
      case 'b': // binary
      case 'B': {
        ch = raw_sql_.char_at(raw_sql_.cur_pos_);
        if ('\'' == ch && OB_FAIL(process_binary(true))) {
          LOG_WARN("failed to process binary", K(ret));
        }
        break;
      }
      case 'x': // hex
      case 'X': {
        ch = raw_sql_.char_at(raw_sql_.cur_pos_);
        if ('\'' == ch && OB_FAIL(process_hex_number(true))) {
          LOG_WARN("failed to process hex", K(ret));
        }
        break;
      }
      case '\\': {
        OZ (process_identifier_begin_with_backslash());
        break;
      }
      default: {
        break;
      }
    }
  }
  if (!is_valid_token()) {
    cur_token_type_ = NORMAL_TOKEN;
    if (need_process_ws) {
      int64_t next_idf_pos = raw_sql_.cur_pos_;
      while (-1 != (next_idf_pos = is_identifier_flags(next_idf_pos))) {
        raw_sql_.cur_pos_ = next_idf_pos;
        if ('.' == raw_sql_.char_at(raw_sql_.cur_pos_)) {
          raw_sql_.scan();
          next_idf_pos++;
        }
      }
    }
  }
  return ret;
}

int ObFastParserMysql::parse_next_token()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !raw_sql_.is_search_end()) {
    process_leading_space();
    char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
    cur_token_begin_pos_ = raw_sql_.cur_pos_;
    switch (ch) {
      case '0': {
        OZ (process_zero_identifier());
        break;
      }
      case '1' ... '9': {
        OZ (process_number(false/*has_minus*/));
        break;
      }
      case '.': {
        if (is_digit(raw_sql_.peek())) {
          OZ (process_number(false/*has_minus*/));
        } else {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        }
        break;
      }
      case '\'':
      case '\"': {
        OZ (process_string(ch));
        break;
      }
      case '`': {
        OZ (process_backtick());
        break;
      }
      case '-': {
        // need to deal with sql_comment or negative sign
        ch = raw_sql_.scan();
        if ('-' == ch &&
            raw_sql_.cur_pos_ + 1 < raw_sql_.raw_sql_len_ &&
            (raw_sql_.raw_sql_[raw_sql_.cur_pos_ + 1] == ' ' ||
             raw_sql_.raw_sql_[raw_sql_.cur_pos_ + 1] == '\t')) {
          // "--"[ \t]+{non_newline}*
          cur_token_type_ = IGNORE_TOKEN;
          // skip the second '-' and space
          raw_sql_.scan(1);
          while (!raw_sql_.is_search_end() && is_non_newline(ch)) {
            ch = raw_sql_.scan();
          }
        } else if ('-' == ch &&
                   raw_sql_.cur_pos_ + 1 < raw_sql_.raw_sql_len_ &&
                   (raw_sql_.raw_sql_[raw_sql_.cur_pos_ + 1] == '\n' ||
                    raw_sql_.raw_sql_[raw_sql_.cur_pos_ + 1] == '\r')) {
          // "--"[\n\r]
          cur_token_type_ = IGNORE_TOKEN;
          //skip the second '-' and ('\n' or \r)
          raw_sql_.scan(1);
        } else {
          OZ (process_negative());
        }
        break;
      }
      case '#': {
        // sql_comment: (#{non_newline}*)
        cur_token_type_ = IGNORE_TOKEN;
        ch = raw_sql_.scan();
        while (is_non_newline(ch)) {
          ch = raw_sql_.scan();
        }
        break;
      }
      case '/': {
        if ('*' == raw_sql_.peek()) {
          raw_sql_.scan();
          OZ (process_comment_content(('!' == raw_sql_.peek())));
        } else {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        }
        break;
      }
      case '*': {
        cur_token_type_ = NORMAL_TOKEN;
        raw_sql_.scan();
        break;
      }
      case ';': {
        // when encountering';', it means the end of sql
        cur_token_type_ = NORMAL_TOKEN;
        raw_sql_.scan();
        if (is_batched_multi_stmt_split_on_) {
          remove_multi_stmt_end_space();
        }
        raw_sql_.search_end_ = true;
        break;
      }
      case '?': {
        OZ (process_question_mark());
        break;
      }
      case ':': {
        // [":"{int_num}]
        if (-1 != is_first_identifier_flags(raw_sql_.cur_pos_ + 1) || is_digit(raw_sql_.peek())) {
          raw_sql_.scan();
          OZ (process_ps_statement());
        } else {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        }
        break;
      }
      case '@': {
        char next_ch = raw_sql_.peek();
        bool is_contain_quote = false;
        if ('@' == next_ch && is_sys_var_first_char(raw_sql_.char_at(raw_sql_.cur_pos_ + 2))) {
          raw_sql_.scan(2);
          process_system_variable(is_contain_quote);
        } else if ('@' == next_ch && raw_sql_.char_at(raw_sql_.cur_pos_ + 2) == '`' &&
                    is_sys_var_first_char(raw_sql_.char_at(raw_sql_.cur_pos_ + 3))) {
          raw_sql_.scan(3);
          is_contain_quote = true;
          process_system_variable(is_contain_quote);
        } else {
          if ('`' == next_ch || '\'' == next_ch || '\"' == next_ch) {
            raw_sql_.scan();
            is_contain_quote = true;
          }
          process_user_variable(is_contain_quote);
        }
        break;
      }
      default : {
        int64_t next_idf_pos = is_identifier_flags(raw_sql_.cur_pos_);
        if (-1 != next_idf_pos) {
          raw_sql_.cur_pos_ = next_idf_pos;
          if (OB_LIKELY(process_idf_func_ != nullptr)) {
            OZ ((this->*process_idf_func_)(false));
          }
        } else if (is_mysql_mode() && raw_sql_.char_at(raw_sql_.cur_pos_) == '\\') {
          raw_sql_.cur_pos_ += 1;
          if (OB_LIKELY(process_idf_func_ != nullptr)) {
            OZ ((this->*process_idf_func_)(false));
          }
        } else if (is_normal_char(ch)) {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        } else {
          cur_token_type_ = IGNORE_TOKEN;
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
        }
        break;
      }
    } // end switch
    OX (process_token());
  } // end while
  if (OB_SUCC(ret)) {
    // After processing the string, there are still parts that have not been saved, save directly
    // for example, in the case of normal tokens
    if (copy_end_pos_ > copy_begin_pos_) {
      append_no_param_sql();
    }
  }
  return ret;
}

/**
 * @param [in] : if in_q_quote is true, means that the current token
 * starts with ("N"|"n")?("Q"|"q"){sqbegin}
 * else, means that the current token starts with ("N"|"n")?{sqbegin }
 */
int ObFastParserOracle::process_string(const bool in_q_quote)
{
  int ret = OB_SUCCESS;
  bool is_quote_end = false;
  ParseNode **child_node = NULL;
  char ch = INVALID_CHAR;
  tmp_buf_len_ = 0;
  if (nullptr == tmp_buf_ &&
      OB_ISNULL(tmp_buf_ = static_cast<char *>(allocator_.alloc(raw_sql_.raw_sql_len_ + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(raw_sql_.raw_sql_len_));
  } else {
    while (OB_SUCC(ret) && !raw_sql_.is_search_end()) {
      ch = raw_sql_.scan();
      int64_t copy_begin_pos = raw_sql_.cur_pos_;
      while (!raw_sql_.is_search_end() && '\\' != ch && '\'' != ch) {
        ch = raw_sql_.scan();
      }
      int64_t len = raw_sql_.cur_pos_ - copy_begin_pos;
      if (len > 0) {
        MEMCPY(tmp_buf_ + tmp_buf_len_, raw_sql_.ptr(copy_begin_pos), len);
        tmp_buf_len_ += len;
      }
      if (!is_valid_char(ch)) {
        break;
      } else if ('\\' == ch) {
        tmp_buf_[tmp_buf_len_++] = '\\';
      } else if ('\'' == ch) {
        if ('\'' == raw_sql_.peek()) { // double quote
          ch = raw_sql_.scan();
          tmp_buf_[tmp_buf_len_++] = '\'';
          if (in_q_quote) {
            tmp_buf_[tmp_buf_len_++] = '\'';
          }
        } else {
          if (in_q_quote) {
            // eg: q'<test>', nq'[asdfasd\'dfasdf]'
            int64_t byte_len = 0;
            if (is_multi_byte_left_parenthesis(tmp_buf_, tmp_buf_len_, 0, byte_len) &&
                is_multi_byte_right_parenthesis(tmp_buf_, tmp_buf_len_,
                tmp_buf_len_ - byte_len, byte_len)) {
              tmp_buf_ += byte_len;
              tmp_buf_len_ -= (2 * byte_len);
              is_quote_end = true;
              break;
            } else if (tmp_buf_len_ >= 2 &&
                ((tmp_buf_[0] == tmp_buf_[tmp_buf_len_ - 1] && tmp_buf_[0] != '(' &&
                tmp_buf_[0] != '[' && tmp_buf_[0] != '{' && tmp_buf_[0] != '<' &&
                tmp_buf_[0] != ' ' && tmp_buf_[0] != '\t' && tmp_buf_[0] != '\r') ||
                (tmp_buf_[0] == '(' && tmp_buf_[tmp_buf_len_ - 1] == ')') ||
                (tmp_buf_[0] == '[' && tmp_buf_[tmp_buf_len_ - 1] == ']') ||
                (tmp_buf_[0] == '{' && tmp_buf_[tmp_buf_len_ - 1] == '}') ||
                (tmp_buf_[0] == '<' && tmp_buf_[tmp_buf_len_ - 1] == '>'))) {
              tmp_buf_ += 1;
              tmp_buf_len_ -= 2;
              is_quote_end = true;
              break;
            } else {
              tmp_buf_[tmp_buf_len_++] = '\'';
            }
          } else {
            is_quote_end = true;
            break;
          }
        }
      }
    } // end while
    if (OB_SUCC(ret)) {
      raw_sql_.scan();
      if (!is_quote_end) {
        cur_token_type_ = IGNORE_TOKEN;
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
      } else {
        char *buf = nullptr;
        cur_token_type_ = PARAM_TOKEN;
        ObItemType param_type = T_CHAR;
        int64_t need_mem_size = FIEXED_PARAM_NODE_SIZE;
        int64_t text_len = raw_sql_.cur_pos_ - cur_token_begin_pos_;
        int64_t str_len = tmp_buf_len_;
        need_mem_size += str_len + 1; // '\0'
        if ('n' == raw_sql_.char_at(cur_token_begin_pos_) ||
            'N' == raw_sql_.char_at(cur_token_begin_pos_) ||
            'u' == raw_sql_.char_at(cur_token_begin_pos_) ||
            'U' == raw_sql_.char_at(cur_token_begin_pos_)) {
          param_type = T_NCHAR;
        }
        // allocate all the memory needed at once
        if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(need_mem_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(need_mem_size));
        } else {
          ParseNode *node = new_node(buf, param_type);
          node->text_len_ = text_len;
          node->str_len_ = str_len;
          node->raw_text_ = raw_sql_.ptr(cur_token_begin_pos_);
          if (node->str_len_ > 0) {
            node->str_value_ = parse_strndup(tmp_buf_, tmp_buf_len_, buf);
          }
          // buf points to the beginning of the next available memory
          buf += str_len + 1;
          node->raw_sql_offset_ = cur_token_begin_pos_;
          if (in_q_quote) {
            node->raw_sql_offset_ = cur_token_begin_pos_ + 1;
          } else {
            node->raw_sql_offset_ = cur_token_begin_pos_;
          }

          if ('u' == raw_sql_.char_at(cur_token_begin_pos_) ||
              'U' == raw_sql_.char_at(cur_token_begin_pos_)) {
            node->value_ = -1;
          }
          lex_store_param(node, buf);
        }
      }
    }
  }
  return ret;
}

int ObFastParserOracle::process_identifier_begin_with_n()
{
  int ret = OB_SUCCESS;
  char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
  if (CHECK_EQ_STRNCASECMP("ull", 3)) {
    raw_sql_.scan(3);
    if (-1 == is_identifier_flags(raw_sql_.cur_pos_)) {
      cur_token_type_ = PARAM_TOKEN;
      OZ (add_null_type_node());
    }
  } else if ('q' == ch || 'Q' == ch || '\'' == ch) {
    if ('\'' == ch) {
      OZ (process_string(false));
    } else {
      char next_ch = raw_sql_.peek();
      if ('\'' == next_ch) {
        raw_sql_.scan();
        OZ (process_string(true));
      }
    }
  } else {
  }
  return ret;
}

int ObFastParserOracle::process_values(const char *str)
{
  int ret = OB_SUCCESS;
  if (found_insert_status_ == FOUND_INSERT_TOKEN_ONCE) {
    if (is_oracle_mode_) {
      if (CHECK_EQ_STRNCASECMP("alues", 5)) {
        values_token_pos_ = raw_sql_.cur_pos_;
        raw_sql_.scan(5);
      }
    }
  }
  return ret;
}

int ObFastParserOracle::process_identifier(bool is_number_begin)
{
  int ret = OB_SUCCESS;
  bool need_process_ws = true;
  cur_token_type_ = INVALID_TOKEN;
  char ch = INVALID_CHAR;
  if (!is_number_begin) {
    char prev_ch = raw_sql_.char_at(cur_token_begin_pos_);
    switch (prev_ch) {
      case 't': // true, time, timestamp
      case 'T': {
        OZ (process_identifier_begin_with_t(need_process_ws));
        break;
      }
      case 'f': // false
      case 'F': {
        if (CHECK_EQ_STRNCASECMP("alse", 4)) {
          raw_sql_.scan(4);
          if (-1 == is_identifier_flags(raw_sql_.cur_pos_)) {
            cur_token_type_ = PARAM_TOKEN;
            OZ (add_bool_type_node(false/*is_true*/));
          }
        }
        break;
      }
      case 'n': // null, nowait, no_wait
      case 'N': {
        OZ (process_identifier_begin_with_n());
        break;
      }
      case 'd': // date, delete
      case 'D': {
        if (CHECK_EQ_STRNCASECMP("ate", 3)) {
          raw_sql_.scan(3);
          OZ (process_time_relate_type(need_process_ws, T_DATETIME));
        } else {
          CHECK_AND_PROCESS_HINT("elete", 5);
        }
        break;
      }
      case 's': // select
      case 'S': {
        CHECK_AND_PROCESS_HINT("elect", 5);
        break;
      }
      case 'u': // update
      case 'U': {
        if ('\'' == raw_sql_.char_at(raw_sql_.cur_pos_)) {
          OZ (process_string(false));
        } else {
          CHECK_AND_PROCESS_HINT("pdate", 5);
        }
        break;
      }
      case 'i': // insert or interval
      case 'I': {
        if (CHECK_EQ_STRNCASECMP("nterval", 7)) {
          raw_sql_.scan(7);
          OZ (process_time_relate_type(need_process_ws));
        } else {
          OZ (process_insert_or_replace("nsert", 5));
        }
        break;
      }
      case 'm': // merge
      case 'M': {
        CHECK_AND_PROCESS_HINT("erge", 4);
        break;
      }
      case 'h': // hint
      case 'H': {
        CHECK_AND_PROCESS_HINT("int", 3);
        break;
      }
      case 'l': // load{space}+data
      case 'L': {
        OZ (process_identifier_begin_with_l(need_process_ws));
        break;
      }
      case 'q':
      case 'Q': {
        ch = raw_sql_.char_at(raw_sql_.cur_pos_);
        if ('\'' == ch && OB_FAIL(process_string(true))) {
          LOG_WARN("failed to handle string", K(ret));
        }
        break;
      }
      case 'v':
      case 'V':
        OZ (process_values("alues"));
        break;
      default: {
        break;
      }
    }
  }
  if (!is_valid_token()) {
    cur_token_type_ = NORMAL_TOKEN;
    if (need_process_ws) {
      int64_t next_idf_pos = raw_sql_.cur_pos_;
      ch = raw_sql_.char_at(raw_sql_.cur_pos_);
      while (-1 != (next_idf_pos = is_identifier_flags(next_idf_pos))) {
        raw_sql_.cur_pos_ = next_idf_pos;
      }
    }
  }
  return ret;
}

int ObFastParserOracle::parse_next_token()
{
  int ret = OB_SUCCESS;
  char last_ch;
  last_ch = '0';
  while (OB_SUCC(ret) && !raw_sql_.is_search_end()) {
    process_leading_space();
    char ch = raw_sql_.char_at(raw_sql_.cur_pos_);
    cur_token_begin_pos_ = raw_sql_.cur_pos_;
    switch (ch) {
      case '0' ... '9': {
        if (OB_FAIL(process_number(false/*has_minus*/))) {
          LOG_WARN("failed to handle number", K(ret));
        }
        break;
      }
      case '.': {
        if (is_digit(raw_sql_.peek())) {
          if (OB_FAIL(process_number(false/*has_minus*/))) {
            LOG_WARN("failed to handle number", K(ret));
          }
        } else {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        }
        break;
      }
      case '\'': {
        if (OB_FAIL(process_string(false/*q_quote*/))) {
          LOG_WARN("failed to handle string", K(ret));
        }
        break;
      }
      case '-': {
        // need to deal with sql_comment or negative sign
        ch = raw_sql_.scan();
        if ('-' == ch) {
          // "--"{non_newline}*
          cur_token_type_ = IGNORE_TOKEN;
          ch = raw_sql_.scan();
          while (!raw_sql_.is_search_end() && is_non_newline(ch)) {
            ch = raw_sql_.scan();
          }
        } else if (OB_FAIL(process_negative())) {
          LOG_WARN("failed to handle negative", K(ret));
        }
        break;
      }
      case '\"': {
        OZ (process_double_quote());
        break;
      }
      case '/': {
        if ('*' == raw_sql_.peek()) {
          raw_sql_.scan();
          OZ (process_comment_content());
        } else {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        }
        break;
      }
      case ';': {
        // when encountering';', it means the end of sql
        cur_token_type_ = NORMAL_TOKEN;
        raw_sql_.scan();
        if (is_batched_multi_stmt_split_on_) {
          remove_multi_stmt_end_space();
        }
        raw_sql_.search_end_ = true;
        break;
      }
      case '?': {
        OZ (process_question_mark());
        break;
      }
      case ':': {
        if ((-1 != is_first_identifier_flags(raw_sql_.cur_pos_ + 1) || is_digit(raw_sql_.peek())) && last_ch != '\'') {
          raw_sql_.scan();
          OZ (process_ps_statement());
        } else {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        }
        break;
      }
      case '@': {
        char next_ch = raw_sql_.peek();
        bool is_contain_quote = false;
        if ('@' == next_ch && is_sys_var_first_char(raw_sql_.char_at(raw_sql_.cur_pos_ + 2))) {
          raw_sql_.scan(2);
          process_system_variable(is_contain_quote);
        } else {
          if ('\'' == next_ch || '\"' == next_ch) {
            raw_sql_.scan();
            is_contain_quote = true;
          }
          process_user_variable(is_contain_quote);
        }
        break;
      }
      default : {
        int64_t next_idf_pos = is_first_identifier_flags(raw_sql_.cur_pos_);
        if (-1 != next_idf_pos) {
          raw_sql_.cur_pos_ = next_idf_pos;
          if (OB_LIKELY(process_idf_func_ != nullptr)) {
            OZ ((this->*process_idf_func_)(false));
          }
        } else if (is_normal_char(ch)) {
          cur_token_type_ = NORMAL_TOKEN;
          raw_sql_.scan();
        } else {
          cur_token_type_ = IGNORE_TOKEN;
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("parser syntax error", K(ret), K(raw_sql_.to_string()), K_(raw_sql_.cur_pos));
        }
        break;
      }
    } // end switch
    last_ch = ch;
    OX (process_token());
  } // end while
  if (OB_SUCC(ret)) {
    // After processing the string, there are still parts that have not been saved, save directly
    // for example, in the case of normal tokens
    if (copy_end_pos_ > copy_begin_pos_) {
      append_no_param_sql();
    }
  }
  return ret;
}

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

#include "lib/charset/ob_charset.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/json/ob_json.h"

#ifndef _OB_LOAD_DATA_PARSER_H_
#define _OB_LOAD_DATA_PARSER_H_

namespace oceanbase
{
namespace sql
{
class ObDataInFileStruct;

struct ObCSVGeneralFormat {
  ObCSVGeneralFormat () :
    line_start_str_(),
    line_term_str_(),
    field_term_str_(),
    field_escaped_char_(INT64_MAX),
    field_enclosed_char_(INT64_MAX),
    cs_type_(common::CHARSET_INVALID),
    skip_header_lines_(0),
    skip_blank_lines_(false),
    trim_space_(false),
    null_if_(),
    empty_field_as_null_(false),
    file_column_nums_(0)
  {}
  static constexpr const char *OPTION_NAMES[] = {
    "LINE_DELIMITER",
    "FIELD_DELIMITER",
    "ESCAPE",
    "FIELD_OPTIONALLY_ENCLOSED_BY",
    "ENCODING",
    "SKIP_HEADER",
    "SKIP_BLANK_LINES",
    "TRIM_SPACE",
    "NULL_IF_EXETERNAL",
    "EMPTY_FIELD_AS_NULL",
  };
  common::ObString line_start_str_;
  common::ObString line_term_str_;
  common::ObString field_term_str_;
  int64_t field_escaped_char_;    // valid escaped char after stmt validation
  int64_t field_enclosed_char_;   // valid enclosed char after stmt validation
  common::ObCharsetType cs_type_; // charset type of format strings
  int64_t skip_header_lines_;
  bool skip_blank_lines_;
  bool trim_space_;
  common::ObArrayWrap<common::ObString> null_if_;
  bool empty_field_as_null_;

  int64_t file_column_nums_;

  int init_format(const ObDataInFileStruct &format,
                  int64_t file_column_nums,
                  ObCollationType file_cs_type);
  int64_t to_json_kv_string(char* buf, const int64_t buf_len) const;
  int load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator);

  TO_STRING_KV(K(cs_type_), K(file_column_nums_), K(line_start_str_), K(field_enclosed_char_),
               K(field_escaped_char_), K(field_term_str_), K(line_term_str_));
  OB_UNIS_VERSION(1);
};

/**
 * @brief Fast csv general parser is mysql compatible csv parser
 *        It support single-byte or multi-byte separators
 *        It support utf8, gbk and gb18030 character set
 */
class ObCSVGeneralParser
{
public:
  struct LineErrRec
  {
    LineErrRec() : line_no(0), err_code(0) {}
    int line_no;
    int err_code;
    TO_STRING_KV(K(line_no), K(err_code));
  };
  struct FieldValue {
    FieldValue() : ptr_(nullptr), len_(0), flags_(0) {}
    char *ptr_;
    int32_t len_;
    union {
      struct {
        uint32_t is_null_:1;
        uint32_t reserved_:31;
      };
      uint32_t flags_;
    };
    TO_STRING_KV(KP(ptr_), K(len_), K(flags_), "string", common::ObString(len_, ptr_));
  };
  struct OptParams {
    OptParams() : line_term_c_(0), field_term_c_(0),
      is_filling_zero_to_empty_field_(false),
      is_line_term_by_counting_field_(false),
      is_same_escape_enclosed_(false),
      is_simple_format_(false)
    {}
    char line_term_c_;
    char field_term_c_;
    bool is_filling_zero_to_empty_field_;
    bool is_line_term_by_counting_field_;
    bool is_same_escape_enclosed_;
    bool is_simple_format_;
  };
public:
  ObCSVGeneralParser() {}
  int init(const ObCSVGeneralFormat &format);

  int init(const ObDataInFileStruct &format,
           int64_t file_column_nums,
           common::ObCollationType file_cs_type);
  const ObCSVGeneralFormat &get_format() { return format_; }
  const OptParams &get_opt_params() { return opt_param_; }

  template<common::ObCharsetType cs_type, typename handle_func, bool NEED_ESCAPED_RESULT = false>
  int scan_proto(const char *&str, const char *end, int64_t &nrows,
                 char *escape_buf, char *escaped_buf_end,
                 handle_func &handle_one_line,
                 common::ObIArray<LineErrRec> &errors,
                 bool is_end_file);

  template<typename handle_func, bool NEED_ESCAPED_RESULT = false>
  int scan(const char *&str, const char *end, int64_t &nrows,
           char *escape_buf, char *escaped_buf_end,
           handle_func &handle_one_line,
           common::ObIArray<LineErrRec> &errors,
           bool is_end_file = false) {
    int ret = common::OB_SUCCESS;
    switch (format_.cs_type_) {
    case common::CHARSET_UTF8MB4:
      ret = scan_proto<common::CHARSET_UTF8MB4, handle_func, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    case common::CHARSET_GBK:
      ret = scan_proto<common::CHARSET_GBK, handle_func, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    case common::CHARSET_GB18030:
    case common::CHARSET_GB18030_2022:
      ret = scan_proto<common::CHARSET_GB18030, handle_func, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    default:
      ret = scan_proto<common::CHARSET_BINARY, handle_func, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    }
    return ret;
  }
  common::ObIArray<FieldValue>& get_fields_per_line() { return fields_per_line_; }

private:
  int init_opt_variables();
  template<common::ObCharsetType cs_type>
  inline int mbcharlen(const char *ptr, const char *end) {
    UNUSED(ptr);
    UNUSED(end);
    return 1;
  }

  int handle_irregular_line(int field_idx,
                            int line_no,
                            common::ObIArray<LineErrRec> &errors);
  inline
  char escape(char c) {
    switch (c) {
    case '0':
      return '\0';
    case 'b':
      return '\b';
    case 'n':
      return '\n';
    case 'r':
      return '\r';
    case 't':
      return '\t';
    case 'Z':
      return '\032';
    }
    return c;
  }


  inline
  bool is_null_field(const char* ori_field_begin, int64_t ori_field_len,
                     const char* final_field_begin, int64_t final_field_len) {
    bool ret = false;

    if ((2 == ori_field_len && format_.field_escaped_char_ == ori_field_begin[0] && 'N' == ori_field_begin[1])
        || (format_.field_enclosed_char_ != INT64_MAX && 4 == ori_field_len && 0 == MEMCMP(ori_field_begin, "NULL", 4))
        || (format_.empty_field_as_null_ && 0 == final_field_len)) {
      ret = true;
    } else {
      for (int i = 0; i < format_.null_if_.count(); i++) {
        if (format_.null_if_.at(i).length() == final_field_len
            && 0 == MEMCMP(final_field_begin, format_.null_if_.at(i).ptr(), final_field_len)) {
          ret = true;
          break;
        }
      }
    }
    return ret;
  }

  inline
  void gen_new_field(const bool is_enclosed, const char *ori_field_begin, const char *ori_field_end,
                     const char *field_begin, const char *field_end, const int field_idx) {
    FieldValue &new_field = fields_per_line_[field_idx - 1];
    new_field = FieldValue();
    if (format_.trim_space_) {
      while (field_begin < field_end && ' ' == *field_begin) field_begin++;
      while (field_begin < field_end && ' ' == *(field_end - 1)) field_end--;
    }
    if (is_null_field(ori_field_begin, ori_field_end - ori_field_begin, field_begin, field_end - field_begin)) {
      new_field.is_null_ = 1;
    } else {
      new_field.ptr_ = const_cast<char*>(field_begin);
      new_field.len_ = static_cast<int32_t>(field_end - field_begin);
    }
  }

  inline bool is_escape_next(const bool is_enclosed, const char cur, const char next) {
    // 1. the next char escaped by escape_char "A\tB" => A  B
    // 2. enclosed char escaped by another enclosed char in enclosed field. E.g. "A""B" => A"B
    return (format_.field_escaped_char_ == cur && !opt_param_.is_same_escape_enclosed_)
           || (is_enclosed && format_.field_enclosed_char_ == cur && format_.field_enclosed_char_ == next);
  }

protected:
  ObCSVGeneralFormat format_;
  common::ObSEArray<FieldValue, 1> fields_per_line_;
  OptParams opt_param_;
};


template<>
inline int ObCSVGeneralParser::mbcharlen<common::CHARSET_UTF8MB4>(const char *ptr, const char *end) {
  int mb_len = 1;
  UNUSED(end);
  unsigned char c = *ptr;
  if (c < 0x80) {
    mb_len = 1;
  } else if (c < 0xc2) {
    mb_len = 1; /* Illegal mb head */
  } else if (c < 0xe0) {
    mb_len = 2;
  } else if (c < 0xf0) {
    mb_len = 3;
  } else if (c < 0xf8) {
    mb_len = 4;
  }
  return mb_len; /* Illegal mb head */;
}

template<>
inline int ObCSVGeneralParser::mbcharlen<common::CHARSET_GBK>(const char *ptr, const char *end) {
  UNUSED(end);
  unsigned char c = *ptr;
  return (0x81 <= c && c <= 0xFE) ? 2 : 1;
}

template<>
inline int ObCSVGeneralParser::mbcharlen<common::CHARSET_GB18030>(const char *ptr, const char *end) {
  int mb_len = 1;
  if (end - ptr > 1) {
    unsigned char low_c = static_cast<unsigned char>(*(ptr + 1));
    unsigned char high_c = static_cast<unsigned char>(*ptr);
    if (!(0x81 <= high_c && high_c <= 0xFE)) {
      mb_len = 1;
    } else if ((0x40 <= low_c && low_c <= 0x7E) || (0x80 <= low_c && low_c <= 0xFE)) {
      mb_len = 2;
    } else if (0x30 <= low_c && low_c <= 0x39) {
      mb_len = 4;
    }
  }
  return mb_len;
}

template<common::ObCharsetType cs_type, typename handle_func, bool NEED_ESCAPED_RESULT>
int ObCSVGeneralParser::scan_proto(const char *&str,
                                   const char *end,
                                   int64_t &nrows,
                                   char *escape_buf,
                                   char *escape_buf_end,
                                   handle_func &handle_one_line,
                                   common::ObIArray<LineErrRec> &errors,
                                   bool is_end_file)
{
  int ret = common::OB_SUCCESS;
  int line_no = 0;
  int blank_line_cnt = 0;
  const char *line_begin = str;
  char *escape_buf_pos = escape_buf;

  if (NEED_ESCAPED_RESULT) {
    if (escape_buf_end - escape_buf < end - str) {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
  }

  while (OB_SUCC(ret) && str < end && line_no - blank_line_cnt < nrows) {
    bool find_new_line = false;
    int field_idx = 0;

    if (!format_.line_start_str_.empty()) {
      bool is_line_start_found = false;
      for (; str + format_.line_start_str_.length() <= end; str++) {
        if (0 == MEMCMP(str, format_.line_start_str_.ptr(), format_.line_start_str_.length())) {
          str += format_.line_start_str_.length();
          is_line_start_found = true;
          break;
        }
      }
      if (!is_line_start_found) {
        if (is_end_file) {
          line_begin = end;
        } else {
          line_begin = str;
        }
        break;
      }
    }

    while (str < end && !find_new_line) {
      const char *ori_field_begin = str;
      const char *field_begin = str;
      bool is_enclosed = false;
      const char *last_end_enclosed = nullptr;
      const char *last_escaped_str = nullptr;
      bool is_term = false;
      bool is_field_term = false;
      bool is_line_term = false;

      if (format_.field_enclosed_char_ == *str) {
        is_enclosed = true;
        str++;
      }
      while (str < end && !is_term) {
        const char *next = str + 1;
        if (next < end && is_escape_next(is_enclosed, *str, *next)) {
          if (NEED_ESCAPED_RESULT) {
            if (last_escaped_str == nullptr) {
              last_escaped_str = field_begin;
              field_begin = escape_buf_pos;
            }
            int64_t copy_len = str - last_escaped_str;
            //if (OB_UNLIKELY(escape_buf_pos + copy_len + 1 > escape_buf_end)) {
            //  ret = common::OB_SIZE_OVERFLOW; break;
            //} else {
              MEMCPY(escape_buf_pos, last_escaped_str, copy_len);
              escape_buf_pos+=copy_len;
              *(escape_buf_pos++) = escape(*next);
              last_escaped_str = next + 1;
            //}
          }
          str += 2;
        } else if (format_.field_enclosed_char_ == *str) {
          last_end_enclosed = str;
          str++;
        } else {
          is_field_term = (*str == opt_param_.field_term_c_
              && str <= end - format_.field_term_str_.length()
              && 0 == MEMCMP(str, format_.field_term_str_.ptr(), format_.field_term_str_.length()));

          is_line_term = (*str == opt_param_.line_term_c_
              && str <= end - format_.line_term_str_.length()
              && 0 == MEMCMP(str, format_.line_term_str_.ptr(), format_.line_term_str_.length()));

          //if field is enclosed, there is a enclose char just before the terminate string
          is_term = (is_field_term || is_line_term)
                    && (!is_enclosed || (str - 1 == last_end_enclosed));

          if (!is_term) {
            int mb_len = mbcharlen<cs_type>(str, end);
            str += mb_len;
          }
        }
      }

      if (OB_LIKELY(is_term) || is_end_file) {
        const char *ori_field_end = str;
        const char *field_end = str;
        if (is_enclosed && field_end - 1 == last_end_enclosed) {
          field_begin++;
          field_end--;
        }
        if (OB_LIKELY(is_term)) {
          str += is_field_term ? format_.field_term_str_.length() : format_.line_term_str_.length();
        } else {
          str = end;
        }

        if (NEED_ESCAPED_RESULT) {
          if (last_escaped_str != nullptr) {
            int64_t copy_len = field_end - last_escaped_str;
            //if (OB_UNLIKELY(escape_buf_pos + copy_len > escape_buf_end)) {
            //  ret = common::OB_SIZE_OVERFLOW;
            //} else {
            if  (copy_len > 0) {
              MEMCPY(escape_buf_pos, last_escaped_str, copy_len);
              escape_buf_pos+=copy_len;
            }
            //}
            field_end = escape_buf_pos;
          }
        }

        if (is_field_term || ori_field_end > ori_field_begin
            || (field_idx < format_.file_column_nums_ && !format_.skip_blank_lines_)) {
          if (field_idx++ < format_.file_column_nums_) {
            gen_new_field(is_enclosed, ori_field_begin, ori_field_end, field_begin, field_end, field_idx);
          }
        }

        if (is_line_term
            && (!opt_param_.is_line_term_by_counting_field_ || field_idx == format_.file_column_nums_)) {
          find_new_line = true;
        }
      }
    }
    if (OB_LIKELY(find_new_line) || is_end_file) {
      if (!format_.skip_blank_lines_ || field_idx > 0) {
        if (field_idx != format_.file_column_nums_) {
          ret = handle_irregular_line(field_idx, line_no, errors);
        }
        if (OB_SUCC(ret)) {
          ret = handle_one_line(fields_per_line_);
        }
      } else {
        if (format_.skip_blank_lines_) {
          blank_line_cnt++;
        }
      }
      line_no++;
      line_begin = str;
    }
  }

  str = line_begin;
  nrows = line_no;
  return ret;
}

// user using to define create external table format
struct ObOriginFileFormat
{
  int64_t to_json_kv_string(char *buf, const int64_t buf_len) const;
  int load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator);
  TO_STRING_KV(K(origin_line_term_str_), K(origin_field_term_str_), K(origin_field_escaped_str_),
                K(origin_field_enclosed_str_), K(origin_null_if_str_));

  static constexpr const char *ORIGIN_FORMAT_STRING[] = {
    "ORIGIN_LINE_DELIMITER",
    "ORIGIN_FIELD_DELIMITER",
    "ORIGIN_ESCAPE",
    "ORIGIN_FIELD_OPTIONALLY_ENCLOSED_BY",
    "ORIGIN_NULL_IF_EXETERNAL",
  };

  common::ObString origin_line_term_str_;
  common::ObString origin_field_term_str_;
  common::ObString origin_field_escaped_str_;
  common::ObString origin_field_enclosed_str_;
  common::ObString origin_null_if_str_;
};

struct ObExternalFileFormat
{
  struct StringData {
    StringData(common::ObIAllocator &alloc) : allocator_(alloc) {}
    int store_str(const ObString &str);
    common::ObString str_;
    common::ObIAllocator &allocator_;
    TO_STRING_KV(K_(str));
    OB_UNIS_VERSION(1);
  };

  enum FormatType {
    INVALID_FORMAT = -1,
    CSV_FORMAT,
    MAX_FORMAT
  };


  ObExternalFileFormat() : format_type_(INVALID_FORMAT) {}

  int64_t to_string(char* buf, const int64_t buf_len) const;
  int load_from_string(const common::ObString &str, ObIAllocator &allocator);

  ObOriginFileFormat origin_file_format_str_;
  FormatType format_type_;
  sql::ObCSVGeneralFormat csv_format_;
};


}
}

#endif //_OB_LOAD_DATA_PARSER_H_

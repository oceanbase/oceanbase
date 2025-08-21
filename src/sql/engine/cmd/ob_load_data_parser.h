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
#ifndef _OB_LOAD_DATA_PARSER_H_
#define _OB_LOAD_DATA_PARSER_H_

#include "lib/charset/ob_charset.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/json/ob_json.h"
#include "lib/charset/ob_charset_string_helper.h"
#include "plugin/external_table/ob_external_format.h"

namespace oceanbase
{
namespace share {
namespace schema {
class ObColumnSchemaV2;
}
}
namespace sql
{
class ObDataInFileStruct;
struct ObODPSGeneralFormatParam {
  const static ObString TUNNEL_API;
  const static ObString STORAGE_API;
  const static ObString BYTE;
  const static ObString ROW;
};

enum ColumnIndexType
{
  NAME = 0,      // 按列名索引
  POSITION = 1,  // 按列顺序（位置）索引
  ID = 2         // 按列id索引
};

struct ObODPSGeneralFormat {
  enum ApiMode {
    TUNNEL_API = 0,
    BYTE,
    ROW,
  };
  ObODPSGeneralFormat() :
    access_type_(),
    access_id_(),
    access_key_(),
    sts_token_(),
    endpoint_(),
    tunnel_endpoint_(),
    project_(),
    schema_(),
    table_(),
    quota_(),
    compression_code_(),
    collect_statistics_on_create_(false),
    region_(),
    api_mode_(ApiMode::TUNNEL_API)
  {
  }
  int deep_copy_str(const ObString &src,
                    ObString &dest);
  int deep_copy(const ObODPSGeneralFormat &src);
  int encrypt_str(common::ObString &src, common::ObString &dst);
  int decrypt_str(common::ObString &src, common::ObString &dst);
  int encrypt();
  int decrypt();
  static constexpr const char *OPTION_NAMES[] = {
    "ACCESSTYPE",
    "ACCESSID",
    "ACCESSKEY",
    "STSTOKEN",
    "ENDPOINT",
    "TUNNEL_ENDPOINT",
    "PROJECT_NAME",
    "SCHEMA_NAME",
    "TABLE_NAME",
    "QUOTA_NAME",
    "COMPRESSION_CODE",
    "COLLECT_STATISTICS_ON_CREATE",
    "REGION",
    "API_MODE",
    "SPLIT"
  };
  common::ObString access_type_;
  common::ObString access_id_;
  common::ObString access_key_;
  common::ObString sts_token_;
  common::ObString endpoint_;
  common::ObString tunnel_endpoint_;
  common::ObString project_;
  common::ObString schema_;
  common::ObString table_;
  common::ObString quota_;
  common::ObString compression_code_;
  bool collect_statistics_on_create_;
  common::ObString region_;
  ApiMode api_mode_;
  common::ObArenaAllocator arena_alloc_;
  int to_json_kv_string(char* buf, const int64_t buf_len, int64_t &pos) const;
  int load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator);
  TO_STRING_KV(K_(access_type), K_(access_id), K_(access_key), K_(sts_token),
               K_(endpoint), K_(tunnel_endpoint), K_(project), K_(schema), K_(table), K_(quota),
               K_(compression_code), K_(collect_statistics_on_create), K_(region), K(api_mode_));
  OB_UNIS_VERSION(1);
};

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
    ignore_extra_fields_(false),
    trim_space_(false),
    null_if_(),
    empty_field_as_null_(false),
    file_column_nums_(0),
    compression_algorithm_(ObCSVCompression::NONE),
    is_optional_(false),
    file_extension_(DEFAULT_FILE_EXTENSION),
    parse_header_(false),
    binary_format_(ObCSVBinaryFormat::DEFAULT),
    ignore_last_empty_col_(true)
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
    "COMPRESSION",
    "IS_OPTIONAL",
    "FILE_EXTENSION",
    "PARSE_HEADER",
    "BINARY_FORMAT",
    "IGNORE_LAST_EMPTY_COLUMN"
  };

  // ObCSVOptionsEnum should keep the same order as OPTION_NAMES
  enum class ObCSVOptionsEnum {
    LINE_DELIMITER = 0,
    FIELD_DELIMITER,
    ESCAPE,
    FIELD_OPTIONALLY_ENCLOSED_BY,
    ENCODING,
    SKIP_HEADER,
    SKIP_BLANK_LINES,
    TRIM_SPACE,
    NULL_IF_EXETERNAL,
    EMPTY_FIELD_AS_NULL,
    COMPRESSION,
    IS_OPTIONAL,
    FILE_EXTENSION,
    PARSE_HEADER,
    BINARY_FORMAT,
    IGNORE_LAST_EMPTY_COLUMN,
    // put new options here, before MAX_OPTIONS
    // ....
    MAX_OPTIONS
  };

  // Make sure OPTION_NAMES has the same length as ObCSVOptionsEnum
  static_assert((sizeof(OPTION_NAMES) / sizeof(OPTION_NAMES[0])) == static_cast<size_t>(ObCSVOptionsEnum::MAX_OPTIONS),
    "OPTION_NAMES should has the same length as ObCSVOptionsEnum");

  enum class ObCSVCompression
  {
    INVALID = -1,
    NONE = 0,
    AUTO = 1,
    GZIP = 2,
    DEFLATE = 3,
    ZSTD = 4,
  };
  enum class ObCSVBinaryFormat {
    DEFAULT = 0,
    HEX = 1,
    BASE64 = 2
  };
  static constexpr const char *DEFAULT_FILE_EXTENSION = ".csv";
  common::ObString line_start_str_;
  common::ObString line_term_str_;
  common::ObString field_term_str_;
  int64_t field_escaped_char_;    // valid escaped char after stmt validation
  int64_t field_enclosed_char_;   // valid enclosed char after stmt validation
  common::ObCharsetType cs_type_; // charset type of format strings
  int64_t skip_header_lines_;
  bool skip_blank_lines_;
  bool ignore_extra_fields_;
  bool trim_space_;
  common::ObArrayWrap<common::ObString> null_if_;
  bool empty_field_as_null_;
  int64_t file_column_nums_;
  ObCSVCompression compression_algorithm_;
  bool is_optional_;
  common::ObString file_extension_;
  bool parse_header_;
  ObCSVBinaryFormat binary_format_;
  bool ignore_last_empty_col_;

  int init_format(const ObDataInFileStruct &format,
                  int64_t file_column_nums,
                  ObCollationType file_cs_type);
  int to_json_kv_string(char* buf, const int64_t buf_len, int64_t &pos, bool into_outfile = false) const;
  int load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator);

  TO_STRING_KV(K(cs_type_), K(file_column_nums_), K(line_start_str_), K(field_enclosed_char_),
               K(is_optional_), K(field_escaped_char_), K(field_term_str_), K(line_term_str_),
               K(compression_algorithm_), K(file_extension_), K(binary_format_), K(skip_blank_lines_), K(ignore_extra_fields_));
  OB_UNIS_VERSION(1);
};

struct ObParquetGeneralFormat {
  ObParquetGeneralFormat () :
    row_group_size_(256LL * 1024 * 1024), /* default 256 MB */
    compress_type_index_(0), /* default UNCOMPRESSED */
    column_index_type_(sql::ColumnIndexType::NAME)
  {}
  static constexpr const char *OPTION_NAMES[] = {
    "ROW_GROUP_SIZE",
    "COMPRESSION",
    "COLUMN_INDEX_TYPE"
  };
  static constexpr const char *COMPRESSION_ALGORITHMS[] = {
    "UNCOMPRESSED",
    "SNAPPY",
    "GZIP",
    "BROTLI",
    "ZSTD",
    "LZ4",
    "LZ4_FRAME",
    "LZO",
    "BZ2",
    "LZ4_HADOOP"
  };
  static constexpr const char *DEFAULT_FILE_EXTENSION = ".parquet";

  int64_t row_group_size_;
  int64_t compress_type_index_;
  sql::ColumnIndexType column_index_type_;
  int to_json_kv_string(char* buf, const int64_t buf_len, int64_t &pos) const;
  int load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator);
  TO_STRING_KV(K_(row_group_size), K_(compress_type_index), K_(column_index_type));
  OB_UNIS_VERSION(1);
};

struct ObOrcGeneralFormat {
  ObOrcGeneralFormat () :
    stripe_size_(64LL * 1024 * 1024),      /* default 64 MB */
    compress_type_index_(0),               /* default UNCOMPRESSED */
    compression_block_size_(256LL * 1024), /* default 256 KB */
    row_index_stride_(10000),
    column_use_bloom_filter_(),
    column_index_type_(sql::ColumnIndexType::NAME)
  {}
  static constexpr const char *OPTION_NAMES[] = {
    "STRIPE_SIZE",
    "COMPRESSION",
    "COMPRESSION_BLOCK_SIZE",
    "ROW_INDEX_STRIDE",
    "COLUMN_USE_BLOOM_FILTER",
    "COLUMN_INDEX_TYPE"
  };
  static constexpr const char *COMPRESSION_ALGORITHMS[] = {
    "UNCOMPRESSED",
    "ZLIB",
    "SNAPPY",
    "LZO",
    "LZ4",
    "ZSTD"
  };
  static constexpr const char *DEFAULT_FILE_EXTENSION = ".orc";

  int64_t stripe_size_;
  int64_t compress_type_index_;
  int64_t compression_block_size_;
  int64_t row_index_stride_;
  common::ObArrayWrap<int64_t> column_use_bloom_filter_;
  sql::ColumnIndexType column_index_type_;

  int to_json_kv_string(char* buf, const int64_t buf_len, int64_t &pos) const;
  int load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator);
  TO_STRING_KV(K(stripe_size_), K(compress_type_index_), K(compression_block_size_),
              K(row_index_stride_), K(column_use_bloom_filter_), K(column_index_type_));
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
      is_simple_format_(false),
      max_term_(0),
      min_term_(UINT32_MAX)
    {}
    char line_term_c_;
    char field_term_c_;
    bool is_filling_zero_to_empty_field_;
    bool is_line_term_by_counting_field_;
    bool is_same_escape_enclosed_;
    bool is_simple_format_;
    unsigned max_term_;
    unsigned min_term_;
  };
public:
  ObCSVGeneralParser() {}
  int init(const ObCSVGeneralFormat &format);

  int init(const ObDataInFileStruct &format,
           int64_t file_column_nums,
           common::ObCollationType file_cs_type);
  const ObCSVGeneralFormat &get_format() { return format_; }
  const OptParams &get_opt_params() { return opt_param_; }

  template<common::ObCharsetType cs_type, typename handle_func, bool HAS_ENCLOSED, bool SINGLE_CHAR_TERM, bool NEED_ESCAPED_RESULT = false>
  int scan_proto(const char *&str, const char *end, int64_t &nrows,
                 char *escape_buf, char *escaped_buf_end,
                 handle_func &handle_one_line,
                 common::ObIArray<LineErrRec> &errors,
                 bool is_end_file);
  // used for utf8, single char term, no enclosed char and no need escaped result
  template<typename handle_func, bool HAS_ESCAPE, bool HAS_LINE_START, bool SKIP_BLANK_LINES, bool NO_FIELD, bool IS_END_FILE, bool IGNORE_LAST_EMPTY_COLUMN>
  int scan_utf8_ex(const char *&str, const char *end, int64_t &nrows,
                   char *escape_buf, char *escaped_buf_end,
                   handle_func &handle_one_line,
                   common::ObIArray<LineErrRec> &errors);

  template<typename handle_func, bool HAS_ESCAPE>
  OB_INLINE int dispatch_scan_utf8_l1(bool HAS_LINE_START,
                            bool SKIP_BLANK_LINES,
                            bool NO_FIELD,
                            bool IGNORE_LAST_EMPTY_COLUMN,
                            const char *&str, const char *end, int64_t &nrows,
                            char *escape_buf, char *escaped_buf_end,
                            handle_func &handle_one_line,
                            common::ObIArray<LineErrRec> &errors,
                            bool is_end_file)
  {
    int ret = OB_SUCCESS;
    if (HAS_LINE_START) {
      ret = dispatch_scan_utf8_l2<handle_func, HAS_ESCAPE, true> (SKIP_BLANK_LINES, NO_FIELD,
                                                        IGNORE_LAST_EMPTY_COLUMN, str, end,
                                                        nrows, escape_buf, escaped_buf_end,
                                                        handle_one_line, errors, is_end_file);
    } else {
      ret = dispatch_scan_utf8_l2<handle_func, HAS_ESCAPE, false> (SKIP_BLANK_LINES, NO_FIELD,
                                                        IGNORE_LAST_EMPTY_COLUMN, str, end,
                                                        nrows, escape_buf, escaped_buf_end,
                                                        handle_one_line, errors, is_end_file);
    }
    return ret;
  }

  template<typename handle_func, bool HAS_ESCAPE, bool HAS_LINE_START>
  OB_INLINE int dispatch_scan_utf8_l2(bool SKIP_BLANK_LINES,
                            bool NO_FIELD,
                            bool IGNORE_LAST_EMPTY_COLUMN,
                            const char *&str, const char *end, int64_t &nrows,
                            char *escape_buf, char *escaped_buf_end,
                            handle_func &handle_one_line,
                            common::ObIArray<LineErrRec> &errors,
                            bool is_end_file)
  {
    int ret = OB_SUCCESS;
    if (SKIP_BLANK_LINES) {
      ret = dispatch_scan_utf8_l3<handle_func, HAS_ESCAPE, HAS_LINE_START, true> (NO_FIELD,
                                                                IGNORE_LAST_EMPTY_COLUMN, str, end,
                                                                nrows, escape_buf,
                                                                escaped_buf_end, handle_one_line,
                                                                errors, is_end_file);
    } else {
      ret = dispatch_scan_utf8_l3<handle_func, HAS_ESCAPE, HAS_LINE_START, false> (NO_FIELD,
                                                                IGNORE_LAST_EMPTY_COLUMN, str, end,
                                                                nrows, escape_buf,
                                                                escaped_buf_end, handle_one_line,
                                                                errors, is_end_file);
}
    return ret;
  }
  template<typename handle_func, bool HAS_ESCAPE, bool HAS_LINE_START, bool SKIP_BLANK_LINES>
  OB_INLINE int dispatch_scan_utf8_l3(bool NO_FIELD, bool IGNORE_LAST_EMPTY_COLUMN,
                            const char *&str, const char *end, int64_t &nrows,
                            char *escape_buf, char *escaped_buf_end,
                            handle_func &handle_one_line,
                            common::ObIArray<LineErrRec> &errors,
                            bool is_end_file)
  {
    int ret = OB_SUCCESS;
    if (NO_FIELD) {
      ret = dispatch_scan_utf8_l4<handle_func, HAS_ESCAPE, HAS_LINE_START, SKIP_BLANK_LINES, true>(
                                                          IGNORE_LAST_EMPTY_COLUMN, str, end, nrows,
                                                          escape_buf, escaped_buf_end,
                                                          handle_one_line, errors, is_end_file);
    } else {
      ret = dispatch_scan_utf8_l4<handle_func, HAS_ESCAPE, HAS_LINE_START, SKIP_BLANK_LINES, false>(
                                                          IGNORE_LAST_EMPTY_COLUMN, str, end, nrows,
                                                          escape_buf, escaped_buf_end,
                                                          handle_one_line, errors, is_end_file);
    }
    return ret;
  }

  template<typename handle_func, bool HAS_ESCAPE, bool HAS_LINE_START, bool SKIP_BLANK_LINES, bool NO_FIELD>
  OB_INLINE int dispatch_scan_utf8_l4(bool IGNORE_LAST_EMPTY_COLUMN,
                                      const char *&str, const char *end, int64_t &nrows,
                                      char *escape_buf, char *escaped_buf_end,
                                      handle_func &handle_one_line,
                                      common::ObIArray<LineErrRec> &errors,
                                      bool is_end_file)
  {
    int ret = OB_SUCCESS;
    if (is_end_file) {
      ret = dispatch_scan_utf8_l5<handle_func, HAS_ESCAPE, HAS_LINE_START, SKIP_BLANK_LINES, NO_FIELD, true>(
                                                        IGNORE_LAST_EMPTY_COLUMN,  str, end, nrows,
                                                        escape_buf, escaped_buf_end,
                                                        handle_one_line, errors);
    } else {
      ret = dispatch_scan_utf8_l5<handle_func, HAS_ESCAPE, HAS_LINE_START, SKIP_BLANK_LINES, NO_FIELD, false>(
                                                          IGNORE_LAST_EMPTY_COLUMN, str, end, nrows,
                                                          escape_buf, escaped_buf_end,
                                                          handle_one_line, errors);
    }
    return ret;
  }

  template<typename handle_func, bool HAS_ESCAPE, bool HAS_LINE_START, bool SKIP_BLANK_LINES, bool NO_FIELD, bool IS_END_FILE>
  OB_INLINE int dispatch_scan_utf8_l5(bool IGNORE_LAST_EMPTY_COLUMN,
                                      const char *&str, const char *end, int64_t &nrows,
                                      char *escape_buf, char *escaped_buf_end,
                                      handle_func &handle_one_line,
                                      common::ObIArray<LineErrRec> &errors)
  {
    int ret = OB_SUCCESS;
    if (IGNORE_LAST_EMPTY_COLUMN) {
      ret = scan_utf8_ex<handle_func, HAS_ESCAPE, HAS_LINE_START, SKIP_BLANK_LINES, NO_FIELD, IS_END_FILE, true>(
                                                                        str, end, nrows,
                                                                        escape_buf, escaped_buf_end,
                                                                        handle_one_line, errors);
    } else {
      ret = scan_utf8_ex<handle_func, HAS_ESCAPE, HAS_LINE_START, SKIP_BLANK_LINES, NO_FIELD, IS_END_FILE, false>(
                                                                        str, end, nrows,
                                                                        escape_buf, escaped_buf_end,
                                                                        handle_one_line, errors);
    }
    return ret;
  }

  template <bool SKIP_BLANK_LINES, bool IGNORE_LAST_EMPTY_COLUMN>
  OB_INLINE void process_term(const char *line_t, const char *&ori_field_begin, const char *&field_begin,
                              const char *&str, int &field_idx, bool &find_new_line)
  {
    while (OB_LIKELY(str < line_t)) {
      if (static_cast<unsigned> (*str) == opt_param_.field_term_c_
          && field_idx++ < format_.file_column_nums_) {
        gen_new_field(false, ori_field_begin, str, field_begin, str, field_idx);
        ori_field_begin = str + 1;
        field_begin = ori_field_begin;
      }
      ++str;
    }
    find_new_line = true;
    if (field_idx < format_.file_column_nums_) {
      if ((!SKIP_BLANK_LINES || field_idx > 0) &&
          (str > ori_field_begin || !IGNORE_LAST_EMPTY_COLUMN)) {
        ++field_idx;
        gen_new_field(false, ori_field_begin, str, field_begin, str, field_idx);
      } else {
        find_new_line = false;
      }
    }
    str++;
  }

  template<typename handle_func, bool NEED_ESCAPED_RESULT = false>
  int scan(const char *&str, const char *end, int64_t &nrows,
           char *escape_buf, char *escaped_buf_end,
           handle_func &handle_one_line,
           common::ObIArray<LineErrRec> &errors,
           bool is_end_file = false) {
    int ret = common::OB_SUCCESS;
    bool has_enclosed = (INT64_MAX != format_.field_enclosed_char_);
    bool single_char_term = static_cast<unsigned> (opt_param_.field_term_c_) < 0x80
                               && static_cast<unsigned> (opt_param_.line_term_c_) < 0x80
                               && format_.field_term_str_.length() == 1
                               && format_.line_term_str_.length() == 1;
    switch (format_.cs_type_) {
    case common::CHARSET_UTF8MB4:
      if (has_enclosed) {
        if (single_char_term) {
          ret = scan_proto<common::CHARSET_UTF8MB4, handle_func, true, true, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
        } else {
          ret = scan_proto<common::CHARSET_UTF8MB4, handle_func, true, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
        }
      } else {
        if (single_char_term) {
          const bool has_line_start = !format_.line_start_str_.empty();
          const bool skip_blank_lines = format_.skip_blank_lines_;
          const bool no_field = (0 == format_.file_column_nums_ && !skip_blank_lines);
          const bool ignore_last_empty_col = format_.ignore_last_empty_col_;
          if (opt_param_.is_line_term_by_counting_field_ || (!no_field && NEED_ESCAPED_RESULT)) {
            ret = scan_proto<common::CHARSET_UTF8MB4, handle_func, false, true, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
          } else {
            ret = dispatch_scan_utf8_l1<handle_func, NEED_ESCAPED_RESULT/*only process no field*/> (
                                                      has_line_start, skip_blank_lines, no_field,
                                                      ignore_last_empty_col, str, end, nrows,
                                                      escape_buf, escaped_buf_end,
                                                      handle_one_line, errors, is_end_file);
          }
        } else {
          ret = scan_proto<common::CHARSET_UTF8MB4, handle_func, false, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
        }
      }
      break;
    case common::CHARSET_GBK:
      if (has_enclosed) {
        ret = scan_proto<common::CHARSET_GBK, handle_func, true, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      } else {
        ret = scan_proto<common::CHARSET_GBK, handle_func, false, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      }
      break;
    case common::CHARSET_GB2312:
      ret = scan_proto<common::CHARSET_GB2312, handle_func, true, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    case common::CHARSET_UJIS:
      ret = scan_proto<common::CHARSET_UJIS, handle_func, true, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    case common::CHARSET_EUCKR:
      ret = scan_proto<common::CHARSET_EUCKR, handle_func, true, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    case common::CHARSET_EUCJPMS:
      ret = scan_proto<common::CHARSET_EUCJPMS, handle_func, true, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    case common::CHARSET_CP932:
      ret = scan_proto<common::CHARSET_CP932, handle_func, true, false, NEED_ESCAPED_RESULT>(
            str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      break;
    case common::CHARSET_GB18030:
    case common::CHARSET_GB18030_2022:
      if (has_enclosed) {
        ret = scan_proto<common::CHARSET_GB18030, handle_func, true, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      } else {
        ret = scan_proto<common::CHARSET_GB18030, handle_func, false, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      }
      break;
    case common::CHARSET_SJIS:
      if (has_enclosed) {
        ret = scan_proto<common::CHARSET_SJIS, handle_func, true, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      } else {
        ret = scan_proto<common::CHARSET_SJIS, handle_func, false, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      }
      break;
    case common::CHARSET_BIG5:
      if (has_enclosed) {
        ret = scan_proto<common::CHARSET_BIG5, handle_func, true, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      } else {
        ret = scan_proto<common::CHARSET_BIG5, handle_func, false, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      }
      break;
    case common::CHARSET_HKSCS:
    case common::CHARSET_HKSCS31:
      if (has_enclosed) {
        ret = scan_proto<common::CHARSET_HKSCS, handle_func, true, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      } else {
        ret = scan_proto<common::CHARSET_HKSCS, handle_func, false, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      }
      break;
    default:
      if (has_enclosed) {
        ret = scan_proto<common::CHARSET_BINARY, handle_func, true, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      } else {
        ret = scan_proto<common::CHARSET_BINARY, handle_func, false, false, NEED_ESCAPED_RESULT>(
              str, end, nrows, escape_buf, escaped_buf_end, handle_one_line, errors, is_end_file);
      }
      break;
    }
    return ret;
  }
  common::ObIArray<FieldValue>& get_fields_per_line() { return fields_per_line_; }

  struct HandleOneLineParam {
    HandleOneLineParam(common::ObIArray<FieldValue> &fields, int field_cnt)
      : fields_(fields), field_cnt_(field_cnt), is_file_end_(false) {}

    HandleOneLineParam(common::ObIArray<FieldValue> &fields, int field_cnt,
                      ObString line_data, bool is_file_end)
      : fields_(fields), field_cnt_(field_cnt), line_data_(line_data), is_file_end_(is_file_end) {}
    common::ObIArray<FieldValue> &fields_;
    int field_cnt_;
    ObString line_data_;
    bool is_file_end_;
  };

private:
  int init_opt_variables();

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


  OB_INLINE
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

  OB_INLINE
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
  template <bool HAS_ENCLOSED>
  OB_INLINE bool is_escape_next(const bool is_enclosed, const char cur, const char next) {
    // 1. the next char escaped by escape_char "A\tB" => A  B
    // 2. enclosed char escaped by another enclosed char in enclosed field. E.g. "A""B" => A"B
    return (format_.field_escaped_char_ == cur && !opt_param_.is_same_escape_enclosed_)
           || (HAS_ENCLOSED && is_enclosed && format_.field_enclosed_char_ == cur && format_.field_enclosed_char_ == next);
  }

protected:
  ObCSVGeneralFormat format_;
  common::ObSEArray<FieldValue, 1> fields_per_line_;
  OptParams opt_param_;
};


template<common::ObCharsetType cs_type, typename handle_func, bool HAS_ENCLOSED, bool SINGLE_CHAR_TERM, bool NEED_ESCAPED_RESULT>
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

      if (HAS_ENCLOSED && format_.field_enclosed_char_ == *str) {
        is_enclosed = true;
        str++;
      }
      while (str < end && !is_term) {
        const char *next = str + 1;
        if (next < end && is_escape_next<HAS_ENCLOSED>(is_enclosed, *str, *next)) {
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
        } else if (HAS_ENCLOSED && format_.field_enclosed_char_ == *str) {
          last_end_enclosed = str;
          str++;
        } else if (SINGLE_CHAR_TERM && (static_cast<unsigned> (*str) > opt_param_.max_term_
                                          || static_cast<unsigned> (*str) < opt_param_.min_term_)) {
          str++;
        } else {
          is_field_term = (*str == opt_param_.field_term_c_
              && (SINGLE_CHAR_TERM
                  || (str <= end - format_.field_term_str_.length()
                      && 0 == MEMCMP(str, format_.field_term_str_.ptr(), format_.field_term_str_.length()))));

          is_line_term = (*str == opt_param_.line_term_c_
              && (SINGLE_CHAR_TERM
                  || (str <= end - format_.line_term_str_.length()
                      && 0 == MEMCMP(str, format_.line_term_str_.ptr(), format_.line_term_str_.length()))));

          //if field is enclosed, there is a enclose char just before the terminate string
          is_term = (is_field_term || is_line_term)
                    && (!HAS_ENCLOSED || !is_enclosed || (str - 1 == last_end_enclosed));

          if (!is_term) {
            int mb_len = ob_charset_char_len<cs_type>((const unsigned char *)str, (const unsigned char*)end);
            if (mb_len < 0) {
              mb_len = 1;
            }
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
        if (is_field_term || ori_field_end > ori_field_begin ||
            (field_idx < format_.file_column_nums_
            && !format_.skip_blank_lines_
            && !format_.ignore_last_empty_col_)
        ) {
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
        if (field_idx < format_.file_column_nums_
            || (field_idx > format_.file_column_nums_ && !format_.ignore_extra_fields_)) {
          ret = handle_irregular_line(field_idx, line_no, errors);
        }
        if (OB_SUCC(ret)) {
          bool is_file_end = str + format_.line_term_str_.length() > end;
          ObString line_data = ObString(str - line_begin, line_begin);
          HandleOneLineParam param(fields_per_line_, field_idx, line_data, is_file_end && !find_new_line);
          ret = handle_one_line(param);
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

OB_INLINE bool is_valid_term(const char *begin, const char *term, const char escape) {
  int64_t escape_num = 0;
  const char *curr = term;
  while (curr > begin && OB_UNLIKELY(*(curr - 1) == escape)) {
    curr = curr - 1;
    ++escape_num;
  }
  return escape_num == 0 || (escape_num  % 2) == 0;
}

template<typename handle_func, bool HAS_ESCAPE, bool HAS_LINE_START, bool SKIP_BLANK_LINES, bool NO_FIELD, bool IS_END_FILE, bool IGNORE_LAST_EMPTY_COLUMN>
int ObCSVGeneralParser::scan_utf8_ex(const char *&str,
                                     const char *end,
                                     int64_t &nrows,
                                     char *escape_buf,
                                     char *escape_buf_end,
                                     handle_func &handle_one_line,
                                     common::ObIArray<LineErrRec> &errors)
{
  int ret = common::OB_SUCCESS;
  int line_no = 0;
  int blank_line_cnt = 0;
  const char *line_begin = str;
  char *escape_buf_pos = escape_buf;
  while (OB_SUCC(ret) && str < end && line_no - blank_line_cnt < nrows) {
    bool find_new_line = false;
    int field_idx = 0;
    if (HAS_LINE_START) {
      bool is_line_start_found = false;
      for (; str + format_.line_start_str_.length() <= end; str++) {
        if (0 == MEMCMP(str, format_.line_start_str_.ptr(), format_.line_start_str_.length())) {
          str += format_.line_start_str_.length();
          is_line_start_found = true;
          break;
        }
      }
      if (!is_line_start_found) {
        if (IS_END_FILE) {
          line_begin = end;
        } else {
          line_begin = str;
        }
        break;
      }
    }
    const char *ori_field_begin = str;
    const char *field_begin = str;
    const char *line_t = nullptr;
    line_t = static_cast<const char *> (memchr(str, opt_param_.line_term_c_, end - str));
    if (HAS_ESCAPE) {
      while (OB_LIKELY(line_t != nullptr)
            && OB_UNLIKELY(!is_valid_term(line_begin, line_t, format_.field_escaped_char_))
            && OB_LIKELY(++line_t < end)) {
        line_t = static_cast<const char *> (memchr(line_t, opt_param_.line_term_c_, end - line_t));
      }
    }
    if (NO_FIELD && OB_NOT_NULL(line_t)) {
      str = line_t + 1;
      find_new_line = true;
    } else if (OB_NOT_NULL(line_t)) {
      process_term<SKIP_BLANK_LINES, IGNORE_LAST_EMPTY_COLUMN> (line_t, ori_field_begin, field_begin,
                                      str, field_idx, find_new_line);
    } else if (IS_END_FILE) {
      line_t = end;
      process_term<SKIP_BLANK_LINES, IGNORE_LAST_EMPTY_COLUMN> (line_t, ori_field_begin, field_begin,
                                      str, field_idx, find_new_line);
    } else {
      str = end;
    }

    if (OB_LIKELY(find_new_line) || IS_END_FILE) {
      if (!SKIP_BLANK_LINES || field_idx > 0) {
        if (field_idx < format_.file_column_nums_
            || (field_idx > format_.file_column_nums_ && !format_.ignore_extra_fields_)) {
          ret = handle_irregular_line(field_idx, line_no, errors);
        }
        if (OB_SUCC(ret)) {
          bool is_file_end = line_t + format_.line_term_str_.length() > end;
          int32_t line_len = is_file_end ?  line_t - line_begin :
                                            line_t - line_begin + format_.line_term_str_.length();
          ObString line_data = ObString(line_len, line_begin);
          HandleOneLineParam param(fields_per_line_, field_idx, line_data, is_file_end);
          ret = handle_one_line(param);
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
  int to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const;
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

const char *compression_algorithm_to_string(ObCSVGeneralFormat::ObCSVCompression compression_algorithm);
int compression_algorithm_from_string(ObString compression_name,
                                      ObCSVGeneralFormat::ObCSVCompression &compression_algorithm);
const char *binary_format_to_string(const ObCSVGeneralFormat::ObCSVBinaryFormat binary_format);
int binary_format_from_string(const ObString binary_format_str,
                                        ObCSVGeneralFormat::ObCSVBinaryFormat &binary_format);
const char *column_index_type_to_string(const sql::ColumnIndexType column_index_type);
int column_index_type_from_string(const ObString column_index_type_str,
                                  sql::ColumnIndexType &column_index_type);
/**
 * guess compression format from filename suffix
 *
 * Return NONE if none of the known compression format matches.
 */
int compression_algorithm_from_suffix(ObString filename,
                                      ObCSVGeneralFormat::ObCSVCompression &compression_algorithm);

const char *compression_algorithm_to_suffix(ObCSVGeneralFormat::ObCSVCompression compression_algorithm);

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

  struct StringList {
    StringList(common::ObIAllocator &alloc) : allocator_(alloc), strs_(alloc) {}
    int store_strs(ObIArray<ObString> &strs);
    common::ObIAllocator &allocator_;
    common::ObFixedArray<common::ObString, common::ObIAllocator> strs_;
    TO_STRING_KV(K_(strs));
    OB_UNIS_VERSION(1);
  };

  enum FormatType {
    INVALID_FORMAT = -1,
    CSV_FORMAT,
    PARQUET_FORMAT,
    ODPS_FORMAT,
    ORC_FORMAT,
    PLUGIN_FORMAT,
    ICEBERG_FORMAT,
    HIVE_FORMAT,
    MAX_FORMAT
  };

  enum Options {
    OPT_REPLACE_INVALID_CHARACTERS = 1 << 0,
    OPT_BINARY_AS_TEXT = 1 << 1,
  };

  ObExternalFileFormat() : format_type_(INVALID_FORMAT) {}
  int to_string_with_alloc(common::ObString &str, common::ObIAllocator &allocator, bool into_outfile = false) const;
  int to_string(char* buf, const int64_t buf_len, int64_t &pos, bool into_outfile = false) const;
  int64_t to_string(char* buf, const int64_t buf_len, bool into_outfile = false) const;
  int load_from_string(const common::ObString &str, common::ObIAllocator &allocator);
  int mock_gen_column_def(const share::schema::ObColumnSchemaV2 &column, common::ObIAllocator &allocator, common::ObString &def);
  int get_format_file_extension(FormatType format_type, ObString &file_extension);

  static int parse_format_type(const common::ObString &str, common::ObIAllocator &allocator, FormatType &format_type);

  ObOriginFileFormat origin_file_format_str_;
  FormatType format_type_;
  sql::ObCSVGeneralFormat csv_format_;
  sql::ObParquetGeneralFormat parquet_format_;
  sql::ObODPSGeneralFormat odps_format_;
  sql::ObOrcGeneralFormat orc_format_;
  plugin::ObPluginFormat plugin_format_;
  uint64_t options_;
  static const char *FORMAT_TYPE_STR[];

private:
  int load_from_string_(const common::ObString &str, common::ObIAllocator &allocator, bool parse_format_type_only);
};


}
}

#endif //_OB_LOAD_DATA_PARSER_H_

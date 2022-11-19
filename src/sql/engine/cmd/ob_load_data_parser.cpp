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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

int ObCSVGeneralParser::init(const ObDataInFileStruct &format,
                             int64_t file_column_nums,
                             ObCollationType file_cs_type)
{
  int ret = OB_SUCCESS;
  const char INVALID_TERM_CHAR = '\xff';

  if (!ObCharset::is_valid_collation(file_cs_type)) {
    ret = OB_ERR_UNKNOWN_CHARSET;
    LOG_WARN("invalid charset", K(ret), K(file_cs_type));
  } else {
    format_.cs_type_ = ObCharset::charset_type_by_coll(file_cs_type);
    format_.file_column_nums_ = file_column_nums;
    format_.field_enclosed_char_ = format.field_enclosed_char_;
    format_.field_escaped_char_ = format.field_escaped_char_;
    format_.field_term_str_ = format.field_term_str_;
    format_.line_term_str_ = format.line_term_str_;
    format_.line_start_str_ = format.line_start_str_;
    if (format_.line_term_str_.empty() && !format_.field_term_str_.empty()) {
      format_.line_term_str_ = format_.field_term_str_;
    }
  }

  if (OB_SUCC(ret)) {
    opt_param_.line_term_c_ = format_.line_term_str_.empty() ? INVALID_TERM_CHAR : format_.line_term_str_[0];
    opt_param_.field_term_c_ = format_.field_term_str_.empty() ? INVALID_TERM_CHAR : format_.field_term_str_[0];
    opt_param_.is_filling_zero_to_empty_field_ = lib::is_mysql_mode();
    opt_param_.is_line_term_by_counting_field_ =
        0 == format_.line_term_str_.compare(format_.field_term_str_);
    opt_param_.is_same_escape_enclosed_ = (format_.field_enclosed_char_ == format_.field_escaped_char_);

    opt_param_.is_simple_format_ =
        !opt_param_.is_line_term_by_counting_field_
        && format_.field_term_str_.length() == 1
        && format_.line_term_str_.length() == 1
        && format_.line_start_str_.length() == 0
        && !opt_param_.is_same_escape_enclosed_
        && format_.field_enclosed_char_ == INT64_MAX;

  }

  if (OB_SUCC(ret) && OB_FAIL(fields_per_line_.prepare_allocate(file_column_nums))) {
    LOG_WARN("fail to allocate memory", K(ret), K(file_column_nums));
  }

  return ret;
}

int ObCSVGeneralParser::handle_irregular_line(int field_idx, int line_no,
                                              ObIArray<LineErrRec> &errors)
{
  int ret = OB_SUCCESS;
  LineErrRec rec;
  rec.err_code = field_idx > format_.file_column_nums_ ?
        OB_WARN_TOO_MANY_RECORDS : OB_WARN_TOO_FEW_RECORDS;
  rec.line_no = line_no;
  OX (errors.push_back(rec));
  for (int i = field_idx; i < format_.file_column_nums_; ++i) {
    FieldValue &new_field = fields_per_line_.at(i);
    new_field = FieldValue();
    new_field.is_null_ = 1;
  }
  return ret;
}

}
}

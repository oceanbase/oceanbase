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
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_hex_utils_base.h"
#include "deps/oblib/src/lib/list/ob_dlist.h"
#include "share/schema/ob_column_schema.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
const char INVALID_TERM_CHAR = '\xff';

const char * ObExternalFileFormat::FORMAT_TYPE_STR[] = {
  "CSV",
  "PARQUET",
};

static_assert(array_elements(ObExternalFileFormat::FORMAT_TYPE_STR) == ObExternalFileFormat::MAX_FORMAT,
              "Not enough initializer for ObExternalFileFormat");

int ObCSVGeneralFormat::init_format(const ObDataInFileStruct &format,
                                    int64_t file_column_nums,
                                    ObCollationType file_cs_type)
{
  int ret = OB_SUCCESS;

  if (!ObCharset::is_valid_collation(file_cs_type)) {
    ret = OB_ERR_UNKNOWN_CHARSET;
    LOG_WARN("invalid charset", K(ret), K(file_cs_type));
  } else {
    cs_type_ = ObCharset::charset_type_by_coll(file_cs_type);
    file_column_nums_ = file_column_nums;
    field_enclosed_char_ = format.field_enclosed_char_;
    field_escaped_char_ = format.field_escaped_char_;
    field_term_str_ = format.field_term_str_;
    line_term_str_ = format.line_term_str_;
    line_start_str_ = format.line_start_str_;
    if (line_term_str_.empty() && !field_term_str_.empty()) {
      line_term_str_ = field_term_str_;
    }
  }
  return ret;
}


int ObCSVGeneralParser::init(const ObDataInFileStruct &format,
                             int64_t file_column_nums,
                             ObCollationType file_cs_type)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(format_.init_format(format, file_column_nums, file_cs_type))) {
    LOG_WARN("fail to init format", K(ret));
  } else if (OB_FAIL(init_opt_variables())) {
    LOG_WARN("fail to init opt values", K(ret));
  }

  return ret;
}

int ObCSVGeneralParser::init(const ObCSVGeneralFormat &format)
{
  int ret = OB_SUCCESS;

  format_ = format;

  if (OB_FAIL(init_opt_variables())) {
    LOG_WARN("fail to init opt values", K(ret));
  }

  return ret;
}

int ObCSVGeneralParser::init_opt_variables()
{
  int ret = OB_SUCCESS;
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

  if (OB_SUCC(ret) && OB_FAIL(fields_per_line_.prepare_allocate(format_.file_column_nums_))) {
    LOG_WARN("fail to allocate memory", K(ret), K(format_.file_column_nums_));
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
  for (int i = field_idx; OB_SUCC(ret) && i < format_.file_column_nums_; ++i) {
    FieldValue &new_field = fields_per_line_.at(i);
    new_field = FieldValue();
    new_field.is_null_ = 1;
  }
  return ret;
}

int64_t ObCSVGeneralFormat::to_json_kv_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t idx = 0;
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(line_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(field_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], field_escaped_char_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], field_enclosed_char_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], ObCharset::charset_name(cs_type_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], skip_header_lines_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%s", OPTION_NAMES[idx++], STR_BOOL(skip_blank_lines_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%s", OPTION_NAMES[idx++], STR_BOOL(trim_space_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":", OPTION_NAMES[idx++]);
    J_ARRAY_START();
      for (int64_t i = 0; i < null_if_.count(); i++) {
        if (i != 0) {
          J_COMMA();
        }
        databuff_printf(buf, buf_len, pos, "\"%s\"", to_cstring(ObHexStringWrap(null_if_.at(i))));
      }
    J_ARRAY_END();
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%s", OPTION_NAMES[idx++], STR_BOOL(empty_field_as_null_));
  return pos;
}

int ObCSVGeneralFormat::load_from_json_data(json::Pair *&node, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_SUCC(ret) && OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      line_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      field_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    field_escaped_char_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    field_enclosed_char_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    cs_type_ = ObCharset::charset_type(node->value_->get_string());
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    skip_header_lines_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      skip_blank_lines_ = true;
    } else {
      skip_blank_lines_ = false;
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      trim_space_ = true;
    } else {
      trim_space_ = false;
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_ARRAY == node->value_->get_type()) {
    const json::Array &it_array = node->value_->get_array();
    int64_t idx = 0;
    if (it_array.get_size() > 0
        && OB_FAIL(null_if_.allocate_array(allocator, it_array.get_size()))) {
      LOG_WARN("allocate array failed", K(ret));
    }
    for (auto it_tmp = it_array.get_first();
         OB_SUCC(ret) && it_tmp != it_array.get_header() && it_tmp != NULL;
         it_tmp = it_tmp->get_next()) {
      if (OB_UNLIKELY(json::JT_STRING != it_tmp->get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null_if_ child is not string", K(ret), "type", it_tmp->get_type());
      } else {
        ObObj obj;
        OZ (ObHexUtilsBase::unhex(it_tmp->get_string(), allocator, obj));
        if (OB_SUCC(ret) && !obj.is_null()) {
          null_if_.at(idx++) = obj.get_string();
        }
      }
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      empty_field_as_null_ = true;
    } else {
      empty_field_as_null_ = false;
    }
    node = node->get_next();
  }
  return ret;
}

int64_t ObOriginFileFormat::to_json_kv_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t idx = 0;
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_line_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_field_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_field_escaped_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_field_enclosed_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_null_if_str_)));
  return pos;
}

int ObOriginFileFormat::load_from_json_data(json::Pair *&node, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_line_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_field_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_field_escaped_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_field_enclosed_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_null_if_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  return ret;
}

int64_t ObExternalFileFormat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  bool is_valid_format = format_type_ > INVALID_FORMAT && format_type_ < MAX_FORMAT;

  J_OBJ_START();

  databuff_print_kv(buf, buf_len, pos, "\"TYPE\"", is_valid_format ? FORMAT_TYPE_STR[format_type_] : "INVALID");

  switch (format_type_) {
    case CSV_FORMAT:
      pos += csv_format_.to_json_kv_string(buf + pos, buf_len - pos);
      pos += origin_file_format_str_.to_json_kv_string(buf + pos, buf_len - pos);
      break;
    default:
      pos += 0;
  }

  J_OBJ_END();
  return pos;
}

int ObExternalFileFormat::load_from_string(const ObString &str, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  json::Value *data = NULL;
  json::Parser parser;
  ObArenaAllocator temp_allocator;
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("format string is empty", K(ret), K(str));
  } else if (OB_FAIL(parser.init(&temp_allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(str.ptr(), str.length(), data))) {
    LOG_WARN("parse json failed", K(ret), K(str));
  } else if (NULL == data || json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json value", K(ret), KPC(data));
  } else {
    auto format_type_node = data->get_object().get_first();
    if (format_type_node->value_->get_type() != json::JT_STRING) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json format", K(ret), K(str));
    } else {
      ObString format_type_str = format_type_node->value_->get_string();
      for (int i = 0; i < array_elements(FORMAT_TYPE_STR); ++i) {
        if (format_type_str.case_compare(FORMAT_TYPE_STR[i]) == 0) {
          format_type_ = static_cast<FormatType>(i);
          break;
        }
      }
      format_type_node = format_type_node->get_next();
      switch (format_type_) {
        case CSV_FORMAT:
          OZ (csv_format_.load_from_json_data(format_type_node, allocator));
          OZ (origin_file_format_str_.load_from_json_data(format_type_node, allocator));
          break;
        case PARQUET_FORMAT:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid format type", K(ret), K(format_type_str));
          break;
      }
    }
  }
  return ret;
}

int ObExternalFileFormat::mock_gen_column_def(
    const share::schema::ObColumnSchemaV2 &column,
    ObIAllocator &allocator,
    ObString &def)
{
  int ret = OB_SUCCESS;
  ObSqlString temp_str;
  switch (format_type_) {
    case CSV_FORMAT: {
      uint64_t file_column_idx = column.get_column_id() - OB_APP_MIN_COLUMN_ID + 1;
      if (OB_FAIL(temp_str.append_fmt("%s%lu", N_EXTERNAL_FILE_COLUMN_PREFIX, file_column_idx))) {
        LOG_WARN("fail to append sql str", K(ret));
      }
      break;
    }
    case PARQUET_FORMAT: {
      if (OB_FAIL(temp_str.append_fmt("get_path(%s, '%.*s')",
                                      N_EXTERNAL_FILE_ROW,
                                      column.get_column_name_str().length(),
                                      column.get_column_name_str().ptr()))) {
        LOG_WARN("fail to append sql str", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format", K(ret), K(format_type_));
    }

  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, temp_str.string(), def))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }

  return ret;
}

int ObExternalFileFormat::StringData::store_str(const ObString &str)
{
  return ob_write_string(allocator_, str, str_);
}

OB_DEF_SERIALIZE(ObExternalFileFormat::StringData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, str_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExternalFileFormat::StringData)
{
  int ret = OB_SUCCESS;
  ObString temp_str;
  LST_DO_CODE(OB_UNIS_DECODE, temp_str);
  if (OB_SUCC(ret)) {
    ret = store_str(temp_str);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExternalFileFormat::StringData)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, str_);
  return len;
}

}
}

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

#define USING_LOG_PREFIX SQL_RESV

#include "ob_load_data_stmt.h"
namespace oceanbase {
namespace sql {
int64_t ObLoadArgument::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(load_file_storage), K_(is_default_charset), K_(ignore_rows), K_(dupl_action), K_(charset),K_(file_name), K_(access_info), K_(database_name), K_(table_name), K_(combined_name), K_(tenant_id),K_(database_id), K_(table_id), K_(is_csv_format), K_(full_file_path));
  J_OBJ_END();
  return pos;
}
int64_t ObDataInFileStruct::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(line_term_str), K_(line_start_str), K_(field_term_str), K_(field_escaped_str), K_(field_enclosed_str),K_(field_escaped_char), K_(field_enclosed_char), K_(is_opt_field_enclosed));
  J_OBJ_END();
  return pos;
}
int64_t ObLoadDataHint::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("Int Hint Item", common::ObArrayWrap<int64_t>(integer_values_, TOTAL_INT_ITEM), "String Hint Item",common::ObArrayWrap<ObString>(string_values_, TOTAL_STRING_ITEM));
  J_OBJ_END();
  return pos;
}
int64_t ObLoadDataStmt::FieldOrVarStruct::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(field_or_var_name), K_(column_id), K_(column_type), K_(is_table_column));
  J_OBJ_END();
  return pos;
}
int64_t ObLoadDataStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(load_args), K_(data_struct_in_file), K_(field_or_var_list),K_(assignments), K_(hints), K_(is_default_table_columns));
  J_OBJ_END();
  return pos;
}

const char* ObDataInFileStruct::DEFAULT_LINE_TERM_STR = "\n";
const char* ObDataInFileStruct::DEFAULT_LINE_BEGIN_STR = "";
const char* ObDataInFileStruct::DEFAULT_FIELD_TERM_STR = "\t";
const char* ObDataInFileStruct::DEFAULT_FIELD_ESCAPED_STR = "\\";
const char* ObDataInFileStruct::DEFAULT_FIELD_ENCLOSED_STR = "";
const int64_t ObDataInFileStruct::DEFAULT_FIELD_ESCAPED_CHAR = static_cast<int64_t>('\\');
const int64_t ObDataInFileStruct::DEFAULT_FIELD_ENCLOSED_CHAR = INT64_MAX;
const bool ObDataInFileStruct::DEFAULT_OPTIONAL_ENCLOSED = false;

int ObLoadDataStmt::add_column_item(ColumnItem& item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_items_.push_back(item))) {
    LOG_WARN("push column item failed", K(ret));
  }
  return ret;
}

ColumnItem* ObLoadDataStmt::get_column_item_by_idx(uint64_t column_id)
{
  ColumnItem* tar_item = NULL;
  for (int64_t i = 0; i < column_items_.count(); ++i) {
    ColumnItem& item = column_items_.at(i);
    if (item.column_id_ == column_id) {
      tar_item = &item;
      break;
    }
  }
  return tar_item;
}

int ObLoadDataHint::get_value(IntHintItem item, int64_t& value) const
{
  int ret = OB_SUCCESS;
  if (item >= TOTAL_INT_ITEM || item < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    value = integer_values_[item];
  }
  return ret;
}

int ObLoadDataHint::set_value(IntHintItem item, int64_t value)
{
  int ret = OB_SUCCESS;
  if (item >= TOTAL_INT_ITEM || item < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    integer_values_[item] = value;
  }
  return ret;
}

int ObLoadDataHint::get_value(StringHintItem item, ObString& value) const
{
  int ret = OB_SUCCESS;
  if (item >= TOTAL_STRING_ITEM || item < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    value = string_values_[item];
  }
  return ret;
}

int ObLoadDataHint::set_value(StringHintItem item, const ObString& value)
{
  int ret = OB_SUCCESS;
  if (item >= TOTAL_STRING_ITEM || item < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    string_values_[item] = value;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

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
namespace oceanbase
{
namespace sql
{

const char* ObDataInFileStruct::DEFAULT_LINE_TERM_STR = "\n";
const char* ObDataInFileStruct::DEFAULT_LINE_BEGIN_STR = "";
const char* ObDataInFileStruct::DEFAULT_FIELD_TERM_STR = "\t";
const char* ObDataInFileStruct::DEFAULT_FIELD_ESCAPED_STR = "\\";
const char* ObDataInFileStruct::DEFAULT_FIELD_ENCLOSED_STR = "";
const int64_t ObDataInFileStruct::DEFAULT_FIELD_ESCAPED_CHAR = static_cast<int64_t>('\\');
const int64_t ObDataInFileStruct::DEFAULT_FIELD_ENCLOSED_CHAR = INT64_MAX;
const bool ObDataInFileStruct::DEFAULT_OPTIONAL_ENCLOSED = false;

void ObLoadFileIterator::reset()
{
  files_.reset();
  pos_ = 0;
}

int ObLoadFileIterator::add_files(ObString *start, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(start) || OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(start), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(files_.push_back(start[i]))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObLoadFileIterator::get_next_file(ObString &file)
{
  int ret = OB_SUCCESS;
  if (pos_ >= files_.count()) {
    ret = OB_ITER_END;
  } else {
    file = files_.at(pos_++);
  }
  return ret;
}

int ObLoadFileIterator::copy(const ObLoadFileIterator &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(other));
  } else if (OB_FAIL(files_.assign(other.files_))) {
    LOG_WARN("fail to copy array", K(ret));
  }
  return ret;
}

int ObLoadDataStmt::add_column_item(ColumnItem &item) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_items_.push_back(item))) {
    LOG_WARN("push column item failed", K(ret));
  }
  return ret;
}

ColumnItem* ObLoadDataStmt::get_column_item_by_idx(uint64_t column_id) {
  ColumnItem* tar_item = NULL;
  for (int64_t i = 0; i < column_items_.count(); ++i) {
    ColumnItem &item = column_items_.at(i);
    if (item.column_id_ == column_id) {
      tar_item = &item;
      break;
    }
  }
  return tar_item;
}

int ObLoadDataHint::get_value(IntHintItem item, int64_t &value) const
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

int ObLoadDataHint::get_value(StringHintItem item, ObString &value) const
{
  int ret = OB_SUCCESS;
  if (item >= TOTAL_STRING_ITEM || item < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    value = string_values_[item];
  }
  return ret;
}

int ObLoadDataHint::set_value(StringHintItem item, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (item >= TOTAL_STRING_ITEM || item < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    string_values_[item] = value;
  }
  return ret;
}

}
}

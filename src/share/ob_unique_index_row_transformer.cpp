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

#define USING_LOG_PREFIX SHARE

#include "ob_unique_index_row_transformer.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

int ObUniqueIndexRowTransformer::check_need_shadow_columns(
    const common::ObNewRow &row,
    const common::ObCompatibilityMode sql_mode,
    const int64_t unique_key_cnt,
    const common::ObIArray<int64_t> *projector,
    bool &need_shadow_columns)
{
  int ret = OB_SUCCESS;
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || sql_mode > ORACLE_MODE
      || unique_key_cnt <= 0 || unique_key_cnt > row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(row), K(sql_mode), K(unique_key_cnt));
  } else {
    if (is_mysql_compatible(sql_mode)) {
      if (OB_FAIL(check_mysql_need_shadow_columns(row, unique_key_cnt, projector, need_shadow_columns))) {
        LOG_WARN("fail to check mysql need shadow columns", K(ret));
      }
    } else {
      if (OB_FAIL(check_oracle_need_shadow_columns(row, unique_key_cnt, projector, need_shadow_columns))) {
        LOG_WARN("fail to check oracle need shadow columns", K(ret));
      }
    }
  }
  return ret;
}

int ObUniqueIndexRowTransformer::check_oracle_need_shadow_columns(
    const common::ObNewRow &row,
    const int64_t unique_key_cnt,
    const common::ObIArray<int64_t> *projector,
    bool &need_shadow_columns)
{
  int ret = OB_SUCCESS;
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || unique_key_cnt <= 0 || unique_key_cnt > row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(row), K(unique_key_cnt));
  } else {
    bool is_rowkey_all_null = true;
    // oracle compatible: when all columns of unique key is null, fill the shadow columns
    for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_cnt && is_rowkey_all_null; ++i) {
      const int64_t idx = nullptr == projector ? i : projector->at(i);
      if (idx >= row.count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, idx exceed the row cells count", K(ret), K(idx), K(row));
      } else {
        is_rowkey_all_null = row.cells_[idx].is_null();
      }
    }
    need_shadow_columns = is_rowkey_all_null;
  }
  return ret;
}

int ObUniqueIndexRowTransformer::check_mysql_need_shadow_columns(
    const common::ObNewRow &row,
    const int64_t unique_key_cnt,
    const common::ObIArray<int64_t> *projector,
    bool &need_shadow_columns)
{
  int ret = OB_SUCCESS;
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || unique_key_cnt <= 0 || unique_key_cnt > row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(row), K(unique_key_cnt));
  } else {
    bool rowkey_has_null = false;
    // mysql compatible: when at least one column of unique key is null, fill the shadow columns
    for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_cnt && !rowkey_has_null; ++i) {
      const int64_t idx = NULL == projector ? i : projector->at(i);
      if (idx >= row.count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, idx exceed the row cells count", K(ret), K(idx), K(row));
      } else {
        rowkey_has_null = row.cells_[idx].is_null();
      }
    }
    need_shadow_columns = rowkey_has_null;
  }
  return ret;
}

int ObUniqueIndexRowTransformer::convert_to_unique_index_row(
    const common::ObNewRow &row,
    const common::ObCompatibilityMode sql_mode,
    const int64_t unique_key_cnt,
    const int64_t shadow_column_cnt,
    const ObIArray<int64_t> *projector,
    bool &need_shadow_columns,
    common::ObNewRow &result_row,
    const bool need_copy_cell)
{
  int ret = OB_SUCCESS;
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || sql_mode > ORACLE_MODE || unique_key_cnt <= 0
        || shadow_column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(row), K(sql_mode), K(unique_key_cnt), K(shadow_column_cnt));
  } else if (OB_FAIL(check_need_shadow_columns(row, sql_mode, unique_key_cnt, projector, need_shadow_columns))) {
    LOG_WARN("fail to check need shadow columns", K(ret));
  } else {
    // 1. fill the unique key columns
    // 2. fill shadow columns on demand
    // 3. fill other columns
    for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_cnt && need_copy_cell; ++i) {
      const int64_t idx = NULL == projector ? i : projector->at(i);
      if (idx >= row.count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, idx is not valid", K(idx), K(row));
      } else {
        result_row.cells_[i] = row.get_cell(idx);
      }
    }
    for (int64_t i = unique_key_cnt; OB_SUCC(ret) && i < unique_key_cnt + shadow_column_cnt; ++i) {
      const int64_t idx = NULL == projector ? i : projector->at(i);
      if (idx >= row.count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, idx is not valid", K(idx), K(row));
      } else {
        if (need_shadow_columns) {
          if (need_copy_cell) {
            result_row.cells_[i] = row.get_cell(idx);
          }
        } else {
          result_row.cells_[i].set_null();
        }
      }
    }
    const int64_t output_cell_cnt = NULL == projector ? row.count_ : projector->count();
    for (int64_t i = unique_key_cnt + shadow_column_cnt; OB_SUCC(ret) && i < output_cell_cnt; ++i) {
      const int64_t idx = NULL == projector ? i : projector->at(i);
      if (idx >= row.count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, idx is not valid", K(idx), K(row));
      } else {
        result_row.cells_[i] = row.get_cell(idx);
      }
    }
    result_row.count_ = output_cell_cnt;
  }
  return ret;
}

int ObUniqueIndexRowTransformer::convert_to_unique_index_row(
    const common::ObNewRow &row,
    const common::ObCompatibilityMode sql_mode,
    const int64_t unique_key_cnt,
    const int64_t shadow_column_cnt,
    const ObIArray<int64_t> *projector,
    common::ObNewRow &result_row,
    const bool need_copy_cell)
{
  int ret = OB_SUCCESS;
  bool need_shadow_columns = false;
  if (OB_FAIL(convert_to_unique_index_row(row, sql_mode, unique_key_cnt, shadow_column_cnt, projector, need_shadow_columns, result_row, need_copy_cell))) {
    LOG_WARN("fail to convert to unique index row", K(ret));
  }
  return ret;
}

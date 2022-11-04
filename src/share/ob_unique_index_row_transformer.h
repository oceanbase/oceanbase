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

#ifndef OB_UNIQUE_INDEX_ROW_TRANSFORMER_H_
#define OB_UNIQUE_INDEX_ROW_TRANSFORMER_H_

#include "common/row/ob_row.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace share
{

// TODO(cangdi): remove this after origin global index builder code is removed
class ObUniqueIndexRowTransformer
{
public:
  static int check_need_shadow_columns(
      const common::ObNewRow &row,
      const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt,
      const common::ObIArray<int64_t> *projector,
      bool &need_shadow_columns);
  static int convert_to_unique_index_row(
      const common::ObNewRow &row,
      const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt,
      const int64_t shadow_column_cnt,
      const common::ObIArray<int64_t> *projector,
      common::ObNewRow &result_row,
      const bool need_copy_cell = false);
  static int convert_to_unique_index_row(
      const common::ObNewRow &row,
      const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt,
      const int64_t shadow_column_cnt,
      const common::ObIArray<int64_t> *projector,
      bool &need_shadow_columns,
      common::ObNewRow &result_row,
      const bool need_copy_cell = false);
private:
  static int check_oracle_need_shadow_columns(
      const common::ObNewRow &row,
      const int64_t unique_key_cnt,
      const common::ObIArray<int64_t> *projector,
      bool &need_shadow_columns);
  static int check_mysql_need_shadow_columns(
      const common::ObNewRow &row,
      const int64_t unique_key_cnt,
      const common::ObIArray<int64_t> *projector,
      bool &need_shadow_columns);
};

template<typename T>
class ObUniqueIndexRowTransformerV2
{
public:
  static int check_need_shadow_columns(
      const T &row,
      const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt,
      const common::ObIArray<int64_t> *projector,
      bool &need_shadow_columns);
  static int convert_to_unique_index_row(
      const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt,
      const int64_t shadow_column_cnt,
      const common::ObIArray<int64_t> *projector,
      T &row,
      bool &need_shadow_columns);
private:
  static int check_oracle_need_shadow_columns(
      const T &row,
      const int64_t unique_key_cnt,
      const common::ObIArray<int64_t> *projector,
      bool &need_shadow_columns);
  static int check_mysql_need_shadow_columns(
      const T &row,
      const int64_t unique_key_cnt,
      const common::ObIArray<int64_t> *projector,
      bool &need_shadow_columns);
};

template<typename T>
int ObUniqueIndexRowTransformerV2<T>::check_need_shadow_columns(
    const T &row,
    const common::ObCompatibilityMode sql_mode,
    const int64_t unique_key_cnt,
    const common::ObIArray<int64_t> *projector,
    bool &need_shadow_columns)
{
  int ret = common::OB_SUCCESS;
  const int64_t cell_cnt = row.get_count();
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || sql_mode > common::ORACLE_MODE
      || unique_key_cnt <= 0 || unique_key_cnt > cell_cnt)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(row), K(sql_mode), K(unique_key_cnt));
  } else {
    if (common::is_mysql_compatible(sql_mode)) {
      if (OB_FAIL(check_mysql_need_shadow_columns(row, unique_key_cnt, projector, need_shadow_columns))) {
        SHARE_LOG(WARN, "fail to check mysql need shadow columns", K(ret));
      }
    } else {
      if (OB_FAIL(check_oracle_need_shadow_columns(row, unique_key_cnt, projector, need_shadow_columns))) {
        SHARE_LOG(WARN, "fail to check oracle need shadow columns", K(ret));
      }
    }
  }
  return ret;
}

template<typename T>
int ObUniqueIndexRowTransformerV2<T>::check_oracle_need_shadow_columns(
    const T &row,
    const int64_t unique_key_cnt,
    const common::ObIArray<int64_t> *projector,
    bool &need_shadow_columns)
{
  int ret = common::OB_SUCCESS;
  const int64_t cell_cnt = row.get_count();
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || unique_key_cnt <= 0 || unique_key_cnt > cell_cnt)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(row), K(unique_key_cnt));
  } else {
    bool is_rowkey_all_null = true;
    // oracle compatible: when all columns of unique key is null, fill the shadow columns
    for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_cnt && is_rowkey_all_null; ++i) {
      const int64_t idx = nullptr == projector ? i : projector->at(i);
      if (idx >= cell_cnt) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "error unexpected, idx exceed the row cells count", K(ret), K(idx), K(row));
      } else {
        is_rowkey_all_null = row.get_cell(idx).is_null();
      }
    }
    need_shadow_columns = is_rowkey_all_null;
  }
  return ret;
}

template<typename T>
int ObUniqueIndexRowTransformerV2<T>::check_mysql_need_shadow_columns(
    const T &row,
    const int64_t unique_key_cnt,
    const common::ObIArray<int64_t> *projector,
    bool &need_shadow_columns)
{
  int ret = common::OB_SUCCESS;
  const int64_t cell_cnt = row.get_count();
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || unique_key_cnt <= 0 || unique_key_cnt > cell_cnt)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(row), K(unique_key_cnt));
  } else {
    bool rowkey_has_null = false;
    // mysql compatible: when at least one column of unique key is null, fill the shadow columns
    for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_cnt && !rowkey_has_null; ++i) {
      const int64_t idx = NULL == projector ? i : projector->at(i);
      if (idx >= cell_cnt) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "error unexpected, idx exceed the row cells count", K(ret), K(idx), K(row));
      } else {
        rowkey_has_null = row.get_cell(idx).is_null();
      }
    }
    need_shadow_columns = rowkey_has_null;
  }
  return ret;
}

template <typename T>
int ObUniqueIndexRowTransformerV2<T>::convert_to_unique_index_row(
    const common::ObCompatibilityMode sql_mode,
    const int64_t unique_key_cnt,
    const int64_t shadow_column_cnt,
    const common::ObIArray<int64_t> *projector,
    T &row,
    bool &need_shadow_columns)
{
  int ret = common::OB_SUCCESS;
  need_shadow_columns = false;
  if (OB_UNLIKELY(!row.is_valid() || sql_mode > common::ORACLE_MODE || unique_key_cnt <= 0
      || shadow_column_cnt <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), K(row), K(sql_mode), K(unique_key_cnt), K(shadow_column_cnt));
  } else if (OB_FAIL(check_need_shadow_columns(row, sql_mode, unique_key_cnt, projector, need_shadow_columns))) {
    SHARE_LOG(WARN, "fail to check need shadow columns", K(ret));
  } else {
    const int64_t cell_cnt = row.get_count();
    for (int64_t i = unique_key_cnt; OB_SUCC(ret) && i < unique_key_cnt + shadow_column_cnt; ++i) {
      const int64_t idx = NULL == projector ? i : projector->at(i);
      if (idx >= cell_cnt) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "error unexpected, idx is not valid", K(idx), K(row));
      } else {
        if (!need_shadow_columns) {
          row.get_cell(i).set_null();
        }
      }
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_UNIQUE_INDEX_ROW_TRANSFORMER_H_

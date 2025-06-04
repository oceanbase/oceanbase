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
#include "share/vector/ob_continuous_base.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "share/vector/ob_continuous_format.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace common
{
int ObContinuousBase::to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                              const uint16_t selector[], const int64_t size,
                              const int64_t col_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!is_collection_expr())) {
    for (int64_t i = 0; i < size; i++) {
      int64_t row_idx = selector[i];
      if (nulls_->at(row_idx)) {
        stored_rows[i]->set_null(row_meta, col_idx);
      } else {
        stored_rows[i]->set_cell_payload(row_meta, col_idx, data_ + offsets_[row_idx],
                                         get_length(row_idx));
      }
    }
  } else {
    ret = sql::ObCollectionExprUtil::write_collections_to_rows(
      static_cast<const ObContinuousFormat *>(this), row_meta, stored_rows, selector, size,
      col_idx);
  }
  return ret;
}

int ObContinuousBase::to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                              const int64_t size, const int64_t col_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!is_collection_expr())) {
    for (int i = 0; i < size; i++) {
      if (nulls_->at(i)) {
        stored_rows[i]->set_null(row_meta, col_idx);
      } else {
        stored_rows[i]->set_cell_payload(row_meta, col_idx, data_ + offsets_[i], get_length(i));
      }
    }
  } else {
    ret = sql::ObCollectionExprUtil::write_collections_to_rows(
      static_cast<const ObContinuousFormat *>(this), row_meta, stored_rows, size, col_idx);
  }
  return ret;
}
}
}
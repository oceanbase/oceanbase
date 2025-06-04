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
#include "share/vector/ob_discrete_base.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "share/vector/ob_discrete_format.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace common
{
int ObDiscreteBase::to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
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
        stored_rows[i]->set_cell_payload(row_meta, col_idx, ptrs_[row_idx], lens_[row_idx]);
      }
    }
  } else {
    ret = sql::ObCollectionExprUtil::write_collections_to_rows(
      static_cast<const ObDiscreteFormat *>(this), row_meta, stored_rows, selector, size, col_idx);
  }
  return ret;
}

int ObDiscreteBase::to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                             const int64_t size, const int64_t col_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!is_collection_expr())) {
    for (int64_t row_idx = 0; row_idx < size; row_idx++) {
      if (nulls_->at(row_idx)) {
        stored_rows[row_idx]->set_null(row_meta, col_idx);
      } else {
        stored_rows[row_idx]->set_cell_payload(row_meta, col_idx, ptrs_[row_idx], lens_[row_idx]);
      }
    }
  } else {
    ret = sql::ObCollectionExprUtil::write_collections_to_rows(
      static_cast<const ObDiscreteFormat *>(this), row_meta, stored_rows, size, col_idx);
  }
  return ret;
}

  DEF_TO_STRING(ObDiscreteBase)
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(has_null), K_(is_batch_ascii), K_(max_row_cnt));
    J_OBJ_END();
    return pos;
  }
}
}
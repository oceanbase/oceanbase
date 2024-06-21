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

namespace oceanbase
{
namespace common
{
  void ObContinuousBase::to_rows(const sql::RowMeta &row_meta,
                                 sql::ObCompactRow **stored_rows,
                                 const uint16_t selector[],
                                 const int64_t size,
                                 const int64_t col_idx) const
  {
    for (int64_t i = 0; i < size; i++) {
      int64_t row_idx = selector[i];
      if (nulls_->at(row_idx)) {
        stored_rows[i]->set_null(row_meta, col_idx);
      } else {
        stored_rows[i]->set_cell_payload(row_meta, col_idx,
                                         data_ + offsets_[row_idx],
                                         get_length(row_idx));
      }
    }
  }

  void ObContinuousBase::to_rows(const sql::RowMeta &row_meta,
                                            sql::ObCompactRow **stored_rows,
                                            const int64_t size,
                                            const int64_t col_idx) const
  {
    for (int i = 0; i < size; i++) {
      if (nulls_->at(i)) {
        stored_rows[i]->set_null(row_meta, col_idx);
      } else {
        stored_rows[i]->set_cell_payload(row_meta, col_idx, data_ + offsets_[i], get_length(i));
      }
    }
  }
}
}
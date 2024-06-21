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
#include "share/vector/ob_uniform_base.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace common
{
  void ObUniformBase::to_rows(const sql::RowMeta &row_meta,
                              sql::ObCompactRow **stored_rows,
                              const uint16_t selector[],
                              const int64_t size,
                              const int64_t col_idx) const
  {
    if (get_format() == VEC_UNIFORM) {
      for (int64_t i = 0; i < size; i++) {
        int64_t row_idx = selector[i];
        if (datums_[row_idx].is_null()) {
          stored_rows[i]->set_null(row_meta, col_idx);
        } else {
          stored_rows[i]->set_cell_payload(row_meta, col_idx,
                                          datums_[row_idx].ptr_,
                                          datums_[row_idx].len_);
        }
      }
    } else {
      int64_t row_idx = 0;
      for (int64_t i = 0; i < size; i++) {
        if (datums_[row_idx].is_null()) {
          stored_rows[i]->set_null(row_meta, col_idx);
        } else {
          stored_rows[i]->set_cell_payload(row_meta, col_idx,
                                          datums_[row_idx].ptr_,
                                          datums_[row_idx].len_);
        }
      }
    }
  }

  void ObUniformBase::to_rows(const sql::RowMeta &row_meta, sql::ObCompactRow **stored_rows,
                              const int64_t size, const int64_t col_idx) const
  {
    if (get_format() == VEC_UNIFORM) {
      for (int64_t row_idx = 0; row_idx < size; row_idx++) {
        if (datums_[row_idx].is_null()) {
          stored_rows[row_idx]->set_null(row_meta, col_idx);
        } else {
          stored_rows[row_idx]->set_cell_payload(row_meta, col_idx, datums_[row_idx].ptr_,
                                                 datums_[row_idx].len_);
        }
      }
    } else {
      for (int64_t i = 0, row_idx = 0; i < size; i++) {
        if (datums_[row_idx].is_null()) {
          stored_rows[i]->set_null(row_meta, col_idx);
        } else {
          stored_rows[i]->set_cell_payload(row_meta, col_idx, datums_[row_idx].ptr_,
                                           datums_[row_idx].len_);
        }
      }
    }
  }

  DEF_TO_STRING(ObUniformBase)
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(max_row_cnt));
    BUF_PRINTF(", eval_info: ");
    pos += eval_info_->to_string(buf + pos, buf_len - pos);
    J_OBJ_END();
    return pos;
  }

}
}
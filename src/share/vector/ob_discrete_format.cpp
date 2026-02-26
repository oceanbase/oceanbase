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

#include "ob_discrete_format.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace common
{
using namespace sql;
using ATTR0_FMT = ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>;

void ObDiscreteFormat::set_collection_payload_shallow(const int64_t idx, const void *payload, const ObLength length)
{
  if (ObCollectionExprUtil::is_compact_fmt_cell(payload)) {
    set_has_compact_collection();
  }
}

int ObDiscreteFormat::write_collection_to_row(const sql::RowMeta &row_meta,
                                              sql::ObCompactRow *stored_row, const uint64_t row_idx,
                                              const int64_t col_idx) const
{
  return ObCollectionExprUtil::write_collection_to_row(this, row_meta, stored_row, row_idx, col_idx);
}

int ObDiscreteFormat::write_collection_to_row(const sql::RowMeta &row_meta,
                                              sql::ObCompactRow *stored_row, const uint64_t row_idx,
                                              const int64_t col_idx, const int64_t remain_size,
                                              const bool is_fixed_length_data, int64_t &row_size) const
{
  return ObCollectionExprUtil::write_collection_to_row(this, row_meta, stored_row, row_idx, col_idx,
                                                       remain_size, is_fixed_length_data, row_size);
}
} // end common
} // end oceanbase
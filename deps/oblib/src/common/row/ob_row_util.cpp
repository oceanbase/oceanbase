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

#define USING_LOG_PREFIX COMMON

#include "common/row/ob_row_util.h"
#include "common/cell/ob_cell_reader.h"

namespace oceanbase
{
namespace common
{

int ObRowUtil::convert(const ObString &compact_row, ObNewRow &row)
{
  return convert(compact_row.ptr(), compact_row.length(), row);
}

int ObRowUtil::convert(const char *compact_row, int64_t buf_len, ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ObCellReader cell_reader;
  bool is_row_finished = false;
  uint64_t column_id = OB_INVALID_ID;
  int64_t cell_idx = 0;
  ObObj cell;
  if (OB_FAIL(cell_reader.init(compact_row, buf_len, DENSE))) {
    LOG_WARN("fail to init cell reader", K(ret));
  }
  while (OB_SUCC(ret) && !is_row_finished && OB_SUCC(cell_reader.next_cell())) {
    if (OB_FAIL(cell_reader.get_cell(column_id, cell, &is_row_finished))) {
      LOG_WARN("failed to get cell", K(column_id), K(ret));
    } else if (is_row_finished) {
      ret = OB_SUCCESS;
    } else if (cell_idx < row.count_) {
      row.cells_[cell_idx++] = cell;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("accept row column count is not enough", K(ret), K(cell_idx), K_(row.count));
    }
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase

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

#include "observer/virtual_table/ob_all_virtual_engine_table.h"

namespace oceanbase
{
using namespace common;

namespace observer
{

ObAllVirtualEngineTable::ObAllVirtualEngineTable() :
    ObVirtualTableScannerIterator()
{
}

ObAllVirtualEngineTable::~ObAllVirtualEngineTable()
{
}

int ObAllVirtualEngineTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else {
    if (!start_to_read_) {
      ObObj *cells = NULL;
      const int64_t col_count = output_column_ids_.count();
      if (OB_ISNULL(cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
      } else if (OB_UNLIKELY(col_count < 0 || col_count > ENGINE_COLUMN_COUNT)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
      } else if (OB_UNLIKELY(col_count > reserved_column_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cells count error", K(ret), K(col_count),
                   K(reserved_column_cnt_));
      } else {
        uint64_t cell_idx = 0;
        for (int64_t k = 0; OB_SUCC(ret) && k < col_count; ++k) {
          uint64_t col_id = output_column_ids_.at(k);
          switch (col_id) {
          case ENGINE: {
              cells[cell_idx].set_varchar("OceanBase");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
              break;
            }
          case SUPPORT: {
              cells[cell_idx].set_varchar("YES");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
              break;
            }
          case COMMENT: {
              cells[cell_idx].set_varchar("Supports transactions");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
              break;
            }
          case TRANSACTIONS: {
              cells[cell_idx].set_varchar("YES");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
              break;
            }
          case XA: {
              cells[cell_idx].set_varchar("NO");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
              break;
            }
          case SAVEPOINTS: {
              cells[cell_idx].set_varchar("NO");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
              break;
            }
          default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                K(output_column_ids_), K(col_id));
              break;
            }
        }
          if (OB_SUCC(ret)) {
            ++cell_idx;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(scanner_.add_row(cur_row_))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
          }
        }
        if (OB_SUCC(ret)) {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }
    }

    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }

  return ret;
}
} // namespace observer
} // namespace oceanbase

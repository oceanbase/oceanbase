/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_datum_row_utils.h"
namespace oceanbase
{
namespace blocksstable
{

int ObDatumRowUtils::ob_create_row(ObIAllocator &allocator, int64_t col_count, ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row_count is invalid", K(ret), K(col_count));
  } else {
    void *row_buf = NULL;
    if (OB_ISNULL(row_buf = allocator.alloc(sizeof(blocksstable::ObDatumRow)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed", K(ret), K(sizeof(blocksstable::ObDatumRow)));
    } else if (FALSE_IT(datum_row = new(row_buf) blocksstable::ObDatumRow())) {
    } else if (OB_FAIL(datum_row->init(allocator, col_count))) {
      LOG_WARN("fail to init datum row", K(ret), K(col_count));
    }
    if (OB_FAIL(ret) && nullptr != datum_row) {
      datum_row->~ObDatumRow();
      datum_row = nullptr;
      allocator.free(row_buf);
    }
  }
  return ret;
}

int ObDatumRowUtils::ob_create_rows(ObIAllocator &allocator, int64_t row_count, int64_t col_count, ObDatumRow *&datum_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_count <= 0 || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("col count or row count is invalid", K(ret), K(col_count), K(row_count));
  } else {
    void *rows_buf = nullptr;
    const size_t rows_buf_len = sizeof(blocksstable::ObDatumRow) * row_count;
    if (OB_ISNULL(rows_buf = allocator.alloc(rows_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate row buffer", K(ret), K(rows_buf_len));
    } else {
      char *row_buf = static_cast<char*>(rows_buf);
      datum_rows = new(row_buf) blocksstable::ObDatumRow[row_count]();
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < row_count; ++i) {
        if (OB_FAIL(datum_rows[i].init(allocator, col_count))) {
          LOG_WARN("fail to init datum row", K(ret), K(row_count), K(col_count), K(i), K(datum_rows[i]));
        }
      }
      if (OB_FAIL(ret)) {
        // release storage_datums
        for (int64_t i = 0; i < row_count; ++i) {
          datum_rows[i].~ObDatumRow();
        }
        allocator.free(rows_buf);
      }
    }
  }
  return ret;
}

int ObDatumRowUtils::ob_create_rows_shallow_copy(ObIAllocator &allocator, const ObIArray<ObDatumRow *> &src_rows, ObDatumRow *&datum_rows)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = src_rows.count();
  if (row_count <= 0) {
    datum_rows = nullptr;
  } else {
    void *rows_buf = nullptr;
    const size_t rows_buf_len = sizeof(blocksstable::ObDatumRow) * row_count;
    if (OB_ISNULL(rows_buf = allocator.alloc(rows_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate row buffer", K(ret), K(rows_buf_len));
    } else {
      char *row_buf = static_cast<char*>(rows_buf);
      datum_rows = new(row_buf) blocksstable::ObDatumRow[row_count]();
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
        if (OB_FAIL(datum_rows[i].shallow_copy(*src_rows.at(i)))) {
          LOG_WARN("fail to init datum row", K(ret), KPC(src_rows.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObDatumRowUtils::ob_create_rows_shallow_copy(ObIAllocator &allocator,
                                                 const ObDatumRow *src_rows,
                                                 const ObIArray<int64_t> &dst_row_ids,
                                                 ObDatumRow *&dst_rows)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = dst_row_ids.count();
  if (row_count <= 0) {
    dst_rows = nullptr;
  } else {
    void *rows_buf = nullptr;
    const size_t rows_buf_len = sizeof(blocksstable::ObDatumRow) * row_count;
    if (OB_ISNULL(rows_buf = allocator.alloc(rows_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate row buffer", K(ret), K(rows_buf_len));
    } else {
      char *row_buf = static_cast<char*>(rows_buf);
      dst_rows = new(row_buf) blocksstable::ObDatumRow[row_count]();
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
        if (OB_FAIL(dst_rows[i].shallow_copy(src_rows[dst_row_ids.at(i)]))) {
          LOG_WARN("fail to init datum row", K(ret), K(i), K(src_rows[dst_row_ids.at(i)]));
        }
      }
    }
  }
  return ret;
}

int ObDatumRowUtils::prepare_rowkey(
    const ObDatumRow &datum_row,
    const int key_datum_cnt, 
    const ObColDescIArray &col_descs,
    common::ObIAllocator &allocator,
    ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (!datum_row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid datum row", K(ret), K(datum_row));
  } else if (OB_FAIL(rowkey.assign(datum_row.storage_datums_, key_datum_cnt))) {
    LOG_WARN("failed to assign datum rowkey", K(ret), K(datum_row), K(key_datum_cnt));
  } else if (OB_FAIL(rowkey.prepare_memtable_readable(col_descs, allocator))) {
    LOG_WARN("failed to prepare store rowkey to read memtable", K(ret), K(datum_row), K(rowkey));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

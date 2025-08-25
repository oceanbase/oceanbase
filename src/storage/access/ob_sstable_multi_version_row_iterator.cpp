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

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_multi_version_row_iterator.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;

namespace storage {

static int get_not_exist_row(
    const ObDatumRowkey &rowkey,
    ObDatumRow &not_exist_row,
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_cnt = rowkey.get_datum_cnt();
  if (rowkey_cnt > not_exist_row.get_column_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid rowkey cnt", K(ret), K(rowkey_cnt), K(not_exist_row));
  } else {
    for (int64_t i = 0; i < rowkey_cnt; i++) {
      not_exist_row.storage_datums_[i] = rowkey.datums_[i];
    }
    for (int64_t i = rowkey_cnt; i < not_exist_row.get_column_count(); i++) {
      not_exist_row.storage_datums_[i].set_nop();
    }

    row = &not_exist_row;
  }
  return ret;
}

///////////////////////////// Getter ///////////////////////////////////////////
void ObSSTableMultiVersionRowGetter::reset()
{
  ObSSTableRowScanner::reset();
  multi_version_range_.reset();
  range_idx_ = 0;
  base_rowkey_ = nullptr;
  not_exist_row_.reset();
}

void ObSSTableMultiVersionRowGetter::reuse()
{
  ObSSTableRowScanner::reuse();
  multi_version_range_.reset();
  range_idx_ = 0;
  base_rowkey_ = nullptr;
  not_exist_row_.reset();
}

void ObSSTableMultiVersionRowGetter::reclaim()
{
  ObSSTableRowScanner::reclaim();
  multi_version_range_.reset();
  range_idx_ = 0;
  base_rowkey_ = nullptr;
  not_exist_row_.reset();
}

int ObSSTableMultiVersionRowGetter::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(query_range), KP(table));
  } else {
    base_rowkey_ = static_cast<const ObDatumRowkey *>(query_range);
    if (OB_FAIL(base_rowkey_->to_multi_version_range(*access_ctx.get_range_allocator(), multi_version_range_))) {
      STORAGE_LOG(WARN, "Failed to transfer multi version range", K(ret), KPC_(base_rowkey));
    } else if (OB_FAIL(ObSSTableRowScanner::inner_open(
                iter_param, access_ctx, table, &multi_version_range_))) {
      LOG_WARN("failed to open scanner", K(ret));
    } else if (OB_FAIL(not_exist_row_.init(*access_ctx.get_range_allocator(), iter_param.get_out_col_cnt()))) {
        LOG_WARN("fail to init datum row", K(ret));
    } else {
      not_exist_row_.row_flag_.reset();
      not_exist_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowGetter::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(base_rowkey_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(base_rowkey_));
  } else if (OB_FAIL(ObSSTableRowScanner::inner_get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    } else if (0 == range_idx_) {
      if (OB_FAIL(get_not_exist_row(
                  *base_rowkey_,
                  not_exist_row_,
                  row))) {
        LOG_WARN("failed to get not exist row, ", K(ret));
      } else {
        ++range_idx_;
        ret = OB_SUCCESS;
      }
    }
  } else {
    ++range_idx_;
  }
  return ret;
}


///////////////////////////// Scanner ///////////////////////////////////////////
void ObSSTableMultiVersionRowScanner::reset()
{
  ObSSTableMultiVersionRowGetter::reset();
  base_range_ = NULL;
}

void ObSSTableMultiVersionRowScanner::reuse()
{
  ObSSTableMultiVersionRowGetter::reuse();
  base_range_ = NULL;
}

void ObSSTableMultiVersionRowScanner::reclaim()
{
  ObSSTableMultiVersionRowGetter::reclaim();
  base_range_ = NULL;
}

int ObSSTableMultiVersionRowScanner::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(query_range), KP(table));
  } else {
    ObSSTable *sstable = static_cast<ObSSTable *>(table);
    base_range_ = static_cast<const ObDatumRange *>(query_range);
    if (OB_FAIL(base_range_->to_multi_version_range(*access_ctx.get_range_allocator(), multi_version_range_))) {
      STORAGE_LOG(WARN, "Failed to transfer multi version range", K(ret), KPC(base_range_));
    } else if (OB_FAIL(ObSSTableRowScanner::inner_open(iter_param, access_ctx, table, &multi_version_range_))) {
      LOG_WARN("failed to open scanner", K(ret));
    }
  }
  return ret;
}

///////////////////////////// MultiGetter ///////////////////////////////////////////
void ObSSTableMultiVersionRowMultiGetter::reset()
{
  ObSSTableRowMultiScanner::reset();
  multi_version_ranges_.reset();
  range_idx_ = 0;
  not_exist_row_.reset();
  pending_row_ = NULL;
  base_rowkeys_ = NULL;
}

void ObSSTableMultiVersionRowMultiGetter::reuse()
{
  ObSSTableRowMultiScanner::reuse();
  multi_version_ranges_.reset();
  not_exist_row_.reset();
  range_idx_ = 0;
  pending_row_ = NULL;
  base_rowkeys_ = NULL;
}

void ObSSTableMultiVersionRowMultiGetter::reclaim()
{
  ObSSTableRowMultiScanner::reclaim();
  multi_version_ranges_.reset();
  range_idx_ = 0;
  not_exist_row_.reset();
  pending_row_ = NULL;
  base_rowkeys_ = NULL;
}

int ObSSTableMultiVersionRowMultiGetter::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(query_range), KP(table));
  } else {
    base_rowkeys_ = reinterpret_cast<const ObIArray<ObDatumRowkey> *> (query_range);

    if (OB_FAIL(multi_version_ranges_.reserve(base_rowkeys_->count()))) {
      LOG_WARN("reserve multi_version_ranges_ failed", K(ret), K(base_rowkeys_->count()));
    } else {
      ObDatumRange tmp_multi_version_range;
      for (int i = 0; OB_SUCC(ret) && i < base_rowkeys_->count(); i++) {
        tmp_multi_version_range.reset();
        if (OB_FAIL(base_rowkeys_->at(i).to_multi_version_range(*access_ctx.get_range_allocator(), tmp_multi_version_range))) {
          STORAGE_LOG(WARN, "Failed to transfer multi version range", K(ret), K(i), K(base_rowkeys_->at(i)));
        } else if (OB_FAIL(multi_version_ranges_.push_back(tmp_multi_version_range))) {
          LOG_WARN("push back multi version range failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObSSTableRowMultiScanner::inner_open(
                  iter_param, access_ctx, table, &multi_version_ranges_))) {
        LOG_WARN("failed to open multi scanner", K(ret));
      } else if (OB_FAIL(not_exist_row_.init(*access_ctx.get_range_allocator(), iter_param.get_out_col_cnt()))) {
        LOG_WARN("fail to init datum row", K(ret));
      } else {
        not_exist_row_.row_flag_.reset();
        not_exist_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
      }
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowMultiGetter::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (OB_ISNULL(base_rowkeys_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(base_rowkeys_));
  } else {
    bool has_empty_range = false;
    if (NULL == pending_row_) {
      if (OB_FAIL(ObSSTableRowMultiScanner::inner_get_next_row(pending_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row", K(ret));
        } else {
          has_empty_range = (range_idx_ < base_rowkeys_->count());
          if (has_empty_range) {
            ret = OB_SUCCESS;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL != pending_row_) {
        if (range_idx_ < pending_row_->scan_index_) {
          has_empty_range = true;
        } else if (pending_row_->scan_index_ == range_idx_) {
          row = pending_row_;
          pending_row_ = NULL;
        }
      }
    }

    if (OB_SUCC(ret) && has_empty_range) {
      if (OB_FAIL(get_not_exist_row(
                  base_rowkeys_->at(range_idx_),
                  not_exist_row_,
                  row))) {
        LOG_WARN("failed to get not exist row, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is empty", K(ret), K(range_idx_),
                 K(base_rowkeys_->at(range_idx_)));
      } else {
        (const_cast<ObDatumRow*> (row))->scan_index_ = range_idx_;
        ++range_idx_;
      }
    }
  }
  return ret;
}

///////////////////////////// MultiScanner ///////////////////////////////////////////
void ObSSTableMultiVersionRowMultiScanner::reset()
{
  ObSSTableRowMultiScanner::reset();
  multi_version_ranges_.reset();
}

void ObSSTableMultiVersionRowMultiScanner::reuse()
{
  ObSSTableRowMultiScanner::reuse();
  multi_version_ranges_.reset();
}

int ObSSTableMultiVersionRowMultiScanner::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(query_range), KP(table));
  } else {
    const ObIArray<ObDatumRange> *base_ranges = static_cast<const ObIArray<ObDatumRange> *>(query_range);
    int64_t out_cols_cnt = iter_param.get_out_col_cnt();
    if (OB_UNLIKELY(0 == out_cols_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected empty out_cols", K(ret), K(iter_param));
    } else if (OB_FAIL(multi_version_ranges_.reserve(base_ranges->count()))) {
      LOG_WARN("reserve multi_version_ranges_ failed", K(ret), K(base_ranges->count()));
    } else {
      ObDatumRange tmp_multi_version_range;
      for (int i = 0; OB_SUCC(ret) && i < base_ranges->count(); i++) {
        tmp_multi_version_range.reset();
        if (OB_FAIL(base_ranges->at(i).to_multi_version_range(*access_ctx.get_range_allocator(), tmp_multi_version_range))) {
          STORAGE_LOG(WARN, "Failed to transfer multi version range", K(ret), K(i), K(base_ranges->at(i)));
        } else if (OB_FAIL(multi_version_ranges_.push_back(tmp_multi_version_range))) {
          LOG_WARN("push back multi version range failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObSSTableRowMultiScanner::inner_open(iter_param, access_ctx, table, &multi_version_ranges_))) {
        LOG_WARN("failed to open scanner", K(ret));
      }
    }
  }
  return ret;

}

}
}

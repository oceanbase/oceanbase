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
ObSSTableMultiVersionRowIterator::ObSSTableMultiVersionRowIterator()
    : iter_param_(NULL),
      access_ctx_(NULL),
      sstable_(NULL),
      query_range_(NULL),
      iter_(NULL),
      out_cols_cnt_(0),
      range_idx_(0),
      read_newest_(false)
{
  not_exist_row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  not_exist_row_.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
  not_exist_row_.row_val_.count_ = 0;
}

ObSSTableMultiVersionRowIterator::~ObSSTableMultiVersionRowIterator()
{
  if (NULL != iter_) {
    iter_->~ObSSTableRowIterator();
    iter_ = NULL;
  }
}

void ObSSTableMultiVersionRowIterator::reset()
{
  ObISSTableRowIterator::reset();
  query_range_ = NULL;
  if (NULL != iter_) {
    iter_->~ObSSTableRowIterator();
    iter_ = NULL;
  }
  out_cols_cnt_ = 0;
  range_idx_ = 0;
  read_newest_ = false;
}

void ObSSTableMultiVersionRowIterator::reuse()
{
  ObISSTableRowIterator::reuse();
  query_range_ = NULL;
  if (NULL != iter_) {
    iter_->reuse();
  }
  out_cols_cnt_ = 0;
  range_idx_ = 0;
  read_newest_ = false;
}

template <typename T>
int ObSSTableMultiVersionRowIterator::new_iterator(ObArenaAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (NULL == iter_) {
    void* buf = NULL;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate iterator", K(ret));
    } else {
      iter_ = new (buf) T();
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowIterator::get_not_exist_row(const common::ObStoreRowkey& rowkey, const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (out_cols_cnt_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("The multi version row iterator has not been inited, ", K(ret), K(out_cols_cnt_));
  } else {
    const int64_t rowkey_cnt = rowkey.get_obj_cnt();
    not_exist_row_.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
    not_exist_row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    not_exist_row_.row_val_.count_ = out_cols_cnt_;

    for (int64_t i = 0; i < rowkey_cnt; i++) {
      not_exist_row_.row_val_.cells_[i] = rowkey.get_obj_ptr()[i];
    }
    for (int64_t i = rowkey_cnt; i < out_cols_cnt_; i++) {
      not_exist_row_.row_val_.cells_[i].set_nop_value();
    }

    row = &not_exist_row_;
  }
  return ret;
}

int ObSSTableMultiVersionRowGetter::inner_open(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray* out_cols = nullptr;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table), KP(query_range));
  } else if (OB_FAIL(iter_param.get_out_cols(access_ctx.use_fuse_row_cache_, out_cols))) {
    LOG_WARN("fail to get out cols", K(ret));
  } else {
    query_range_ = query_range;
    out_cols_cnt_ = out_cols->count();
    if (OB_FAIL(ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
            *rowkey_, access_ctx.trans_version_range_, *access_ctx.allocator_, multi_version_range_))) {
      LOG_WARN("convert to multi version range failed", K(ret), K(*range_));
    } else if (OB_FAIL(new_iterator<ObSSTableRowScanner>(*access_ctx.allocator_))) {
      LOG_WARN("failed to new iterator", K(ret));
    } else if (OB_FAIL(iter_->init(iter_param, access_ctx, table, &multi_version_range_))) {
      LOG_WARN("failed to open scanner", K(ret));
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowGetter::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_) || OB_ISNULL(query_range_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(iter_), KP(query_range_));
  } else if (OB_FAIL(iter_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    } else if (0 == range_idx_) {
      if (OB_FAIL(get_not_exist_row(rowkey_->get_store_rowkey(), row))) {
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

void ObSSTableMultiVersionRowGetter::reset()
{
  multi_version_range_.reset();
  ObSSTableMultiVersionRowIterator::reset();
}

void ObSSTableMultiVersionRowGetter::reuse()
{
  multi_version_range_.reset();
  ObSSTableMultiVersionRowIterator::reuse();
}

uint8_t ObSSTableMultiVersionRowScannerBase::get_iter_flag()
{
  int ret = OB_SUCCESS;
  uint8_t flag = 0;
  if (!read_newest_) {
    // do nothing
  } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->get_row_iter_flag_impl(flag))) {
    ret = OB_SUCCESS;
  }
  return flag;
}

void ObSSTableMultiVersionRowScannerBase::reset()
{
  ObSSTableMultiVersionRowIterator::reset();
  skip_range_.reset();
  trans_version_range_.reset();
  gap_rowkey_.reset();
}

void ObSSTableMultiVersionRowScannerBase::reuse()
{
  ObSSTableMultiVersionRowIterator::reuse();
  skip_range_.reset();
  trans_version_range_.reset();
  gap_rowkey_.reset();
}

int ObSSTableMultiVersionRowScanner::inner_open(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table), KP(query_range));
  } else {
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    sstable_ = static_cast<ObSSTable*>(table);
    query_range_ = query_range;
    read_newest_ = access_ctx.trans_version_range_.snapshot_version_ >= sstable_->get_upper_trans_version() &&
                   sstable_->has_compact_row();
    trans_version_range_ = access_ctx.trans_version_range_;
    if (OB_FAIL(ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
            *range_, access_ctx.trans_version_range_, *access_ctx.allocator_, multi_version_range_))) {
      LOG_WARN("convert to multi version range failed", K(ret), K(*range_));
    } else if (OB_FAIL(new_iterator<ObSSTableRowScanner>(*access_ctx.allocator_))) {
      LOG_WARN("failed to new iterator", K(ret));
    } else if (OB_FAIL(iter_->init(iter_param, access_ctx, table, &multi_version_range_))) {
      LOG_WARN("failed to open scanner", K(ret));
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowScanner::inner_get_next_row(const ObStoreRow*& row)
{
  return iter_->get_next_row(row);
}

void ObSSTableMultiVersionRowScanner::reset()
{
  multi_version_range_.reset();
  ObSSTableMultiVersionRowScannerBase::reset();
}

void ObSSTableMultiVersionRowScanner::reuse()
{
  multi_version_range_.reset();
  ObSSTableMultiVersionRowScannerBase::reuse();
}

int ObSSTableMultiVersionRowScanner::skip_range(
    int64_t range_idx, const ObStoreRowkey* gap_rowkey, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  ObExtStoreRange* new_range = NULL;
  if (OB_UNLIKELY(range_idx < 0 || NULL == gap_rowkey)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(range_idx), KP(gap_rowkey));
  } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->generate_new_range(
                 range_idx, *gap_rowkey, include_gap_key, *range_, new_range))) {
    LOG_WARN("fail to generate new range", K(ret));
  } else if (NULL != new_range) {
    if (OB_FAIL(ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
            *new_range, trans_version_range_, *access_ctx_->allocator_, skip_range_))) {
      LOG_WARN("fail to do range to multi version range", K(ret));
    } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->skip_range_impl(
                   range_idx, multi_version_range_, skip_range_))) {
      LOG_WARN("fail to skip range impl", K(ret), K(range_idx));
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowScanner::get_gap_end(
    int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size)
{
  int ret = OB_SUCCESS;
  if (!read_newest_) {
    // do nothing
  } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->get_gap_end_impl(
                 multi_version_range_, gap_rowkey_, gap_size))) {
    STORAGE_LOG(WARN, "fail to get gap end impl", K(ret));
  } else {
    gap_key = &gap_rowkey_;
    range_idx = 0;
  }
  return ret;
}

int ObSSTableMultiVersionRowMultiGetter::inner_open(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray* out_cols = nullptr;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table), KP(query_range));
  } else if (OB_FAIL(iter_param.get_out_cols(access_ctx.use_fuse_row_cache_, out_cols))) {
    LOG_WARN("fail to get out cols", K(ret));
  } else {
    query_range_ = query_range;
    out_cols_cnt_ = out_cols->count();

    if (OB_FAIL(multi_version_ranges_.reserve(rowkeys_->count()))) {
      LOG_WARN("reserve multi_version_ranges_ failed", K(ret), K(rowkeys_->count()));
    } else {
      ObExtStoreRange tmp_multi_version_range;
      for (int i = 0; OB_SUCC(ret) && i < rowkeys_->count(); i++) {
        tmp_multi_version_range.reset();
        if (OB_FAIL(ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
                rowkeys_->at(i), access_ctx.trans_version_range_, *access_ctx.allocator_, tmp_multi_version_range))) {
          LOG_WARN("convert to multi version range failed", K(ret), K(i), K(rowkeys_->at(i)));
        } else if (OB_FAIL(multi_version_ranges_.push_back(tmp_multi_version_range))) {
          LOG_WARN("push back multi version range failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(new_iterator<ObSSTableRowMultiScanner>(*access_ctx.allocator_))) {
        LOG_WARN("failed to new iterator", K(ret));
      } else if (OB_FAIL(iter_->init(iter_param, access_ctx, table, &multi_version_ranges_))) {
        LOG_WARN("failed to open multi scanner", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowMultiGetter::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (OB_ISNULL(iter_) || OB_ISNULL(query_range_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(iter_), KP(query_range_));
  } else {
    bool has_empty_range = false;
    if (NULL == pending_row_) {
      if (OB_FAIL(iter_->get_next_row(pending_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row", K(ret));
        } else {
          has_empty_range = (range_idx_ < rowkeys_->count());
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
      if (OB_FAIL(get_not_exist_row(rowkeys_->at(range_idx_).get_store_rowkey(), row))) {
        LOG_WARN("failed to get not exist row, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is empty", K(ret), K(range_idx_), K(rowkeys_->at(range_idx_).get_store_rowkey()));
      } else {
        (const_cast<ObStoreRow*>(row))->scan_index_ = range_idx_;
        (const_cast<ObStoreRow*>(row))->is_get_ = true;
        ++range_idx_;
      }
    }
  }
  return ret;
}

void ObSSTableMultiVersionRowMultiGetter::reset()
{
  multi_version_ranges_.reset();
  pending_row_ = NULL;
  ObSSTableMultiVersionRowIterator::reset();
}

void ObSSTableMultiVersionRowMultiGetter::reuse()
{
  multi_version_ranges_.reset();
  pending_row_ = NULL;
  ObSSTableMultiVersionRowIterator::reuse();
}

int ObSSTableMultiVersionRowMultiScanner::inner_open(
    const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table), KP(query_range));
  } else {
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    sstable_ = static_cast<ObSSTable*>(table);
    read_newest_ = access_ctx.trans_version_range_.snapshot_version_ >= sstable_->get_upper_trans_version() &&
                   sstable_->has_compact_row();
    query_range_ = query_range;
    reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
    range_idx_ = -1;
    last_pending_range_idx_ = -1;
    ObExtStoreRange tmp_multi_version_range;
    out_cols_cnt_ = iter_param.out_cols_->count();
    trans_version_range_ = access_ctx.trans_version_range_;
    if (OB_FAIL(multi_version_ranges_.reserve(ranges_->count()))) {
      LOG_WARN("reserve multi_version_ranges_ failed", K(ret), K(ranges_->count()));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < ranges_->count(); i++) {
        tmp_multi_version_range.reset();
        if (ranges_->at(i).is_single_rowkey()) {
          if (read_newest_) {
            tmp_multi_version_range = ranges_->at(i);
          } else if (OB_FAIL(ObVersionStoreRangeConversionHelper::store_rowkey_to_multi_version_range(
                         ObExtStoreRowkey(ranges_->at(i).get_range().get_start_key()),
                         access_ctx.trans_version_range_,
                         *access_ctx.allocator_,
                         tmp_multi_version_range))) {
            LOG_WARN("convert to multi version range failed", K(ret), K(i), K(ranges_->at(i)));
          }
        } else {
          if (OB_FAIL(ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
                  ranges_->at(i), access_ctx.trans_version_range_, *access_ctx.allocator_, tmp_multi_version_range))) {
            LOG_WARN("convert to multi version range failed", K(ret), K(i), K(ranges_->at(i)));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(multi_version_ranges_.push_back(tmp_multi_version_range))) {
          LOG_WARN("push back multi version range failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(new_iterator<ObSSTableRowMultiScanner>(*access_ctx.allocator_))) {
        LOG_WARN("failed to new iterator", K(ret));
      } else if (OB_FAIL(iter_->init(iter_param, access_ctx, table, &multi_version_ranges_))) {
        LOG_WARN("failed to open scanner", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowMultiScanner::skip_range(
    int64_t range_idx, const ObStoreRowkey* gap_rowkey, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  ObExtStoreRange* new_range = NULL;
  if (OB_UNLIKELY(range_idx < 0 || NULL == gap_rowkey)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(range_idx), KP(gap_rowkey));
  } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->generate_new_range(
                 range_idx, *gap_rowkey, include_gap_key, ranges_->at(range_idx), new_range))) {
    LOG_WARN("fail to generate new range", K(ret));
  } else if (NULL != new_range) {
    if (OB_FAIL(ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
            *new_range, trans_version_range_, *access_ctx_->allocator_, skip_range_))) {
      LOG_WARN("fail to do range to multi version range", K(ret));
    } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->skip_range_impl(
                   range_idx, multi_version_ranges_.at(range_idx), skip_range_))) {
      LOG_WARN("fail to skip range impl", K(ret), K(range_idx));
    }
  }
  return ret;
}

int ObSSTableMultiVersionRowMultiScanner::get_gap_end(
    int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size)
{
  int ret = OB_SUCCESS;
  if (!read_newest_) {
    // do nothing
  } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->get_gap_range_idx(range_idx))) {
    STORAGE_LOG(WARN, "fail to get gap range idx", K(ret));
  } else if (OB_FAIL(static_cast<ObSSTableRowScanner*>(iter_)->get_gap_end_impl(
                 multi_version_ranges_.at(range_idx), gap_rowkey_, gap_size))) {
    STORAGE_LOG(WARN, "fail to get gap end impl", K(ret));
  } else {
    gap_key = &gap_rowkey_;
  }
  return ret;
}

int ObSSTableMultiVersionRowMultiScanner::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (OB_ISNULL(iter_) || OB_ISNULL(query_range_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(iter_), KP(query_range_));
  } else {
    bool has_empty_range = false;
    int64_t pending_idx = 0;

    if (NULL == pending_row_ && OB_FAIL(iter_->get_next_row(pending_row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      } else {
        pending_idx = ranges_->count();
      }
    } else if (OB_ISNULL(pending_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pending_row_ is NULL", K(ret));
    } else {
      pending_idx = pending_row_->scan_index_;
    }

    if (OB_LIKELY(OB_SUCCESS == ret || OB_ITER_END == ret)) {
      if (last_pending_range_idx_ != pending_idx) {
        range_idx_++;
      }
      has_empty_range = range_idx_ < pending_idx;
      last_pending_range_idx_ = pending_idx;
      if (OB_ITER_END == ret && has_empty_range) {
        ret = OB_SUCCESS;
      }
    }

    while (OB_SUCC(ret) && has_empty_range) {
      if (ranges_->at(range_idx_).is_single_rowkey()) {
        if (OB_FAIL(get_not_exist_row(ranges_->at(range_idx_).get_range().get_start_key(), row))) {
          LOG_WARN("failed to get not exist row, ", K(ret));
        }
        break;
      }
      range_idx_++;
      has_empty_range = range_idx_ < pending_idx;
    }

    if (OB_SUCC(ret)) {
      if ((range_idx_ >= ranges_->count())) {
        ret = OB_ITER_END;
      } else {
        if (pending_idx == range_idx_) {
          row = pending_row_;
          pending_row_ = NULL;
        }
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is NULL", K(ret), K(range_idx_), K(ranges_->count()));
        } else {
          (const_cast<ObStoreRow*>(row))->scan_index_ = range_idx_;
          (const_cast<ObStoreRow*>(row))->is_get_ = ranges_->at(range_idx_).is_single_rowkey();
          range_idx_ = has_empty_range ? range_idx_ + 1 : range_idx_;
        }
      }
    }
  }
  return ret;
}

void ObSSTableMultiVersionRowMultiScanner::reset()
{
  multi_version_ranges_.reset();
  last_pending_range_idx_ = 0;
  pending_row_ = NULL;
  reverse_scan_ = false;
  orig_ranges_ = NULL;
  ObSSTableMultiVersionRowScannerBase::reset();
}

void ObSSTableMultiVersionRowMultiScanner::reuse()
{
  multi_version_ranges_.reset();
  last_pending_range_idx_ = 0;
  pending_row_ = NULL;
  reverse_scan_ = false;
  orig_ranges_ = NULL;
  ObSSTableMultiVersionRowScannerBase::reuse();
}

}  // namespace storage
}  // namespace oceanbase

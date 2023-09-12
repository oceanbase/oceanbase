// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "ob_multiple_skip_scan_merge.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{

ObMultipleSkipScanMerge::ObMultipleSkipScanMerge()
  : ObMultipleScanMerge(),
    state_(SCAN_ROWKEY),
    scan_rowkey_cnt_(0),
    schema_rowkey_cnt_(0),
    ss_rowkey_prefix_cnt_(0),
    scan_rowkey_range_(),
    scan_rows_range_(),
    datums_cnt_(0),
    datums_(nullptr),
    origin_range_(nullptr),
    skip_scan_range_(nullptr),
    range_allocator_("SS_RANGE"),
    rowkey_allocator_("SS_ROWKEY")
{
}

ObMultipleSkipScanMerge::~ObMultipleSkipScanMerge()
{
  reset();
}

int ObMultipleSkipScanMerge::init(
    const ObTableAccessParam &param,
    ObTableAccessContext &context,
    const ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;
  context.range_allocator_ = &range_allocator_;
  if (OB_FAIL(ObMultipleScanMerge::init(param, context, get_table_param))) {
    STORAGE_LOG(WARN, "Fail to init ObMultipleScanMerge", K(ret), K(param), K(context), K(get_table_param));
  } else {
    // prepare ranges for finding distinct rowkey prefix and outputing rows
    void *buf = nullptr;
    schema_rowkey_cnt_ = param.iter_param_.get_schema_rowkey_count();
    ss_rowkey_prefix_cnt_ = param.iter_param_.get_ss_rowkey_prefix_cnt();
    datums_cnt_ = SKIP_SCAN_ROWKEY_DATUMS_ARRAY_CNT * schema_rowkey_cnt_;
    if (schema_rowkey_cnt_ <= 0 || ss_rowkey_prefix_cnt_ <= 0 || ss_rowkey_prefix_cnt_ > schema_rowkey_cnt_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", K(ret), K(schema_rowkey_cnt_), K(ss_rowkey_prefix_cnt_));
    } else if (OB_ISNULL(buf = context.stmt_allocator_->alloc(sizeof(ObStorageDatum) * datums_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to alloc datums", K(ret), K(datums_cnt_));
    } else if (FALSE_IT(datums_ = new (buf) ObStorageDatum[datums_cnt_])) {
    } else if (OB_FAIL(prepare_range(start_key_of_scan_rowkey_range(), scan_rowkey_range_))) {
      STORAGE_LOG(WARN, "Fail to prepare distinct scan range", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "success to init ObMultipleSkipScanMerge", K(param), K(context), K(get_table_param),
          K(schema_rowkey_cnt_), K(ss_rowkey_prefix_cnt_));
    }
  }
  return ret;
}

void ObMultipleSkipScanMerge::reset()
{
  state_ = SCAN_ROWKEY;
  scan_rowkey_cnt_ = 0;
  schema_rowkey_cnt_ = 0;
  ss_rowkey_prefix_cnt_ = 0;
  scan_rowkey_range_.reset();
  scan_rows_range_.reset();
  datums_cnt_ = 0;
  if (OB_NOT_NULL(datums_) && OB_NOT_NULL(access_ctx_->stmt_allocator_)) {
    access_ctx_->stmt_allocator_->free(datums_);
  }
  datums_ = nullptr;
  origin_range_ = nullptr;
  skip_scan_range_ = nullptr;
  range_allocator_.reset();
  rowkey_allocator_.reset();
  ObMultipleScanMerge::reset();
}

void ObMultipleSkipScanMerge::reuse()
{
  if (RETIRED_TO_SCAN != state_) {
    state_ = SCAN_ROWKEY;
  }
  reuse_datums();
  range_allocator_.reuse();
  rowkey_allocator_.reuse();
  ObMultipleScanMerge::reuse();
}

// range: the original key range to scan rows
// skip_scan_range: the key range only contains suffix columns in rowkey
int ObMultipleSkipScanMerge::open(const blocksstable::ObDatumRange &range, const blocksstable::ObDatumRange &skip_scan_range)
{
  int ret = OB_SUCCESS;
  origin_range_ = &range;
  skip_scan_range_ = &skip_scan_range;
  if (RETIRED_TO_SCAN == state_) {
    if (OB_FAIL(ObMultipleScanMerge::open(range))) {
      STORAGE_LOG(WARN, "Fail to open ObMultipleScanMerge", K(ret), K(range));
    }
  } else if (OB_FAIL(open_skip_scan(range, skip_scan_range))) {
    STORAGE_LOG(WARN, "Fail to open skip scan", K(ret), K(range), K(skip_scan_range));
  }
  return ret;
}

int ObMultipleSkipScanMerge::open_skip_scan(const blocksstable::ObDatumRange &range, const blocksstable::ObDatumRange &skip_scan_range)
{
  int ret = OB_SUCCESS;
  bool exceeded = false;
  const int64_t skip_range_datum_cnt = schema_rowkey_cnt_ - ss_rowkey_prefix_cnt_;
  access_ctx_->range_allocator_ = &range_allocator_;
  if (skip_scan_range.is_whole_range()) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not supported index skip scan plan", K(ret));
  } else if (skip_scan_range.start_key_.get_datum_cnt() > skip_range_datum_cnt ||
      (!skip_scan_range.start_key_.is_min_rowkey() && skip_scan_range.start_key_.get_datum_cnt() != skip_range_datum_cnt) ||
      (!skip_scan_range.end_key_.is_max_rowkey() && skip_scan_range.end_key_.get_datum_cnt() != skip_range_datum_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid skip scan range", K(ret), K(skip_scan_range), K(schema_rowkey_cnt_), K(ss_rowkey_prefix_cnt_));
  } else if (OB_FAIL(ObMultipleScanMerge::open(range))) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleScanMerge", K(ret), K(range));
  } else {
    prepare_rowkey(start_key_of_scan_rowkey_range(), range.start_key_, schema_rowkey_cnt_, true);
    prepare_rowkey(end_key_of_scan_rowkey_range(), range.end_key_, schema_rowkey_cnt_, false);
    scan_rowkey_range_.set_border_flag(range.get_border_flag());
    scan_rowkey_range_.set_group_idx(range.get_group_idx());
    scan_rowkey_range_.set_group_idx(range.get_group_idx());
    const ObColDescIArray *col_descs = nullptr;
    if (OB_ISNULL(col_descs = access_param_->iter_param_.get_out_col_descs())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "Unexpected null out cols", K(ret));
    } else if (OB_FAIL(scan_rowkey_range_.prepare_memtable_readable(*col_descs, rowkey_allocator_))) {
      STORAGE_LOG(WARN, "Fail to transfer store rowkey", K(ret), K(scan_rowkey_range_));
    }
    STORAGE_LOG(TRACE, "open skip scan", K(ret), K(schema_rowkey_cnt_), K(ss_rowkey_prefix_cnt_),
        K(scan_rows_range_), K(range), K(skip_scan_range));
  }
  return ret;
}

int ObMultipleSkipScanMerge::inner_get_next_row(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    switch(state_) {
      case SCAN_ROWKEY: {
        // get next rowkey
        // after get next rowkey, update and open scan rows range
        if (OB_FAIL(ObMultipleScanMerge::inner_get_next_row(row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
            STORAGE_LOG(WARN, "Fail to get next row", K(ret));
          } else if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
          } else {
            state_ = SCAN_FINISHED;
          }
        } else {
          state_ = UPDATE_SCAN_ROWS_RANGE;
          scan_rowkey_cnt_++;
        }
        break;
      }
      case UPDATE_SCAN_ROWS_RANGE: {
        if (OB_FAIL(update_scan_rows_range(row))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            state_ = UPDATE_SCAN_ROWKEY_RANGE;
          } else {
            STORAGE_LOG(WARN, "Fail to update scan rows range", K(ret), K(row));
          }
        } else if (should_retire_to_scan()) {
          state_ = RETIRED_TO_SCAN;
        } else {
          STORAGE_LOG(DEBUG, "skip scan update scan rows range", K(row), K(scan_rows_range_));
          state_ = SCAN_ROWS;
        }
        break;
      }
      case SCAN_ROWS: {
        // get next row
        // after get next row, update and open scan rowkey range
        if (OB_FAIL(ObMultipleScanMerge::inner_get_next_row(row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
            STORAGE_LOG(WARN, "Fail to get next row", K(ret));
          } else if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
          } else {
            ret = OB_SUCCESS;
            state_ = UPDATE_SCAN_ROWKEY_RANGE;
          }
        } else {
          STORAGE_LOG(DEBUG, "skip scan get next row", K(row));
          got_row = true;
        }
        break;
      }
      case UPDATE_SCAN_ROWKEY_RANGE: {
        if (OB_FAIL(update_scan_rowkey_range())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
              STORAGE_LOG(WARN, "Fail to update scan rowkey range", K(ret), K(row));
          } else {
            state_ = SCAN_FINISHED;
          }
        } else {
          STORAGE_LOG(DEBUG, "skip scan update scan rowkey range", K(row), K(scan_rowkey_range_));
          state_ = SCAN_ROWKEY;
        }
        break;
      }
      case SCAN_FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      case RETIRED_TO_SCAN: {
        ret = ObMultipleScanMerge::inner_get_next_row(row);
        got_row = true;
        break;
      }
      default : {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected state", K(state_));
      }
    }
  }
  return ret;
}

int ObMultipleSkipScanMerge::inner_get_next_rows()
{
  int ret = OB_SUCCESS;
  bool end_loop = false;
  if (SCAN_ROWS != state_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected state", K(ret), K(state_));
  } else {
    while (OB_SUCC(ret) && !end_loop) {
      switch(state_) {
        case SCAN_ROWKEY: {
          if (OB_FAIL(ObMultipleScanMerge::inner_get_next_row(unprojected_row_))) {
            if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
              STORAGE_LOG(WARN, "Fail to get next row", K(ret));
            } else if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
            } else {
              state_ = SCAN_FINISHED;
            }
          } else {
            state_ = UPDATE_SCAN_ROWS_RANGE;
            scan_rowkey_cnt_++;
          }
          break;
        }
        case UPDATE_SCAN_ROWS_RANGE: {
          if (OB_FAIL(update_scan_rows_range(unprojected_row_))) {
            if (OB_LIKELY(OB_ITER_END == ret)) {
              ret = OB_SUCCESS;
              state_ = UPDATE_SCAN_ROWKEY_RANGE;
            } else {
               STORAGE_LOG(WARN, "Fail to update scan rows range", K(ret), K(unprojected_row_));
            }
          } else if (should_retire_to_scan()) {
            state_ = RETIRED_TO_SCAN;
          } else {
            STORAGE_LOG(DEBUG, "skip scan update scan rows range", K(unprojected_row_), K(scan_rows_range_));
            state_ = SCAN_ROWS;
          }
          break;
        }
        case SCAN_ROWS: {
          bool can_batch = false;
          if (OB_FAIL(can_batch_scan(can_batch))) {
            STORAGE_LOG(WARN, "Fail to check can batch scan", K(ret));
          } else if (!can_batch) {
            end_loop = true;
            ret = OB_PUSHDOWN_STATUS_CHANGED;
          } else if (OB_FAIL(ObMultipleScanMerge::inner_get_next_rows())) {
            if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
              STORAGE_LOG(WARN, "Fail to get next rows", K(ret));
            } else if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
            } else {
              ret = OB_SUCCESS;
              state_ = UPDATE_SCAN_ROWKEY_RANGE;
            }
          } else {
            end_loop = true;
          }
          break;
        }
        case UPDATE_SCAN_ROWKEY_RANGE: {
          if (OB_FAIL(update_scan_rowkey_range())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
                STORAGE_LOG(WARN, "Fail to update scan rowkey range", K(ret), K(unprojected_row_));
            } else {
              state_ = SCAN_FINISHED;
            }
          } else {
            STORAGE_LOG(DEBUG, "skip scan update scan rowkey range", K(unprojected_row_), K(scan_rowkey_range_));
            state_ = SCAN_ROWKEY;
          }
          break;
        }
        case SCAN_FINISHED: {
          ret = OB_ITER_END;
          break;
        }
        case RETIRED_TO_SCAN: {
          ret = ObMultipleScanMerge::inner_get_next_rows();
          end_loop = true;
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected state", K(state_));
        }
      }
    }
  }
  return ret;
}

int ObMultipleSkipScanMerge::can_batch_scan(bool &can_batch)
{
  int ret = OB_SUCCESS;
  can_batch = (state_ == SCAN_ROWS);
  if (can_batch && OB_FAIL(ObMultipleScanMerge::can_batch_scan(can_batch))) {
    STORAGE_LOG(WARN, "Fail to check can batch scan", K(ret));
  }
  return ret;
}

int ObMultipleSkipScanMerge::prepare_range(ObStorageDatum *datums, ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(range.start_key_.assign(datums, schema_rowkey_cnt_))) {
    STORAGE_LOG(WARN, "Fail to assign start key", K(ret), K(schema_rowkey_cnt_));
  } else if (OB_FAIL(range.end_key_.assign(datums + schema_rowkey_cnt_, schema_rowkey_cnt_))) {
    STORAGE_LOG(WARN, "Fail to assign end key", K(ret), K(schema_rowkey_cnt_));
  }
  return ret;
}

void ObMultipleSkipScanMerge::prepare_rowkey(
    blocksstable::ObStorageDatum *datums,
    const blocksstable::ObDatumRowkey &rowkey,
    const int64_t datum_cnt,
    const bool is_min)
{
  for (int64_t i = 0; i < datum_cnt; ++i) {
    if (i < rowkey.get_datum_cnt()) {
      datums[i] = rowkey.get_datum(i);
    } else if (is_min) {
      datums[i].set_min();
    } else {
      datums[i].set_max();
    }
  }
}

int ObMultipleSkipScanMerge::update_scan_rows_range(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  range_allocator_.reuse();
  if (should_check_interrupt() && OB_FAIL(THIS_WORKER.check_status())) {
    STORAGE_LOG(WARN, "query interrupt", K(ret));
  } else if (OB_FAIL(prepare_scan_row_range())) {
    STORAGE_LOG(WARN, "Fail to prepare scan row range", K(ret));
  } else if (should_retire_to_scan()) {
    // too many distinct prefix, retire to normal scan
    for (int64_t i = 0; OB_SUCC(ret) && i < ss_rowkey_prefix_cnt_; ++i) {
      ObStorageDatum &prefix_of_rows_key = access_ctx_->query_flag_.is_reverse_scan() ?
        end_key_of_scan_rows_range()[i] :
        start_key_of_scan_rows_range()[i];
      prefix_of_rows_key.reuse();
      if (OB_FAIL(prefix_of_rows_key.deep_copy(row.storage_datums_[i], range_allocator_))) {
        STORAGE_LOG(WARN, "Fail to deep copy start key's datum", K(ret), K(i), K(row), K(scan_rows_range_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (access_ctx_->query_flag_.is_reverse_scan()) {
      scan_rows_range_.set_start_key(origin_range_->get_start_key());
      set_border_falg(true, *origin_range_, scan_rows_range_);
    } else {
      scan_rows_range_.set_end_key(origin_range_->get_end_key());
      set_border_falg(false, *origin_range_, scan_rows_range_);
    }
    STORAGE_LOG(TRACE, "should retire to normal scan", K(ret), K(scan_rows_range_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ss_rowkey_prefix_cnt_; ++i) {
      ObStorageDatum &prefix_of_start_key = start_key_of_scan_rows_range()[i];
      ObStorageDatum &prefix_of_end_key = end_key_of_scan_rows_range()[i];
      prefix_of_start_key.reuse();
      prefix_of_end_key.reuse();
      if (OB_FAIL(prefix_of_start_key.deep_copy(row.storage_datums_[i], range_allocator_))) {
        STORAGE_LOG(WARN, "Fail to deep copy start key's datum", K(ret), K(i), K(row), K(scan_rows_range_));
      } else if (OB_FAIL(prefix_of_end_key.deep_copy(row.storage_datums_[i], range_allocator_))) {
        STORAGE_LOG(WARN, "Fail to deep copy end key's datum", K(ret), K(i), K(row), K(scan_rows_range_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // adjust scan rows range according original query range
    // one case is in parallel execution, splitted range
    bool exceeded = false;
    if (OB_FAIL(shrink_scan_rows_range(exceeded))) {
      STORAGE_LOG(WARN, "Fail to check range exceed", K(ret));
    } else if (exceeded) {
      ret = OB_ITER_END;
    }
  }

  if (OB_SUCC(ret)) {
    ObMultipleScanMerge::reuse();
    const ObColDescIArray *col_descs = nullptr;
    if (OB_ISNULL(col_descs = access_param_->iter_param_.get_out_col_descs())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "Unexpected null out cols", K(ret));
    } else if (OB_FAIL(scan_rows_range_.prepare_memtable_readable(*col_descs, range_allocator_))) {
      STORAGE_LOG(WARN, "Fail to transfer store rowkey", K(ret), K(scan_rows_range_));
    } else if (OB_FAIL(ObMultipleScanMerge::open(scan_rows_range_))) {
      STORAGE_LOG(WARN, "Fail to open scan rows range", K(ret), K(scan_rows_range_));
    }
  }
  STORAGE_LOG(TRACE, "Update and open scan rows range", K(ret), K(row), K(scan_rows_range_));
  return ret;
}

int ObMultipleSkipScanMerge::update_scan_rowkey_range()
{
  int ret = OB_SUCCESS;
  rowkey_allocator_.reuse();
  if (OB_UNLIKELY(!scan_rows_range_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected scan rows range", K(ret), K(scan_rows_range_));
  } else {
    ObStorageDatum *rowkey_datums = access_ctx_->query_flag_.is_reverse_scan() ?
        end_key_of_scan_rowkey_range() :
        start_key_of_scan_rowkey_range();
    for (int64_t i = 0; OB_SUCC(ret) && i < ss_rowkey_prefix_cnt_; ++i) {
      if (OB_FAIL(rowkey_datums[i].deep_copy(scan_rows_range_.start_key_.get_datum(i), rowkey_allocator_))) {
        STORAGE_LOG(WARN, "Fail to deep copy start key's datum", K(ret), K(i), K(scan_rowkey_range_));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = ss_rowkey_prefix_cnt_; i < schema_rowkey_cnt_; ++i) {
        access_ctx_->query_flag_.is_reverse_scan() ? rowkey_datums[i].set_min() : rowkey_datums[i].set_max();
      }
      access_ctx_->query_flag_.is_reverse_scan() ? scan_rowkey_range_.set_right_open() : scan_rowkey_range_.set_left_open();

      int cmp_ret = 0;
      const ObStorageDatumUtils &datum_utils = access_param_->iter_param_.get_read_info()->get_datum_utils();
      if (OB_FAIL(scan_rowkey_range_.start_key_.compare(scan_rowkey_range_.end_key_, datum_utils, cmp_ret))) {
        STORAGE_LOG(WARN, "Fail to compare", K(ret), K(scan_rowkey_range_));
      } else if (cmp_ret >= 0) {
        ret = OB_ITER_END;
      } else {
        ObMultipleScanMerge::reuse();
        const ObColDescIArray *col_descs = nullptr;
        if (OB_ISNULL(col_descs = access_param_->iter_param_.get_out_col_descs())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "Unexpected null out cols", K(ret));
        } else if (OB_FAIL(scan_rowkey_range_.prepare_memtable_readable(*col_descs, rowkey_allocator_))) {
          STORAGE_LOG(WARN, "Fail to transfer store rowkey", K(ret), K(scan_rowkey_range_));
        } else if (OB_FAIL(ObMultipleScanMerge::open(scan_rowkey_range_))) {
          STORAGE_LOG(WARN, "Fail to open scan rowkey range", K(ret), K(scan_rowkey_range_));
        }
      }
    }
  }
  STORAGE_LOG(TRACE, "Update and open scan rowkey range", K(ret), K(scan_rows_range_), K(scan_rowkey_range_));
  return ret;
}

int ObMultipleSkipScanMerge::shrink_scan_rows_range(bool &exceeded)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  const ObStorageDatumUtils &datum_utils = access_param_->iter_param_.get_read_info()->get_datum_utils();
  if (OB_FAIL(scan_rows_range_.start_key_.compare(origin_range_->start_key_, datum_utils, cmp_ret))) {
    STORAGE_LOG(WARN, "Fail to compare", K(ret));
  } else if (cmp_ret > 0) {
  } else {
    set_border_falg(true, *origin_range_, scan_rows_range_);
    if (cmp_ret < 0) {
      scan_rows_range_.start_key_ = origin_range_->start_key_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scan_rows_range_.end_key_.compare(origin_range_->end_key_, datum_utils, cmp_ret))) {
    STORAGE_LOG(WARN, "Fail to compare", K(ret));
  } else if (cmp_ret < 0) {
  } else {
    set_border_falg(false, *origin_range_, scan_rows_range_);
    if (cmp_ret > 0) {
      scan_rows_range_.end_key_ = origin_range_->end_key_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scan_rows_range_.start_key_.compare(scan_rows_range_.end_key_, datum_utils, cmp_ret))) {
    STORAGE_LOG(WARN, "Fail to compare", K(ret), K(scan_rows_range_));
  } else if (cmp_ret > 0 || (0 == cmp_ret && (scan_rows_range_.is_left_open() || scan_rows_range_.is_right_open()))) {
    exceeded = true;
  }
  return ret;
}

int ObMultipleSkipScanMerge::prepare_scan_row_range()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_range(start_key_of_scan_rows_range(), scan_rows_range_))) {
    STORAGE_LOG(WARN, "Fail to prepare skip scan range", K(ret));
  } else {
    for (int64_t i = 0; i < ss_rowkey_prefix_cnt_; ++i) {
      start_key_of_scan_rows_range()[i].set_min();
      end_key_of_scan_rows_range()[i].set_max();
    }
    prepare_rowkey(start_key_of_scan_rows_range() + ss_rowkey_prefix_cnt_,
                    skip_scan_range_->start_key_,
                    schema_rowkey_cnt_ - ss_rowkey_prefix_cnt_,
                    true);
    prepare_rowkey(end_key_of_scan_rows_range() + ss_rowkey_prefix_cnt_,
                    skip_scan_range_->end_key_,
                    schema_rowkey_cnt_ - ss_rowkey_prefix_cnt_,
                    false);
    scan_rows_range_.set_border_flag(skip_scan_range_->get_border_flag());
    scan_rows_range_.set_group_idx(origin_range_->get_group_idx());
  }
  return ret;
}

void ObMultipleSkipScanMerge::set_border_falg(const bool is_left, const blocksstable::ObDatumRange &src, blocksstable::ObDatumRange &dst)
{
  if (is_left) {
    if (src.is_left_open()) {
      dst.set_left_open();
    } else {
      dst.set_left_closed();
    }
  } else {
    if (src.is_right_open()) {
      dst.set_right_open();
    } else {
      dst.set_right_closed();
    }
  }
}

}
}

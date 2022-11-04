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
#include "storage/blocksstable/ob_sstable.h"
#include "ob_sstable_row_whole_scanner.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "ob_table_access_context.h"
#include "ob_table_access_param.h"
#include "ob_dml_param.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

void ObSSTableRowWholeScanner::MacroScanHandle::reset()
{
  macro_io_handle_.reset();
  is_left_border_ = false;
  is_right_border_ = false;
}

ObSSTableRowWholeScanner::~ObSSTableRowWholeScanner()
{
  if (nullptr != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = nullptr;
  }
}

void ObSSTableRowWholeScanner::reset()
{
  ObStoreRowIterator::reset();
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  sstable_ = nullptr;
  query_range_.reset();
  prefetch_macro_cursor_ = 0;
  cur_macro_cursor_ = 0;
  is_macro_prefetch_end_ = false;
  macro_block_iter_.reset();
  micro_block_iter_.reset();
  for (int64_t i = 0; i < PREFETCH_DEPTH; ++i) {
    scan_handles_[i].reset();
  }
  if (nullptr != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = nullptr;
  }
  allocator_.reset();
  is_inited_ = false;
}

void ObSSTableRowWholeScanner::reuse()
{
  ObStoreRowIterator::reuse();
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  sstable_ = nullptr;
  query_range_.reset();
  prefetch_macro_cursor_ = 0;
  cur_macro_cursor_ = 0;
  is_macro_prefetch_end_ = false;
  macro_block_iter_.reset();
  micro_block_iter_.reset();
  for (int64_t i = 0; i < PREFETCH_DEPTH; ++i) {
    scan_handles_[i].reset();
  }
  if (nullptr != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = nullptr;
  }
  allocator_.reuse();
  is_inited_ = false;
}

int ObSSTableRowWholeScanner::init_micro_scanner(const ObDatumRange *range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_UNLIKELY(nullptr == access_ctx_ || nullptr == iter_param_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpceted inner status to init micro scanner", K(ret), KP(range), KP(access_ctx_), KP(iter_param_));
  } else {
    const bool is_whole_macro_scan = access_ctx_->query_flag_.is_whole_macro_scan();
    const bool is_multi_version_minor_merge = access_ctx_->query_flag_.is_multi_version_minor_merge();
    if (OB_UNLIKELY(!is_whole_macro_scan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected query flag without whole macro scan", K(ret), KPC(access_ctx_));
    } else if (nullptr != range && is_multi_version_minor_merge && sstable_->is_multi_version_minor_sstable()) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultiVersionMicroBlockMinorMergeRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block scanner", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMultiVersionMicroBlockMinorMergeRowScanner(allocator_);
        int64_t rowkey_cnt = sstable_->is_multi_version_minor_sstable() ?
            iter_param_->get_schema_rowkey_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() : iter_param_->get_schema_rowkey_count();
        if (is_multi_version_range(*range, rowkey_cnt)) {
          query_range_ = *range;
        } else if (OB_FAIL(range->to_multi_version_range(allocator_, query_range_))) {
          STORAGE_LOG(WARN, "Failed to transfer multi version range", K(ret), KPC(range));
        }
      }
    } else if (nullptr != range && sstable_->is_multi_version_minor_sstable()) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultiVersionMicroBlockRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for multi version micro block scanner, ", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMultiVersionMicroBlockRowScanner(allocator_);
        const int64_t rowkey_cnt = iter_param_->get_schema_rowkey_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        if (is_multi_version_range(*range, rowkey_cnt)) {
          query_range_ = *range;
        } else if (OB_FAIL(range->to_multi_version_range(allocator_, query_range_))) {
          STORAGE_LOG(WARN, "Failed to transfer multi version range", K(ret), KPC(range));
        }
      }
    } else {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMicroBlockRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block scanner, ", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMicroBlockRowScanner(allocator_);
        if (nullptr != range) {
          query_range_ = *range;
        } else {
          query_range_.set_whole_range();
        }
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObSSTableRowWholeScanner has been opened, ", K(ret));
  } else if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(query_range), KP(table));
  } else {
    const ObTableReadInfo *index_read_info = iter_param.read_info_->get_index_read_info();
    const ObDatumRange *range = reinterpret_cast<const ObDatumRange *>(query_range);
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    sstable_ = static_cast<ObSSTable *>(table);
    prefetch_macro_cursor_ = 0;
    cur_macro_cursor_ = 0;

    if (OB_ISNULL(index_read_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null index read info", K(ret));
    } else if (OB_FAIL(init_micro_scanner(range))) {
      LOG_WARN("Failed to init micro scanner", K(ret));
    } else if (OB_FAIL(macro_block_iter_.open(
                *sstable_,
                query_range_,
                *index_read_info,
                *access_ctx.stmt_allocator_))) {
      LOG_WARN("Fail to open macro_block_iter ", K(ret));
    }

    // do prefetch
    for (int64_t i = 0; OB_SUCC(ret) && i < PREFETCH_DEPTH - 1; ++i) {
      if (OB_FAIL(prefetch())) {
        LOG_WARN("failed to do prefetch", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(open_macro_block())) {
        LOG_WARN("failed to open macro block", K(ret));
      } else {
        if (OB_FAIL(open_micro_block(true))) {
          LOG_WARN("failed to open micro block", K(ret));
        } else {
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      const ObDatumRange &query_range,
      const ObMacroBlockDesc &macro_desc,
      ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObSSTableRowWholeScanner has been opened", K(ret));
  } else if (OB_UNLIKELY(!iter_param.is_valid()) || OB_UNLIKELY(!macro_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(query_range), K(macro_desc), K(iter_param));
  } else {
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    sstable_ = &sstable;
    prefetch_macro_cursor_ = 0;
    cur_macro_cursor_ = 0;
    MacroScanHandle &scan_handle = scan_handles_[0];
    scan_handle.reset();

    if (OB_FAIL(init_micro_scanner(&query_range))) {
      LOG_WARN("Fail to init micro scanner", K(ret));
    } else {
      ObMacroBlockReadInfo read_info;
      scan_handle.is_left_border_ = true;
      scan_handle.is_right_border_ = true;
      read_info.macro_block_id_ = macro_desc.macro_block_id_;
      read_info.offset_ = 0;
      read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
      read_info.io_desc_.set_category(ObIOCategory::SYS_IO);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      if (OB_FAIL(ObBlockManager::async_read_block(read_info, scan_handle.macro_io_handle_))) {
        LOG_WARN("Fail to read macro block", K(ret), K(read_info));
      } else {
        ++prefetch_macro_cursor_;
        is_macro_prefetch_end_ = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(open_macro_block())) {
      LOG_WARN("Fail to open macro block", K(ret));
    } else {
      if (OB_FAIL(open_micro_block(true))) {
        LOG_WARN("Fail to open micro block", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::inner_get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (cur_macro_cursor_ >= prefetch_macro_cursor_) {
    ret = OB_ITER_END;
  } else {
    row = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner_->get_next_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row", K(ret), K(cur_macro_cursor_));
        } else if (OB_FAIL(open_micro_block(false))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to open next micro block", K(ret), K_(cur_macro_cursor));
          } else if (++cur_macro_cursor_ >= prefetch_macro_cursor_) {
            ret = OB_ITER_END;
          } else if (OB_FAIL(open_macro_block())) {
            LOG_WARN("failed to open macro block", K(ret),
                K_(cur_macro_cursor), K_(prefetch_macro_cursor));
          } else if (OB_FAIL(open_micro_block(false))) {
            LOG_WARN("failed to open next micro block", K(ret), K_(cur_macro_cursor));
          }
        }
      } else {
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is NULL", K(ret));
        } else {
          const_cast<blocksstable::ObDatumRow *>(row)->scan_index_ = 0;
        }
        break;
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::prefetch()
{
  int ret = OB_SUCCESS;
  if (is_macro_prefetch_end_) {
  } else {
    blocksstable::ObMacroBlockReadInfo read_info;
    const bool is_left_border = 0 == prefetch_macro_cursor_;
    MacroScanHandle &scan_handle = scan_handles_[prefetch_macro_cursor_ % PREFETCH_DEPTH];
    micro_block_iter_.reuse(); // reuse micro iter before release scan handle
    scan_handle.reset();
    if (OB_FAIL(macro_block_iter_.get_next_macro_block(read_info.macro_block_id_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_macro_prefetch_end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get_next_macro_block ", K(ret), K(macro_block_iter_));
      }
    } else {
      scan_handle.is_left_border_ = (0 == prefetch_macro_cursor_);
      scan_handle.is_right_border_ = false; // set right border correctly when open macro block
      read_info.offset_ = 0;
      read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
      read_info.io_desc_.set_category(common::ObIOCategory::SYS_IO);
      read_info.io_desc_.set_wait_event(common::ObWaitEventIds::DB_FILE_COMPACT_READ);
      if (OB_FAIL(ObBlockManager::async_read_block(read_info, scan_handle.macro_io_handle_))) {
        LOG_WARN("Fail to read macro block, ", K(ret), K(read_info));
      } else {
        ++prefetch_macro_cursor_;
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::open_macro_block()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prefetch())) {
    LOG_WARN("failed to prefetch macro block", K(ret));
  } else if (cur_macro_cursor_ < prefetch_macro_cursor_) {
    const int64_t io_timeout_ms = std::max(DEFAULT_IO_WAIT_TIME_MS, GCONF._data_storage_io_timeout / 1000);
    MacroScanHandle &scan_handle = scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
    scan_handle.is_right_border_ = (cur_macro_cursor_ == prefetch_macro_cursor_ - 1);
    if (OB_FAIL(scan_handle.macro_io_handle_.wait(io_timeout_ms))) {
      LOG_WARN("failed to read macro block from io", K(ret), K(io_timeout_ms));
    } else if (OB_FAIL(micro_block_iter_.open(
        scan_handle.macro_io_handle_.get_buffer(),
        scan_handle.macro_io_handle_.get_data_size(),
        query_range_,
        *(iter_param_->read_info_->get_index_read_info()),
        scan_handle.is_left_border_,
        scan_handle.is_right_border_))) {
      LOG_WARN("failed to open micro block iter", K(ret), K(scan_handle.macro_io_handle_));
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::open_micro_block(const bool is_first_open)
{
  int ret = OB_SUCCESS;
  if (cur_macro_cursor_ >= prefetch_macro_cursor_) {
    //do nothing
  } else if (is_first_open && OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
    LOG_WARN("failed to init micro scanner", K(ret));
  } else if (is_first_open && OB_FAIL(micro_scanner_->set_range(query_range_))) {
    LOG_WARN("failed to set range", K(ret), K(query_range_));
  } else {
    ObMicroBlockData block_data;
    MacroScanHandle &scan_handle = scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
    bool is_left_border = scan_handle.is_left_border_ && micro_block_iter_.is_left_border();
    bool is_right_border = scan_handle.is_right_border_ && micro_block_iter_.is_right_border();
    if (OB_FAIL(micro_block_iter_.get_next_micro_block_data(block_data))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to get micro block count", K(ret), K(scan_handle.macro_io_handle_));
      }
    } else if (OB_FAIL(micro_scanner_->open(
                scan_handle.macro_io_handle_.get_macro_id(),
                block_data,
                is_left_border,
                is_right_border))) {
      LOG_WARN("failed to open micro scanner", K(ret),
          K_(cur_macro_cursor), K(scan_handle.macro_io_handle_));
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::get_first_row_mvcc_info(
    bool &is_first_row,
    bool &is_shadow_row) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (typeid(*micro_scanner_) == typeid(ObMultiVersionMicroBlockMinorMergeRowScanner)) {
    if (OB_FAIL(reinterpret_cast<ObMultiVersionMicroBlockMinorMergeRowScanner *>(micro_scanner_)->
                get_first_row_mvcc_info(is_first_row, is_shadow_row))) {
      LOG_WARN("Fail to get row header info", K(ret));
    }
  }
  return ret;
}

}
}

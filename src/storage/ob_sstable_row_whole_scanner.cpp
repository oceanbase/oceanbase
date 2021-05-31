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

#include "share/config/ob_server_config.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "ob_sstable_row_whole_scanner.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
namespace storage {

void ObSSTableRowWholeScanner::MacroScanHandle::reset()
{
  macro_block_id_.reset();
  macro_io_handle_.reset();
  micro_infos_.reuse();
  meta_.reset();
  is_left_border_ = false;
  is_right_border_ = false;
}

int ObSSTableRowWholeScanner::MacroScanHandle::get_block_data(
    const int64_t cur_micro_cursor, ObMicroBlockData& block_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_micro_cursor < 0) || OB_UNLIKELY(cur_micro_cursor >= micro_infos_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid micro cursor", K(ret), K(cur_micro_cursor), K(micro_infos_.count()));
  } else {
    ObMicroBlockInfo& micro_info = micro_infos_.at(cur_micro_cursor);
    const char* buf = macro_io_handle_.get_buffer() + micro_info.offset_;
    bool is_compressed = false;
    if (!meta_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta should not be NULL", K(ret));
    } else if (OB_FAIL(
                   ObRecordHeaderV3::deserialize_and_check_record(buf, micro_info.size_, MICRO_BLOCK_HEADER_MAGIC))) {
      LOG_ERROR("micro block is corrupted", K(ret), K(micro_info), K(macro_io_handle_.get_macro_id()));
    } else if (OB_FAIL(macro_reader_.decompress_data(
                   meta_, buf, micro_info.size_, block_data.get_buf(), block_data.get_buf_size(), is_compressed))) {
      LOG_WARN("failed to decompress data", K(ret));
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::inner_open(
    const ObTableIterParam& iter_pram, ObTableAccessContext& access_ctx, ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObSSTableRowWholeScanner has been opened, ", K(ret));
  } else if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(query_range), KP(table));
  } else if (table->is_sstable() &&
             OB_FAIL(file_handle_.assign(static_cast<ObSSTable*>(table)->get_storage_file_handle()))) {
    LOG_WARN("fail to get file handle", K(ret), K(table->get_partition_key()));
  } else {
    iter_param_ = &iter_pram;
    access_ctx_ = &access_ctx;
    sstable_ = static_cast<ObSSTable*>(table);
    prefetch_macro_cursor_ = 0;
    cur_macro_cursor_ = 0;

    // alloc micro block scanner
    const ObExtStoreRange* range = reinterpret_cast<const ObExtStoreRange*>(query_range);
    void* buf = NULL;
    const bool is_whole_macro_scan = access_ctx.query_flag_.is_whole_macro_scan();
    const bool is_multi_version_minor_merge = access_ctx.query_flag_.is_multi_version_minor_merge();
    if (OB_UNLIKELY(!is_whole_macro_scan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is not whole scan", K(ret), K(access_ctx));
    } else if (is_multi_version_minor_merge && sstable_->is_multi_version_minor_sstable()) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultiVersionMicroBlockMinorMergeRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block scanner, ", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMultiVersionMicroBlockMinorMergeRowScanner();
        const int64_t multi_version_rowkey_col_cnt =
            iter_pram.rowkey_cnt_ +
            ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(sstable_->get_multi_version_rowkey_type());
        if (is_multi_version_range(range, multi_version_rowkey_col_cnt)) {
          query_range_ = *range;
        } else if (OB_FAIL(ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
                       *range, access_ctx.trans_version_range_, allocator_, query_range_))) {
          LOG_WARN("convert to multi version range failed", K(ret), K(*range));
        }
      }
    } else if (sstable_->is_multi_version_minor_sstable()) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultiVersionMicroBlockRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for multi version micro block scanner, ", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMultiVersionMicroBlockRowScanner();
        if (OB_FAIL(ObVersionStoreRangeConversionHelper::range_to_multi_version_range(
                *range, access_ctx.trans_version_range_, allocator_, query_range_))) {
          LOG_WARN("convert to multi version range failed", K(ret), K(*range));
        }
      }
    } else {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMicroBlockRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block scanner, ", K(ret));
      } else {
        micro_scanner_ = new (buf) ObMicroBlockRowScanner();
        query_range_ = *range;
      }
    }

    if (OB_SUCC(ret)) {
      macro_blocks_.reset();
      if (OB_FAIL(sstable_->find_macros(query_range_, macro_blocks_))) {
        LOG_WARN("failed to find macro block", K(ret), K(query_range_));
      }
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
        cur_micro_cursor_ = 0;
        if (OB_FAIL(open_micro_block(true))) {
          LOG_WARN("failed to open micro block", K(ret));
        } else {
          is_opened_ = true;
        }
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx,
    const common::ObExtStoreRange* query_range, const blocksstable::ObMacroBlockCtx& macro_block_ctx,
    ObSSTable* sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObSSTableRowWholeScanner has been opened, ", K(ret));
  } else if (OB_ISNULL(query_range) || !macro_block_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(query_range), K(macro_block_ctx));
  } else if (OB_FAIL(file_handle_.assign(sstable->get_storage_file_handle()))) {
    LOG_WARN("fail to get file handle", K(ret), K(sstable->get_storage_file_handle()));
  } else {
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    query_range_ = *query_range;
    sstable_ = sstable;
    prefetch_macro_cursor_ = 0;
    cur_macro_cursor_ = 0;

    // alloc micro block scanner
    void* buf = NULL;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMicroBlockRowScanner)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for micro block scanner, ", K(ret));
    } else {
      bool has_lob_column = false;
      micro_scanner_ = new (buf) ObMicroBlockRowScanner();
      if (OB_FAIL(iter_param.has_lob_column_out(false /*is get*/, has_lob_column))) {
        STORAGE_LOG(WARN, "fail to check has lob column out", K(ret));
      } else if (has_lob_column) {
        // TODO
        STORAGE_LOG(INFO, "Do not need replace lob column in this interface", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      macro_blocks_.reset();
      if (OB_FAIL(macro_blocks_.push_back(macro_block_ctx))) {
        LOG_WARN("Fail to push back macro id to array, ", K(ret), K(macro_block_ctx));
      } else if (OB_FAIL(prefetch())) {
        LOG_WARN("Fail to do prefetch, ", K(ret));
      } else if (OB_FAIL(open_macro_block())) {
        LOG_WARN("failed to open macro block", K(ret));
      } else {
        cur_micro_cursor_ = 0;
        if (OB_FAIL(open_micro_block(true))) {
          LOG_WARN("failed to open micro block", K(ret));
        } else {
          is_opened_ = true;
        }
      }
    }
  }
  return ret;
}

ObSSTableRowWholeScanner::~ObSSTableRowWholeScanner()
{
  if (NULL != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = NULL;
  }
}

void ObSSTableRowWholeScanner::reset()
{
  ObISSTableRowIterator::reset();
  iter_param_ = NULL;
  access_ctx_ = NULL;
  sstable_ = NULL;
  query_range_.reset();
  prefetch_macro_cursor_ = 0;
  cur_macro_cursor_ = 0;
  cur_micro_cursor_ = 0;
  macro_blocks_.reset();
  for (int64_t i = 0; i < PREFETCH_DEPTH; ++i) {
    scan_handles_[i].reset();
  }
  if (NULL != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = NULL;
  }
  allocator_.reset();
  is_opened_ = false;
  file_handle_.reset();
}

void ObSSTableRowWholeScanner::reuse()
{
  ObISSTableRowIterator::reuse();
  iter_param_ = NULL;
  access_ctx_ = NULL;
  sstable_ = NULL;
  query_range_.reset();
  prefetch_macro_cursor_ = 0;
  cur_macro_cursor_ = 0;
  cur_micro_cursor_ = 0;
  macro_blocks_.reset();
  for (int64_t i = 0; i < PREFETCH_DEPTH; ++i) {
    scan_handles_[i].reset();
  }
  if (NULL != micro_scanner_) {
    micro_scanner_->~ObIMicroBlockRowScanner();
    micro_scanner_ = NULL;
  }
  allocator_.reuse();
  is_opened_ = false;
  file_handle_.reset();
}

const ObIArray<ObRowkeyObjComparer*>* ObSSTableRowWholeScanner::get_rowkey_cmp_funcs()
{
  ObIArray<ObRowkeyObjComparer*>* cmp_funcs = nullptr;
  if (OB_NOT_NULL(sstable_) && sstable_->get_rowkey_helper().is_valid()) {
    cmp_funcs = &sstable_->get_rowkey_helper().get_compare_funcs();
  }
  return cmp_funcs;
}

int ObSSTableRowWholeScanner::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (cur_macro_cursor_ >= macro_blocks_.count()) {
    ret = OB_ITER_END;
  } else {
    row = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(batch_get_next_row(micro_scanner_, row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row", K(ret), K(cur_macro_cursor_));
        } else {
          ret = OB_SUCCESS;
          MacroScanHandle* scan_handle = &scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
          if (++cur_micro_cursor_ >= scan_handle->micro_infos_.count()) {
            // the scan of current macro block has finished, scan next macro block
            if (++cur_macro_cursor_ >= macro_blocks_.count()) {
              ret = OB_ITER_END;
            } else if (OB_FAIL(open_macro_block())) {
              LOG_WARN("failed to open macro block", K(ret), K(cur_macro_cursor_), K(cur_micro_cursor_));
            } else {
              cur_micro_cursor_ = 0;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(open_micro_block(false))) {
              LOG_WARN("failed to open micro block, ", K(ret), K(cur_macro_cursor_), K(cur_micro_cursor_));
            }
          }
        }
      } else {
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is NULL", K(ret));
        } else {
          const_cast<ObStoreRow*>(row)->scan_index_ = 0;
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
  if (prefetch_macro_cursor_ < macro_blocks_.count()) {
    const ObMacroBlockCtx& macro_block_ctx = macro_blocks_.at(prefetch_macro_cursor_);
    MacroScanHandle* scan_handle = &scan_handles_[prefetch_macro_cursor_ % PREFETCH_DEPTH];
    blocksstable::ObMacroBlockReadInfo read_info;
    blocksstable::ObStorageFile* file = NULL;
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "pg file is null", K(ret), KP(file));
    } else {
      scan_handle->reset();
      scan_handle->macro_block_id_ = macro_block_ctx.get_macro_block_id();
      scan_handle->is_left_border_ = (0 == prefetch_macro_cursor_);
      scan_handle->is_right_border_ = (macro_blocks_.count() - 1 == prefetch_macro_cursor_);
      read_info.macro_block_ctx_ = &macro_block_ctx;
      read_info.offset_ = 0;
      read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
      read_info.io_desc_.category_ = common::SYS_IO;
      read_info.io_desc_.wait_event_no_ = common::ObWaitEventIds::DB_FILE_COMPACT_READ;
      scan_handle->macro_io_handle_.set_file(file);
      if (OB_FAIL(file->async_read_block(read_info, scan_handle->macro_io_handle_))) {
        STORAGE_LOG(WARN, "Fail to read macro block, ", K(ret), K(read_info));
        //} else if (NULL != context_->ru_) {
        // context_->ru_->sstable_read_count_.mark();
        // context_->ru_->sstable_read_bytes_.mark(read_info.size_);
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
  ObMicroBlockIndexTransformer* transformer = GET_TSI_MULT(blocksstable::ObMicroBlockIndexTransformer, 1);
  if (OB_ISNULL(transformer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate transformer", K(ret));
  } else if (cur_macro_cursor_ < macro_blocks_.count()) {
    const int64_t io_timeout_ms = std::max(DEFAULT_IO_WAIT_TIME_MS, GCONF._data_storage_io_timeout / 1000);
    MacroScanHandle* scan_handle = &scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
    const ObMicroBlockIndexMgr* index_mgr = NULL;
    const char* buf = NULL;
    if (OB_FAIL(scan_handle->macro_io_handle_.wait(io_timeout_ms))) {
      LOG_ERROR("failed to read macro block from io", K(ret), K(io_timeout_ms));
    } else if (OB_ISNULL(buf = scan_handle->macro_io_handle_.get_buffer())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, the buffer must not be NULL", K(ret));
    } else if (OB_FAIL(sstable_->get_meta(scan_handle->macro_block_id_, scan_handle->meta_))) {
      LOG_WARN("fail to get meta", K(ret));
    } else if (!scan_handle->meta_.is_valid()) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, meta must not be null", K(ret));
    } else if (OB_FAIL(transformer->transform(
                   buf + scan_handle->meta_.meta_->micro_block_index_offset_, scan_handle->meta_, index_mgr))) {
      LOG_WARN("failed to init micro block index transformer", K(ret));
    } else if (OB_ISNULL(index_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, the index_mgr is NULL", K(ret));
    } else if (OB_FAIL(index_mgr->search_blocks(query_range_.get_range(),
                   scan_handle->is_left_border_,
                   scan_handle->is_right_border_,
                   scan_handle->micro_infos_,
                   get_rowkey_cmp_funcs()))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("failed to search micro blocks", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prefetch())) {
        LOG_WARN("failed to do prefetch", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::open_micro_block(const bool is_first_open)
{
  int ret = OB_SUCCESS;
  if (cur_macro_cursor_ >= macro_blocks_.count()) {
    // do nothing
  } else if (is_first_open && OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
    LOG_WARN("failed to init micro scanner", K(ret));
  } else if (is_first_open && OB_FAIL(micro_scanner_->set_range(query_range_.get_range()))) {
    LOG_WARN("failed to set range", K(ret), K(query_range_));
  } else {
    MacroScanHandle* scan_handle = &scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
    ObMicroBlockData block_data;
    bool is_left_border = scan_handle->is_left_border_ && (0 == cur_micro_cursor_);
    bool is_right_border =
        scan_handle->is_right_border_ && (scan_handle->micro_infos_.count() - 1 == cur_micro_cursor_);
    if (OB_FAIL(scan_handle->get_block_data(cur_micro_cursor_, block_data))) {
      LOG_WARN("failed to get block data", K(ret), K(cur_macro_cursor_), K(cur_micro_cursor_));
    } else if (OB_FAIL(micro_scanner_->open(
                   scan_handle->macro_block_id_, scan_handle->meta_, block_data, is_left_border, is_right_border))) {
      LOG_WARN("failed to open micro scanner",
          K(ret),
          K(scan_handle->macro_block_id_),
          K(cur_macro_cursor_),
          K(cur_micro_cursor_));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

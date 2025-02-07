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
  macro_block_desc_.reset();
  start_row_offset_ = 0;
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

int ObSSTableRowWholeScanner::alloc_io_buf(compaction::ObCompactionBuffer &io_buf, int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t size = common::OB_DEFAULT_MACRO_BLOCK_SIZE * PREFETCH_DEPTH;
  if (OB_LIKELY(io_buf.is_inited())) {
    if (OB_FAIL(io_buf.reserve(buf_size))) {
      LOG_WARN("fail to reserve io buf", K(ret), K(io_buf), K(buf_size));
    }
  } else if (OB_FAIL(io_buf.init(common::OB_DEFAULT_MACRO_BLOCK_SIZE, buf_size))) {
    LOG_WARN("fail to init io buf", K(ret), K(io_buf), K(buf_size));
  }
  return ret;
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
  last_micro_block_recycled_ = false;
  last_mvcc_row_already_output_ = false;
}

void ObSSTableRowWholeScanner::reuse()
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
  allocator_.reuse();
  is_inited_ = false;
  last_micro_block_recycled_ = false;
  last_mvcc_row_already_output_ = false;
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
    const bool is_bare_row_scan = access_ctx_->query_flag_.is_bare_row_scan();
    if (OB_UNLIKELY(!is_whole_macro_scan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected query flag without whole macro scan", K(ret), KPC(access_ctx_));
    } else if (is_bare_row_scan) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMicroBlockRowDirectScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc mem for micro block scanner failed", K(ret));
      } else {
        micro_scanner_ = new(buf) ObMicroBlockRowDirectScanner(allocator_);
        if (nullptr != range) {
          query_range_ = *range;
        } else {
          query_range_.set_whole_range();
        }
      }
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("failed to init micro scanner", K(ret));
    } else if (OB_FAIL(micro_scanner_->set_range(query_range_))) {
      LOG_WARN("failed to set range", K(ret), K(query_range_));
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
    const ObDatumRange *range = reinterpret_cast<const ObDatumRange *>(query_range);
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    sstable_ = static_cast<ObSSTable *>(table);
    prefetch_macro_cursor_ = 0;
    cur_macro_cursor_ = 0;
    last_mvcc_row_already_output_ = true;
    const ObITableReadInfo *rowkey_read_info = nullptr;

    if (table->is_normal_cg_sstable()) {
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(rowkey_read_info))) {
        STORAGE_LOG(WARN, "unexpected null index read info", K(ret));
      }
    } else {
      rowkey_read_info = iter_param.read_info_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_micro_scanner(range))) {
      LOG_WARN("Failed to init micro scanner", K(ret));
    } else if (OB_FAIL(macro_block_iter_.open(
                *sstable_,
                query_range_,
                *rowkey_read_info,
                allocator_))) {
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
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to open macro block", K(ret));
        }
      } else if (OB_FAIL(open_next_valid_micro_block())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to open next valid micro block", K(ret));
        }
      }

      if (OB_SUCCESS == ret || OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_inited_ = true;
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
    ObSSTable &sstable,
    const bool last_mvcc_row_already_output)
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
    last_mvcc_row_already_output_ = last_mvcc_row_already_output;
    MacroScanHandle &scan_handle = scan_handles_[0];
    scan_handle.reset();

    if (OB_FAIL(alloc_io_buf(io_buf_[0], sstable_->get_macro_read_size()))) {
      LOG_WARN("alloc io buffers failed", K(ret), K(sstable_->get_macro_read_size()));
    } else if (OB_FAIL(init_micro_scanner(&query_range))) {
      LOG_WARN("Fail to init micro scanner", K(ret));
    } else {
      ObStorageObjectReadInfo read_info;
      scan_handle.start_row_offset_ = macro_desc.start_row_offset_;
      scan_handle.is_left_border_ = true;
      scan_handle.is_right_border_ = true;
      scan_handle.macro_block_desc_ = macro_desc;

      read_info.macro_block_id_ = macro_desc.macro_block_id_;
      read_info.offset_ = sstable_->get_macro_offset();
      read_info.size_ = sstable_->get_macro_read_size();
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_WHOLE_SCANNER_IO);
      read_info.buf_ = io_buf_[0].data();
      read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
      read_info.mtl_tenant_id_ = MTL_ID();

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObObjectManager::async_read_object(read_info, scan_handle.macro_io_handle_))) {
        LOG_WARN("Fail to read macro block", K(ret), K(read_info));
      } else {
        ++prefetch_macro_cursor_;
        is_macro_prefetch_end_ = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(open_macro_block())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Fail to open macro block", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(open_next_valid_micro_block())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Fail to open next valid micro block", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::set_ignore_shadow_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(micro_scanner_ == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null micro scanner", K(ret));
  } else if (OB_FAIL(micro_scanner_->set_ignore_shadow_row())) {
    STORAGE_LOG(WARN, "fail to set ignore shadow row", K(ret));
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
        } else if (OB_FAIL(open_next_valid_micro_block())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to open next valid micro block", K(ret), K_(cur_macro_cursor));
          }
        }
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is NULL", K(ret));
      } else {
        const_cast<blocksstable::ObDatumRow *>(row)->scan_index_ = 0;
        break;
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::open_next_valid_micro_block()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(open_micro_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to open next micro block", K(ret), K_(cur_macro_cursor));
      } else if (++cur_macro_cursor_ >= prefetch_macro_cursor_) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(open_macro_block())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to open macro block", K(ret), K_(cur_macro_cursor), K_(prefetch_macro_cursor));
        }
      }
    } else if (OB_UNLIKELY(last_micro_block_recycled_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected micro block status", K(ret), K_(cur_macro_cursor), KPC(this));
    } else {
      break;
    }
  }
  return ret;
}

void ObSSTableRowWholeScanner::reset_query_range()
{
  query_range_.reset();
}

int ObSSTableRowWholeScanner::switch_query_range(const blocksstable::ObDatumRange &query_range)
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(query_range_ = query_range)) {
  } else if (OB_FAIL(micro_scanner_->set_range(query_range_))) {
    LOG_WARN("failed to set range", K(ret), K(query_range_));
  } else if (OB_FAIL(open_macro_block())) {
    STORAGE_LOG(WARN, "fail to open macro block", K(ret));
  } else if (OB_FAIL(open_next_valid_micro_block())) {
    LOG_WARN("Fail to open next valid micro block", K(ret));
  }
  return ret;
}

int ObSSTableRowWholeScanner::prefetch()
{
  int ret = OB_SUCCESS;
  if (is_macro_prefetch_end_) {
  } else {
    blocksstable::ObStorageObjectReadInfo read_info;
    const bool is_left_border = 0 == prefetch_macro_cursor_;
    int64_t io_index = prefetch_macro_cursor_ % PREFETCH_DEPTH;
    MacroScanHandle &scan_handle = scan_handles_[io_index];
    micro_block_iter_.reuse(); // reuse micro iter before release scan handle
    scan_handle.reset();
    if (OB_FAIL(macro_block_iter_.get_next_macro_block(scan_handle.macro_block_desc_))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_macro_prefetch_end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get_next_macro_block ", K(ret), K(macro_block_iter_));
      }
    } else if (OB_FAIL(alloc_io_buf(io_buf_[io_index], sstable_->get_macro_read_size()))) {
      LOG_WARN("alloc io buffers failed", K(ret), K(sstable_->get_macro_read_size()));
    } else {
      scan_handle.is_left_border_ = (0 == prefetch_macro_cursor_);
      scan_handle.is_right_border_ = false; // set right border correctly when open macro block
      scan_handle.start_row_offset_ = scan_handle.macro_block_desc_.start_row_offset_;

      read_info.macro_block_id_ = scan_handle.macro_block_desc_.macro_block_id_;
      read_info.offset_ = sstable_->get_macro_offset();
      read_info.size_ = sstable_->get_macro_read_size();
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(common::ObWaitEventIds::DB_FILE_COMPACT_READ);
      read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_WHOLE_SCANNER_IO);
      read_info.buf_ = io_buf_[io_index].data();
      read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
      read_info.mtl_tenant_id_ = MTL_ID();

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObObjectManager::async_read_object(read_info, scan_handle.macro_io_handle_))) {
        LOG_WARN("Fail to read macro block, ", K(ret), K(read_info));
      } else {
        ++prefetch_macro_cursor_;
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::check_macro_block_recycle(const ObMacroBlockDesc &macro_desc, bool &can_recycle)
{
  int ret = OB_SUCCESS;
  can_recycle = false;
  if (!access_ctx_->query_flag_.is_multi_version_minor_merge()) {
  } else if (OB_UNLIKELY(!macro_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid block data", K(ret), K(macro_desc));
  } else if (!macro_desc.contain_uncommitted_row_ &&
             macro_desc.max_merged_trans_version_ <= access_ctx_->trans_version_range_.base_version_ &&
             (last_micro_block_recycled_ || last_mvcc_row_already_output_)) {
    can_recycle = true;
  }
  // TODO: @dengzhi.ldz enable recycle after making adaptor for migration
  can_recycle = false;
  return ret;
}

int ObSSTableRowWholeScanner::open_macro_block()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(prefetch())) {
      LOG_WARN("failed to prefetch macro block", K(ret));
    } else if (cur_macro_cursor_ >= prefetch_macro_cursor_) {
      ret = OB_ITER_END;
    } else {
      bool can_recycle = false;
      MacroScanHandle &scan_handle = scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
      ObDatumRange range;
      const bool is_need_trans_range = sstable_->is_normal_cg_sstable() && 0 != scan_handle.start_row_offset_;
      scan_handle.is_right_border_ = (cur_macro_cursor_ == prefetch_macro_cursor_ - 1);
      const ObITableReadInfo *rowkey_read_info = nullptr;
      micro_block_iter_.reset();

      if (OB_FAIL(iter_param_->get_index_read_info(sstable_->is_normal_cg_sstable(), rowkey_read_info))) {
        STORAGE_LOG(WARN, "unexpected null index read info", K(ret), K(sstable_->is_normal_cg_sstable()));
      } else if (is_need_trans_range && OB_FAIL(rowkey_helper_.trans_to_cg_range(scan_handle.start_row_offset_, query_range_))) {
        LOG_WARN("failed to trans cg range", K(ret), K(query_range_), K(scan_handle.start_row_offset_));
      } else if (FALSE_IT(range = is_need_trans_range ? rowkey_helper_.get_result_range() : query_range_)) {
      } else  if (access_ctx_->query_flag_.is_multi_version_minor_merge() &&
          OB_FAIL(check_macro_block_recycle(scan_handle.macro_block_desc_, can_recycle))) {
        LOG_WARN("failed to check macro block recycle", K(ret), K(cur_macro_cursor_));
      } else if (can_recycle) {
        last_micro_block_recycled_ = true;
        cur_macro_cursor_++;
        FLOG_INFO("macro block recycled", K(scan_handle.macro_block_desc_.macro_block_id_));
      } else if (OB_FAIL(scan_handle.macro_io_handle_.wait())) {
        LOG_WARN("failed to read macro block from io", K(ret));
      } else if (OB_FAIL(micro_block_iter_.open(
                  scan_handle.macro_io_handle_.get_buffer(),
                  scan_handle.macro_io_handle_.get_data_size(),
                  range,
                  *rowkey_read_info,
                  scan_handle.is_left_border_,
                  scan_handle.is_right_border_))) {
        LOG_WARN("failed to open micro block iter", K(ret), K(scan_handle.macro_io_handle_));
      } else {
        if (iter_macro_cnt_ < 10) {
          LOG_TRACE("iter macro block id", K(scan_handle.macro_block_desc_.macro_block_id_), K(iter_macro_cnt_++), K(sstable_));
        }
        break;
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::check_micro_block_recycle(const ObMicroBlockHeader &micro_header, bool &can_recycle)
{
  int ret = OB_SUCCESS;
  can_recycle = false;
  if (!access_ctx_->query_flag_.is_multi_version_minor_merge()) {
  } else {
    if (!micro_header.contain_uncommitted_rows() &&
        micro_header.max_merged_trans_version_ <= access_ctx_->trans_version_range_.base_version_ &&
        (last_micro_block_recycled_ || last_mvcc_row_already_output_)) {
      can_recycle = true;
    }
  }
  // TODO: @dengzhi.ldz enable recycle after making adaptor for migration
  can_recycle = false;
  return ret;
}

int ObSSTableRowWholeScanner::open_micro_block()
{
  int ret = OB_SUCCESS;

  if (sstable_->is_normal_cg_sstable()) {
    if (OB_FAIL(open_cg_micro_block()) && OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to open cg micro block", K(ret));
    }
  } else {
    ObMicroBlockData block_data;
    bool can_recycle = false;
    while (OB_SUCC(ret)) {
    MacroScanHandle &scan_handle = scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
    bool is_left_border = scan_handle.is_left_border_ && micro_block_iter_.is_left_border();
    bool is_right_border = scan_handle.is_right_border_ && micro_block_iter_.is_right_border();
    const ObMicroBlockHeader *micro_header = nullptr;
    if (OB_FAIL(micro_block_iter_.get_next_micro_block_data(block_data))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to get micro block count", K(ret), K(scan_handle.macro_io_handle_));
      }
    } else if (OB_ISNULL(micro_header = block_data.get_micro_header()))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpceted null micro header", K(ret), K(micro_header));
    } else if (access_ctx_->query_flag_.is_multi_version_minor_merge() &&
        OB_FAIL(check_micro_block_recycle(*micro_header, can_recycle))) {
      LOG_WARN("failed to check micro block recycle", K(ret));
    } else if (can_recycle) {
      last_micro_block_recycled_ = true;
      last_mvcc_row_already_output_ = micro_header->is_last_row_last_flag();
      FLOG_INFO("micro block recycled", KPC(micro_header));
    } else if (OB_FAIL(micro_scanner_->open(
            scan_handle.macro_io_handle_.get_macro_id(),
            block_data,
            is_left_border,
            is_right_border))) {
      LOG_WARN("failed to open micro scanner", K(ret),
          K_(cur_macro_cursor), K(scan_handle.macro_io_handle_));
    } else {
      if (last_micro_block_recycled_ && !last_mvcc_row_already_output_) {
        if (OB_FAIL(recycle_last_rowkey_in_micro_block())) {
          LOG_WARN("Fail to recycle left rows of last macro", K(ret),  "macor_id", scan_handle.macro_io_handle_.get_macro_id());
        }
      }
      last_micro_block_recycled_ = false;
      last_mvcc_row_already_output_ = micro_header->is_last_row_last_flag();
      break;
    }
  }
  }
  return ret;
}

int ObSSTableRowWholeScanner::recycle_last_rowkey_in_micro_block()
{
  int ret = OB_SUCCESS;
  bool is_rowkey_first_row = false;
  bool is_rowkey_first_shadow_row = false;
  if (OB_UNLIKELY(!access_ctx_->query_flag_.is_multi_version_minor_merge())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid block data", K(ret), KPC_(access_ctx));
  } else if (OB_FAIL(reinterpret_cast<ObMultiVersionMicroBlockMinorMergeRowScanner *>(micro_scanner_)->
                     get_first_row_mvcc_info(is_rowkey_first_row, is_rowkey_first_shadow_row))) {
    LOG_WARN("Fail to get row header info", K(ret));
  } else if (OB_UNLIKELY(is_rowkey_first_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpceted first row flag", K(ret));
  } else {
    // recycle left rows of the last rowkey in current micro block
    const blocksstable::ObDatumRow *row = nullptr;
    int64_t trans_col_index = iter_param_->get_schema_rowkey_count();
    int64_t recycle_version = access_ctx_->trans_version_range_.base_version_;
    while(OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner_->get_next_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to get next row", K(ret), K(cur_macro_cursor_));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpceted meet end of the micro scanner", K(ret));
        }
      } else if (OB_UNLIKELY(row->is_uncommitted_row() ||
                             -row->storage_datums_[trans_col_index].get_int() > recycle_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected trans version in row", K(ret), K(recycle_version), KPC(row));
      } else if (row->is_last_multi_version_row()) {
        break;
      }
    }
  }
  return ret;
}

int ObSSTableRowWholeScanner::open_cg_micro_block()
{
  int ret = OB_SUCCESS;
  ObMicroBlockData block_data;
  int64_t micro_block_start_row_offset = -1;
  const MacroScanHandle &scan_handle = scan_handles_[cur_macro_cursor_ % PREFETCH_DEPTH];
  const bool is_left_border = scan_handle.is_left_border_ && micro_block_iter_.is_left_border();
  const bool is_right_border = scan_handle.is_right_border_ && micro_block_iter_.is_right_border();

  if (!query_range_.is_whole_range() && OB_FAIL(micro_block_iter_.get_curr_start_row_offset(micro_block_start_row_offset))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "failed to get prev row offset", K(ret), K(micro_block_iter_));
    }
  } else if (OB_FAIL(micro_block_iter_.get_next_micro_block_data(block_data))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Fail to get micro block count", K(ret), K(scan_handle.macro_io_handle_));
    }
  } else {
    ObCSRange range;
    if (OB_FAIL(get_cs_range(is_left_border, is_right_border,
                              scan_handle.start_row_offset_, micro_block_start_row_offset, range))) {
      STORAGE_LOG(WARN, "failed to get cs range", K(ret), K(scan_handle), K(micro_block_start_row_offset));
    } else if (OB_UNLIKELY(!range.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid range", K(ret), K(range), K(is_left_border), K(is_right_border), K(scan_handle.start_row_offset_), K(micro_block_start_row_offset));
    } else if (OB_FAIL(micro_scanner_->open_column_block(scan_handle.macro_io_handle_.get_macro_id(), block_data, range))) {
      STORAGE_LOG(WARN, "failed to open column block", K(ret), K(scan_handle), K(block_data), K(range));
    }
  }

  return ret;
}

int ObSSTableRowWholeScanner::get_cs_range(
    const bool is_left_border,
    const bool is_right_border,
    const int64_t macro_block_start_row_offset,
    const int64_t micro_block_start_row_offset,
    ObCSRange &range)
{
  int ret = OB_SUCCESS;
  range.start_row_id_ = 0;
  range.end_row_id_ = INT64_MAX;

  if (is_left_border && !query_range_.start_key_.is_min_rowkey()) {
    if (OB_UNLIKELY(query_range_.start_key_.is_static_rowkey() || query_range_.start_key_.datum_cnt_ != 1)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpcted query range", K(ret), K(query_range_));
    } else {
      const int64_t range_start_row_offset = query_range_.is_left_closed() ?
                                           query_range_.start_key_.datums_[0].get_int() : query_range_.start_key_.datums_[0].get_int() + 1;
      range.start_row_id_ = MAX(0, range_start_row_offset - macro_block_start_row_offset - micro_block_start_row_offset);
    }
  }

  if (OB_SUCC(ret) && is_right_border && !query_range_.end_key_.is_max_rowkey()) {
    if (OB_UNLIKELY(query_range_.end_key_.is_static_rowkey() || query_range_.end_key_.datum_cnt_ != 1)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpcted query range", K(ret), K(query_range_));
    } else {
      const int64_t range_end_row_offset = query_range_.is_right_closed() ?
                                           query_range_.end_key_.datums_[0].get_int() : query_range_.end_key_.datums_[0].get_int() - 1;
      range.end_row_id_ =  range_end_row_offset - macro_block_start_row_offset - micro_block_start_row_offset;
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

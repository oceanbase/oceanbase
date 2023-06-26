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
#include "ob_sstable_row_scanner.h"
#include "ob_block_row_store.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
template<typename PrefetchType>
ObSSTableRowScanner<PrefetchType>::~ObSSTableRowScanner()
{
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_scanner_, ObIMicroBlockRowScanner);
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reset()
{
  ObStoreRowIterator::reset();
  FREE_PTR_FROM_CONTEXT(access_ctx_, micro_scanner_, ObIMicroBlockRowScanner);
  is_opened_ = false;
  cur_range_idx_ = -1;
  sstable_ = nullptr;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  prefetcher_.reset();
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reuse()
{
  ObStoreRowIterator::reuse();
  is_opened_ = false;
  cur_range_idx_ = -1;
  if (nullptr != micro_scanner_) {
    micro_scanner_->reuse();
  }
  if (nullptr != block_row_store_) {
    block_row_store_->reuse();
  }
  prefetcher_.reuse();
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObSSTableRowScanner has been opened", K(ret));
  } else if (OB_UNLIKELY(nullptr == query_range ||
                         nullptr == table ||
                         !table->is_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObSSTableRowScanner", K(ret), KP(query_range), KP(table));
  } else {
    sstable_ = static_cast<ObSSTable *>(table);
    iter_param_ = &iter_param;
    access_ctx_ = &access_ctx;
    if (!prefetcher_.is_valid()) {
      if (OB_FAIL(prefetcher_.init(
                  type_, *sstable_, iter_param, access_ctx, query_range))) {
        LOG_WARN("fail to init prefetcher, ", K(ret));
      }
    } else if (OB_FAIL(prefetcher_.switch_context(
        type_, *sstable_, iter_param.get_read_info()->get_datum_utils(), access_ctx, query_range))) {
      LOG_WARN("fail to switch context for prefetcher, ", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (iter_param_->enable_pd_aggregate() && nullptr != block_row_store_ && !sstable_->is_multi_version_table()) {
        prefetcher_.agg_row_store_ = reinterpret_cast<ObAggregatedStore *>(block_row_store_);
      }
      if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("ObSSTableRowScanner prefetch failed", K(ret));
      } else {
        is_opened_ = true;
      }
    }
  }

  if (OB_UNLIKELY(!is_opened_)) {
    reset();
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::init_micro_scanner()
{
  int ret = OB_SUCCESS;
  if (nullptr != micro_scanner_) {
  } else {
    if (sstable_->is_multi_version_minor_sstable()) {
      if (nullptr == (micro_scanner_ = OB_NEWx(ObMultiVersionMicroBlockRowScanner,
                                               access_ctx_->stmt_allocator_,
                                               *access_ctx_->stmt_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block row scanner", K(ret));
      }
    } else {
      if (nullptr == (micro_scanner_ = OB_NEWx(ObMicroBlockRowScanner,
                                               access_ctx_->stmt_allocator_,
                                               *access_ctx_->stmt_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block row scanner", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Fail to init micro scanner", K(ret));
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::open_cur_data_block(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (prefetcher_.cur_micro_data_fetch_idx_ < read_handle.micro_begin_idx_ ||
      prefetcher_.cur_micro_data_fetch_idx_ > read_handle.micro_end_idx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(prefetcher_), K(read_handle));
  } else {
    blocksstable::ObMicroIndexInfo &micro_info = prefetcher_.current_micro_info();
    ObMicroBlockDataHandle &micro_handle = prefetcher_.current_micro_handle();
    if (nullptr == micro_scanner_) {
      if (OB_FAIL(init_micro_scanner())) {
        LOG_WARN("fail to init micro scanner", K(ret));
      }
    } else if (OB_UNLIKELY(!micro_scanner_->is_valid())) {
      if (OB_FAIL(micro_scanner_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
        LOG_WARN("Failed to switch new table context", K(ret), KPC(access_ctx_));
      }
    }

    if (OB_SUCC(ret)) {
      // init scanner
      if (prefetcher_.cur_micro_data_fetch_idx_ == read_handle.micro_begin_idx_ &&
          cur_range_idx_ != read_handle.range_idx_) {
        LOG_DEBUG("[INDEX BLOCK] begin to init micro block scanner", K(ret), K(read_handle),
                  K(prefetcher_.cur_micro_data_fetch_idx_), K(cur_range_idx_));
        micro_scanner_->reuse();
        if (OB_FAIL(micro_scanner_->set_range(*read_handle.range_))) {
          LOG_WARN("Fail to init micro scanner", K(ret), K(read_handle));
        } else {
          cur_range_idx_ = read_handle.range_idx_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool can_blockscan = false;
      ObMicroBlockData block_data;
      if (OB_FAIL(micro_handle.get_data_block_data(macro_block_reader_, block_data))) {
        LOG_WARN("Fail to get block data", K(ret), K(micro_handle));
      } else if (OB_FAIL(micro_scanner_->open(
                  micro_handle.macro_block_id_,
                  block_data,
                  micro_info.is_left_border(),
                  micro_info.is_right_border()))) {
        LOG_WARN("Fail to open micro_scanner", K(ret), K(micro_info), K(micro_handle), KPC(this));
      } else if (OB_FAIL(prefetcher_.check_blockscan(can_blockscan))) {
        LOG_WARN("Fail to check_blockscan", K(ret));
      } else if (can_blockscan && nullptr != block_row_store_ && !block_row_store_->is_disabled()) {
        // Apply pushdown filter and block scan
        if (OB_FAIL(micro_scanner_->apply_blockscan(block_row_store_, access_ctx_->table_store_stat_))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to apply_block_scan", K(ret), KPC(block_row_store_));
          } else {
            ret = OB_SUCCESS;
          }
        }
        EVENT_INC(ObStatEventIds::BLOCKSCAN_BLOCK_CNT);
        LOG_TRACE("[PUSHDOWN] pushdown for block scan", K(prefetcher_.cur_micro_data_fetch_idx_), K(micro_info), KPC(block_row_store_));
      }
      if (OB_SUCC(ret)) {
        ++access_ctx_->table_store_stat_.micro_access_cnt_;
        LOG_DEBUG("Success to open micro block", K(ret), K(read_handle), K(prefetcher_.cur_micro_data_fetch_idx_),
                  K(micro_info), K(micro_handle), KPC(this), K(common::lbt()));
      }
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::inner_get_next_row(const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableRowScanner has not been opened", K(ret));
  } else {
    while(OB_SUCC(ret)) {
      if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
      } else if (prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_) {
        if (OB_LIKELY(prefetcher_.is_prefetch_end_)) {
          ret = OB_ITER_END;
          if (nullptr != block_row_store_) {
            block_row_store_->reset_blockscan();
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), K_(prefetcher));
        }
      } else if (prefetcher_.read_wait()) {
        continue;
      } else if (OB_FAIL(fetch_row(prefetcher_.current_read_handle(), store_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          if (prefetcher_.cur_range_fetch_idx_ < prefetcher_.prefetching_range_idx() || prefetcher_.is_prefetch_end_) {
            ++prefetcher_.cur_range_fetch_idx_;
          }
          ret = OB_SUCCESS;
        } else if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret)) {
          LOG_WARN("Fail to fetch row", K(ret), KPC(this), KPC_(sstable), KPC_(iter_param), KPC_(access_ctx));
        }
      } else {
        break;
      }
    }

    if (OB_SUCC(ret) && NULL != store_row) {
      ObDatumRow &datum_row = *const_cast<ObDatumRow *>(store_row);
      if (!store_row->row_flag_.is_not_exist() &&
          iter_param_->need_scn_ &&
          OB_FAIL(set_row_scn(*iter_param_, *sstable_, store_row))) {
        LOG_WARN("failed to set row scn", K(ret));
      }
      EVENT_INC(ObStatEventIds::SSSTORE_READ_ROW_COUNT);
      LOG_DEBUG("[INDEX BLOCK] inner get next row", KPC(store_row));
    }
  }
      LOG_DEBUG("chaser debug", K(ret), KPC(store_row), KPC(iter_param_));
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::fetch_row(ObSSTableReadHandle &read_handle, const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(read_handle.is_get_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected get in scan", K(ret), K(read_handle));
  } else if (-1 == read_handle.micro_begin_idx_) {
    // empty range
    ret = OB_ITER_END;
    LOG_DEBUG("[INDEX BLOCK] scan empty read handle", K(prefetcher_), K(read_handle));
  } else {
    bool need_open_micro = false;
    if (-1 == prefetcher_.cur_micro_data_fetch_idx_ ||
        cur_range_idx_ != read_handle.range_idx_) {
      LOG_DEBUG("[INDEX BLOCK] begin to fetch row", K(cur_range_idx_),
                K(prefetcher_.cur_micro_data_fetch_idx_), K(read_handle));
      prefetcher_.cur_micro_data_fetch_idx_ = read_handle.micro_begin_idx_;
      need_open_micro = true;
    }
    if (need_open_micro) {
      if (OB_FAIL(open_cur_data_block(read_handle))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to open cur data block", K(ret), KPC(this));
        }
      } else if (can_vectorize()) {
        ret = OB_PUSHDOWN_STATUS_CHANGED;
        LOG_TRACE("[Vectorized|Aggregate] pushdown status changed, fuse=>pushdown", K(ret),
                  K(prefetcher_.cur_micro_data_fetch_idx_));
      }
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner_->get_next_row(store_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next row", K(ret));
        } else if (prefetcher_.cur_micro_data_fetch_idx_ >= read_handle.micro_end_idx_) {
          ret = OB_ITER_END;
          LOG_DEBUG("[INDEX BLOCK] Open data block handle iter end", K(ret),
                    K(prefetcher_.cur_micro_data_fetch_idx_), K(read_handle));
        } else if (FALSE_IT(++prefetcher_.cur_micro_data_fetch_idx_)) {
        } else if (OB_FAIL(open_cur_data_block(read_handle))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to open cur data block", K(ret), KPC(this));
          }
        } else if (can_vectorize()) {
          ret = OB_PUSHDOWN_STATUS_CHANGED;
          LOG_TRACE("[Vectorized|Aggregate] pushdown status changed, fuse=>pushdown", K(ret),
                    K(prefetcher_.cur_micro_data_fetch_idx_));
        }
      } else {
        (const_cast<ObDatumRow*> (store_row))->scan_index_ = read_handle.range_idx_;
        break;
      }
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::refresh_blockscan_checker(const blocksstable::ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (nullptr != block_row_store_ &&
      OB_FAIL(prefetcher_.refresh_blockscan_checker(prefetcher_.cur_micro_data_fetch_idx_ + 1, rowkey))) {
    LOG_WARN("Failed to prepare blockscan check info", K(ret), K(rowkey), KPC(this));
  }
  return ret;
}

template<typename PrefetchType>
bool ObSSTableRowScanner<PrefetchType>::can_vectorize() const
{
  return (iter_param_->vectorized_enabled_ || iter_param_->enable_pd_aggregate()) &&
      nullptr != block_row_store_ && block_row_store_->filter_applied();
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::get_next_rows()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_ || nullptr == block_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObSSTableRowScanner has not been opened or init", K(ret), K_(is_opened), KP_(block_row_store));
  } else {
    while (OB_SUCC(ret) && !block_row_store_->is_end()) {
      // scan macro blocks
      if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to do prefetch", K(ret), K_(prefetcher));
      } else if (prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_) {
        if (OB_LIKELY(prefetcher_.is_prefetch_end_)) {
          ret = OB_ITER_END;
          block_row_store_->reset_blockscan();
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), K_(prefetcher));
        }
      } else if (prefetcher_.read_wait()) {
        continue;
      } else if (OB_FAIL(fetch_rows(prefetcher_.current_read_handle()))) {
        if (OB_ITER_END == ret) {
          if (prefetcher_.cur_range_fetch_idx_ < prefetcher_.prefetching_range_idx() || prefetcher_.is_prefetch_end_) {
            ++prefetcher_.cur_range_fetch_idx_;
          }
          ret = OB_SUCCESS;
        } else if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret)) {
          LOG_WARN("Fail to fetch row", K(ret), K_(prefetcher));
        }
      } else {
        // block scan is not effective or vector store ended
        break;
      }
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::fetch_rows(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (-1 == read_handle.micro_begin_idx_) {
    // empty range
    ret = OB_ITER_END;
    LOG_DEBUG("[INDEX BLOCK] scan empty read handle", K(prefetcher_), K(read_handle));
  } else {
    bool need_open_micro = false;
    if (-1 == prefetcher_.cur_micro_data_fetch_idx_ ||
        cur_range_idx_ != read_handle.range_idx_) {
      LOG_DEBUG("[INDEX BLOCK] begin to fetch row", K(cur_range_idx_),
                K(prefetcher_.cur_micro_data_fetch_idx_), K(read_handle));
      prefetcher_.cur_micro_data_fetch_idx_ = read_handle.micro_begin_idx_;
      need_open_micro = true;
    }
    if (need_open_micro) {
      if (OB_FAIL(open_cur_data_block(read_handle))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to open cur data block", K(ret), KPC(this));
        }
      } else if (!can_vectorize()) {
        ret = OB_PUSHDOWN_STATUS_CHANGED;
        LOG_TRACE("[Vectorized] pushdown status changed, pushdown=>fuse", K(ret),
                  K(prefetcher_.cur_micro_data_fetch_idx_));
      }
    }

    bool need_prefetch = false;
    while (OB_SUCC(ret) && !block_row_store_->is_end()) {
      if (OB_FAIL(micro_scanner_->get_next_rows())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next row", K(ret));
        } else if (prefetcher_.cur_micro_data_fetch_idx_ >= read_handle.micro_end_idx_) {
          ret = OB_ITER_END;
          LOG_DEBUG("[INDEX BLOCK] Open data block handle iter end", K(ret),
                    K(prefetcher_.cur_micro_data_fetch_idx_), K(read_handle));
        } else if (FALSE_IT(++prefetcher_.cur_micro_data_fetch_idx_)) {
        } else if (OB_FAIL(open_cur_data_block(read_handle))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to open cur data block", K(ret), KPC(this));
          }
        } else if (need_prefetch && OB_FAIL(prefetcher_.prefetch())) {
          LOG_WARN("Fail to do prefetch", K(ret), K_(prefetcher));
        } else if (!can_vectorize()) {
          ret = OB_PUSHDOWN_STATUS_CHANGED;
          LOG_TRACE("[Vectorized] pushdown status changed, pushdown=>fuse", K(ret),
                    K(prefetcher_.cur_micro_data_fetch_idx_));
        } else {
          // should do prefetch as all the prefetched micros may be read
          need_prefetch = iter_param_->enable_pd_aggregate();
        }
      }
    }
  }
  return ret;
}

// Explicit instantiations.
template class ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<32, 3>>;
template class ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<2, 2>>;

}
}

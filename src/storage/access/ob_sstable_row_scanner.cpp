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
#include "ob_sstable_index_filter.h"
#include "ob_block_row_store.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/column_store/ob_co_sstable_rows_filter.h"
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
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, micro_scanner_, ObIMicroBlockRowScanner);
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reset()
{
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, micro_scanner_, ObIMicroBlockRowScanner);
  is_opened_ = false;
  cur_range_idx_ = -1;
  sstable_ = nullptr;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  prefetcher_.reset();
  ObStoreRowIterator::reset();
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reuse()
{
  ObStoreRowIterator::reuse();
  is_opened_ = false;
  cur_range_idx_ = -1;
  sstable_ = nullptr;
  if (nullptr != micro_scanner_) {
    micro_scanner_->reuse();
  }
  if (nullptr != block_row_store_) {
    block_row_store_->reuse();
  }
  prefetcher_.reuse();
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reclaim()
{
  is_opened_ = false;
  cur_range_idx_ = -1;
  sstable_ = nullptr;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  prefetcher_.reclaim();
  if (nullptr != micro_scanner_) {
    micro_scanner_->reuse();
  }
  ObStoreRowIterator::reset();
  is_reclaimed_ = true;
}

template<typename PrefetchType>
bool ObSSTableRowScanner<PrefetchType>::can_blockscan() const
{
  return is_scan(type_) &&
         nullptr != block_row_store_ &&
         block_row_store_->can_blockscan();
}

template<typename PrefetchType>
bool ObSSTableRowScanner<PrefetchType>::can_batch_scan() const
{
    return can_blockscan() &&
          block_row_store_->filter_applied() &&
          // can batch scan when only enable_pd_aggregate, as it uses own datum buffer and only return aggregated result
          (iter_param_->vectorized_enabled_ || iter_param_->enable_pd_aggregate());
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
    ObSampleFilterExecutor *sample_executor = static_cast<ObSampleFilterExecutor *>(access_ctx.get_sample_executor());
    if (!prefetcher_.is_valid()) {
      if (OB_FAIL(prefetcher_.init(
                  type_, *sstable_, iter_param, access_ctx, query_range))) {
        LOG_WARN("fail to init prefetcher, ", K(ret));
      }
    } else if (OB_FAIL(prefetcher_.switch_context(type_, *sstable_, iter_param, access_ctx, query_range))) {
      LOG_WARN("fail to switch context for prefetcher, ", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (iter_param_->enable_pd_aggregate() &&
          !iter_param_->is_use_column_store() &&
          nullptr != block_row_store_ &&
          iter_param_->enable_skip_index() &&
          !sstable_->is_multi_version_table()) {
        prefetcher_.agg_row_store_ = reinterpret_cast<ObAggregatedStore *>(block_row_store_);
      }
      if (nullptr != sample_executor
          && sstable_->is_major_sstable()
          && OB_FAIL(sample_executor->build_row_id_handle(
                          prefetcher_.get_index_tree_height(),
                          prefetcher_.get_index_prefetch_depth(),
                          prefetcher_.get_micro_data_pefetch_depth()))) {
        LOG_WARN("Failed to build row id handle", K(ret), KPC(sample_executor));
      } else if (OB_FAIL(prefetcher_.prefetch())) {
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
                                               long_life_allocator_,
                                               *long_life_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block row scanner", K(ret));
      }
    } else {
      if (nullptr == (micro_scanner_ = OB_NEWx(ObMicroBlockRowScanner,
                                               long_life_allocator_,
                                               *long_life_allocator_))) {
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
      if (OB_FAIL(micro_handle.get_micro_block_data(&macro_block_reader_, block_data))) {
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
        sql::ObPushdownFilterExecutor *filter = block_row_store_->get_pd_filter();
        ObSampleFilterExecutor *sample_executor = static_cast<ObSampleFilterExecutor *>(access_ctx_->get_sample_executor());
        if (nullptr != sample_executor && sstable_->is_major_sstable()) {
          sample_executor->set_block_row_range(prefetcher_.cur_micro_data_fetch_idx_,
                                             micro_scanner_->get_current_pos(),
                                             micro_scanner_->get_last_pos());
        }
        if (nullptr != filter) {
          micro_info.pre_process_filter(*filter);
        }
        if (OB_FAIL(micro_scanner_->apply_blockscan(block_row_store_, access_ctx_->table_store_stat_))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to apply_block_scan", K(ret), KPC(block_row_store_));
          } else {
            ret = OB_SUCCESS;
          }
        }
        if (nullptr != filter) {
          micro_info.post_process_filter(*filter);
        }
        if (nullptr != sample_executor && can_batch_scan()) {
          if (sstable_->is_major_sstable()) {
          } else if (OB_FAIL(sample_executor->increase_row_num(access_ctx_->query_flag_.is_reverse_scan() ?
                                                               micro_scanner_->get_current_pos() - micro_scanner_->get_last_pos() + 1 :
                                                               micro_scanner_->get_last_pos() - micro_scanner_->get_current_pos() + 1))) {
            LOG_WARN("Failed to increase row num in sample filter", K(micro_scanner_->get_last_pos()), K(micro_scanner_->get_current_pos()), KPC(sample_executor));
          }
        }
        EVENT_INC(ObStatEventIds::BLOCKSCAN_BLOCK_CNT);
        LOG_TRACE("[PUSHDOWN] pushdown for block scan", K(prefetcher_.cur_micro_data_fetch_idx_), K(micro_info), KPC(block_row_store_));
      }
      if (OB_SUCC(ret)) {
        access_ctx_->inc_micro_access_cnt();
        LOG_DEBUG("Success to open micro block", K(ret), K(read_handle), K(prefetcher_.cur_micro_data_fetch_idx_),
                  K(micro_info), K(micro_handle), KPC(this), K(common::lbt()));
      }
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::inner_get_next_row_with_row_id(const ObDatumRow *&store_row, ObCSRowId &row_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row(store_row))) {
    if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret && OB_ITER_END != ret)) {
      LOG_WARN("Fail to get next row from row scanner", K(ret));
    }
  } else {
    const int64_t step = access_ctx_->query_flag_.is_reverse_scan() ? -1 : 1;
    row_id = prefetcher_.current_micro_info().cs_row_range_.start_row_id_ + micro_scanner_->get_current_pos() - step;
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
  } else if (can_batch_scan()) {
    ret = OB_PUSHDOWN_STATUS_CHANGED;
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
  }
  if (OB_SUCC(ret) && NULL != store_row) {
    ObDatumRow &datum_row = *const_cast<ObDatumRow *>(store_row);
    if (!store_row->row_flag_.is_not_exist() &&
      iter_param_->need_scn_ &&
      OB_FAIL(set_row_scn(*iter_param_, store_row))) {
      LOG_WARN("failed to set row scn", K(ret));
    }
    EVENT_INC(ObStatEventIds::SSSTORE_READ_ROW_COUNT);
    LOG_DEBUG("[INDEX BLOCK] inner get next row", KPC(store_row));
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
      } else if (can_batch_scan()) {
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
        } else if (FALSE_IT(prefetcher_.inc_cur_micro_data_fetch_idx())) {
        } else if (OB_FAIL(open_cur_data_block(read_handle))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to open cur data block", K(ret), KPC(this));
          }
        } else if (can_batch_scan()) {
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
      } else if (!can_batch_scan()) {
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
        } else if (FALSE_IT(prefetcher_.inc_cur_micro_data_fetch_idx())) {
        } else if (OB_FAIL(open_cur_data_block(read_handle))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to open cur data block", K(ret), KPC(this));
          }
        } else if (need_prefetch && OB_FAIL(prefetcher_.prefetch())) {
          LOG_WARN("Fail to do prefetch", K(ret), K_(prefetcher));
        } else if (!can_batch_scan()) {
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

/***************             For columnar store              ****************/

template<>
bool ObSSTableRowScanner<ObCOPrefetcher>::can_blockscan() const
{
  return is_scan(type_) &&
         nullptr != block_row_store_ &&
         prefetcher_.switch_to_columnar_scan() &&
         !sstable_->is_ddl_merge_sstable();
}

template<>
bool ObSSTableRowScanner<ObCOPrefetcher>::can_batch_scan() const
{
  return can_blockscan() &&
         !block_row_store_->is_disabled() &&
         iter_param_->vectorized_enabled_ && iter_param_->enable_pd_filter();
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::get_blockscan_start(
    ObCSRowId &start,
    int32_t &range_idx,
    BlockScanState &block_scan_state)
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  if (OB_UNLIKELY(BLOCKSCAN_RANGE != block_scan_state)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state of block_scan_state", K(ret), K(block_scan_state));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObSSTableRowScanner is not opened", K(ret), K_(is_opened));
  } else {
    start = OB_INVALID_CS_ROW_ID;
    MicroDataBlockScanState state = co_prefetcher->get_block_scan_state();
    switch (state) {
      case PENDING_SWITCH: {
        if (OB_FAIL(update_start_rowid_for_column_store())) {
          // OB_ITER_END: all range scan finish.
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to update start rowid", K(ret), K_(prefetcher));
          } else {
            block_scan_state = SCAN_FINISH;
          }
        } else if (co_prefetcher->has_valid_start_row_id()) {
          range_idx = co_prefetcher->cur_range_fetch_idx_;
          start = co_prefetcher->get_block_scan_start_row_id();
        } else {
          // Switch to row store scan.
          block_scan_state = BLOCKSCAN_FINISH;
        }
        break;
      }
      case ROW_STORE_SCAN: {
        // TODO(hanling): ROW_STORE_SCAN should not appear here.
        block_scan_state = BLOCKSCAN_FINISH;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected scan state", K(state), KP(co_prefetcher));
        break;
      }
    }
  }
  LOG_DEBUG("ObSSTableRowScanner::get_blockscan_start",
             K(ret), K(block_scan_state), K_(co_prefetcher->block_scan_state),
             K(start));
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::forward_blockscan(
    ObCSRowId &end,
    BlockScanState &block_scan_state,
    const ObCSRowId begin)
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  LOG_TRACE("[COLUMNSTORE] SSTableScanner forward_blockscan [start]", K(block_scan_state),
            K_(co_prefetcher->block_scan_state), K(end));
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObSSTableRowScanner is not opened", K(ret), K_(is_opened));
  } else {
    while (OB_SUCC(ret)) {
      MicroDataBlockScanState state = co_prefetcher->get_block_scan_state();
      switch (state) {
        case IN_END_OF_RANGE: {
          // We need to open next micro data block to determine border rowid.
          if (OB_FAIL(update_border_rowid_for_column_store())) {
            LOG_WARN("Failed to update border rowid", K(ret), K_(prefetcher));
          } else {
            // Scan state: ROW_STORE_SCAN or PENDING_SWITCH.
            if (co_prefetcher->is_in_row_store_scan_mode()) {
              block_scan_state = BLOCKSCAN_FINISH;
            } else {
              block_scan_state = SWITCH_RANGE;
            }
            end = co_prefetcher->get_block_scan_border_row_id();
          }
          break;
        }
        case PENDING_BLOCK_SCAN: {
          if (OB_FAIL(detect_border_rowid_for_column_store())) {
            LOG_WARN("Fail to detect border rowid", K(ret), K_(prefetcher));
          } else {
            end = co_prefetcher->get_block_scan_border_row_id();
            // After detect_border_rowid, state of co_prefetcher maybe
            // PENDING_BLOCK_SCAN, IN_END_OF_RANGE.
            // ObCOSSTableRowScanner will continue loop.
            block_scan_state = BLOCKSCAN_RANGE;
          }
          break;
        }
        case PENDING_SWITCH: {
          // Next range is scanned by columnar store.
          end = co_prefetcher->get_block_scan_border_row_id();
          block_scan_state = SWITCH_RANGE;
          break;
        }
        case ROW_STORE_SCAN: {
          // Next block is scanned scan by row store.
          co_prefetcher->set_range_scan_finish();
          end = co_prefetcher->get_block_scan_border_row_id();
          block_scan_state = BLOCKSCAN_FINISH;
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected scan state", K(state), KP(co_prefetcher));
        }
      }
      if (OB_SUCC(ret)) {
        // Continue to detect border row id :
        // 1. There are more undetected rows in current range for columnar scan.
        // 2. The number of detected rows is less than defualt batch size.
        if ((IN_END_OF_RANGE != state && PENDING_BLOCK_SCAN != state)
              || (end - begin + 1) >= iter_param_->get_storage_rowsets_size()) {
          break;
        }
      }
    }
  }
  LOG_TRACE("[COLUMNSTORE] SSTableScanner forward_blockscan [end]", K(ret), K(block_scan_state),
            K_(co_prefetcher->block_scan_state), K(end));
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::update_border_rowid_for_column_store()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("update_border_rowid_for_column_store start");
  // Fetch the last data mirco block of current range, get the border rowid
  // and check whether we can continue to block scan.
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  while(OB_SUCC(ret)) {
    if (OB_FAIL(co_prefetcher->prefetch())) {
      LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
    } else if (OB_UNLIKELY(prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_)) {
      // We will fetch the last data mirco block of current range, so never reach end.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected prefetch end", K(ret), K_(prefetcher));
    } else if (co_prefetcher->read_wait()) {
      continue;
    } else {
      ObSSTableReadHandle& read_handle = co_prefetcher->current_read_handle();
      blocksstable::ObMicroIndexInfo &micro_info = co_prefetcher->current_micro_info();
      ObMicroBlockDataHandle &micro_handle = co_prefetcher->current_micro_handle();
      // 1. Defensive check, must be same range idx.
      if (OB_UNLIKELY(cur_range_idx_ != read_handle.range_idx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected range idx", K(ret), K_(cur_range_idx), K(read_handle));
      } else {
        ObMicroBlockData block_data;
        const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
        // 2. Open the data micro block.
        if (OB_FAIL(micro_handle.get_micro_block_data(&macro_block_reader_, block_data))) {
          LOG_WARN("Fail to get block data", K(ret), K(micro_handle));
        } else if (OB_FAIL(micro_scanner_->open(
                             micro_handle.macro_block_id_,
                             block_data,
                             micro_info.is_left_border(),
                             micro_info.is_right_border()))) {
          LOG_WARN("Failed to open micro_scanner", K(ret), K(micro_info), K(micro_handle), KPC(this));
        } else {
          // 3. Check the rows that satisfy border rowkey.
          int64_t start_offset = 0;
          int64_t end_offset = 0;
          if (OB_FAIL(micro_scanner_->advance_to_border(co_prefetcher->get_border_rowkey(),
                                                        start_offset, end_offset))) {
            LOG_WARN("Failed advance to border", K(ret), K(co_prefetcher->get_border_rowkey()));
          } else {
            co_prefetcher->adjust_border_row_id(end_offset - start_offset, is_reverse_scan);
          }
          if (OB_SUCC(ret)) {
            if (OB_ITER_END == micro_scanner_->end_of_block() && micro_info.is_end_border(is_reverse_scan)) {
              // For non-reverse scan, if current micro block is end, current micro info is also in border.
              // For reverse scan, if current micro block is end, current micro info is not determined.
              co_prefetcher->set_block_scan_state(PENDING_SWITCH);
              co_prefetcher->go_to_next_range();
            } else {
              co_prefetcher->set_block_scan_state(ROW_STORE_SCAN);
            }
          }
        }
        break;
      }
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::update_start_rowid_for_column_store()
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  co_prefetcher->set_range_scan_finish();
  // Determine the start rowid of new range for scanning columnar store.
  while (OB_SUCC(ret)) {
    if (co_prefetcher->read_wait() && OB_FAIL(co_prefetcher->prefetch())) {
      LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
    } else if (co_prefetcher->cur_range_fetch_idx_ >= co_prefetcher->cur_range_prefetch_idx_) {
      co_prefetcher->is_prefetch_end_ = true;
      ret = OB_ITER_END;
    } else if (co_prefetcher->read_wait()) {
      continue;
    } else {
      const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
      ObSSTableReadHandle& read_handle = co_prefetcher->current_read_handle();
      if (-1 == read_handle.micro_begin_idx_) {
        // Empty range, go to next range.
        co_prefetcher->go_to_next_range();
        continue;
      }
      if (OB_FAIL(prepare_micro_scanner_for_column_store(read_handle))) {
        LOG_WARN("Fail to prepare micro scanner", K(ret), K(read_handle));
      } else {
        int64_t start_offset = 0;
        int64_t end_offset = 0;
        if (OB_FAIL(micro_scanner_->advance_to_border(co_prefetcher->get_border_rowkey(),
                                                              start_offset, end_offset))) {
          LOG_WARN("Failed to advance to border", K(ret), K(co_prefetcher->get_border_rowkey()));
        } else if (OB_ITER_END != micro_scanner_->end_of_block()) {
          // Update start_rowid and border_rowid.
          if (start_offset < end_offset && OB_FAIL(update_start_and_end_rowid_for_column_store(start_offset, end_offset))) {
            LOG_WARN("Failed to update start rowid and end rowid", K(ret), KP(co_prefetcher));
          }
        } else if (OB_FAIL(try_refreshing_blockscan_checker_for_column_store(start_offset, end_offset))) {
          LOG_WARN("Failed to try refreshing blockscan checker", K(ret), KP(co_prefetcher));
        } else if (co_prefetcher->is_in_switch_range_scan_mode() && (end_offset == start_offset)) {
          co_prefetcher->go_to_next_range();
          co_prefetcher->set_range_scan_finish();
          continue;
        }
      }
      break;
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::prepare_micro_scanner_for_column_store(ObSSTableReadHandle& read_handle)
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  if (nullptr == micro_scanner_) {
    if (OB_FAIL(init_micro_scanner())) {
      LOG_WARN("fail to init micro scanner", K(ret));
    }
  } else if (OB_UNLIKELY(!micro_scanner_->is_valid())) {
    if (OB_FAIL(micro_scanner_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Failed to switch new table context", K(ret), KPC(access_ctx_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (cur_range_idx_ != read_handle.range_idx_) {
    micro_scanner_->reuse();
    if (OB_FAIL(micro_scanner_->set_range(*(read_handle.range_)))) {
      LOG_WARN("Fail to init micro scanner", K(ret), K(read_handle));
    } else {
      prefetcher_.cur_micro_data_fetch_idx_ = read_handle.micro_begin_idx_;
      cur_range_idx_ = read_handle.range_idx_;
      blocksstable::ObMicroIndexInfo &micro_info = co_prefetcher->current_micro_info();
      ObMicroBlockDataHandle &micro_handle = co_prefetcher->current_micro_handle();
      ObMicroBlockData block_data;
      if (OB_FAIL(micro_handle.get_micro_block_data(&macro_block_reader_, block_data))) {
        LOG_WARN("Fail to get block data", K(ret), K(micro_handle));
      } else if (OB_FAIL(micro_scanner_->open(
                  micro_handle.macro_block_id_,
                  block_data,
                  micro_info.is_left_border(),
                  micro_info.is_right_border()))) {
        LOG_WARN("Failed to open micro_scanner", K(ret), K(micro_info), K(micro_handle), KPC(this));
      }
    }
  } else {
    // micro_scanner_ is already prepared.
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::detect_border_rowid_for_column_store()
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  if (OB_FAIL(prefetcher_.prefetch())) {
    LOG_WARN("Fail to prefetch", K(ret), K_(prefetcher));
  } else if (OB_UNLIKELY(prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_)) {
    // Prefetch end, never reach here in the current implementation.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), K_(prefetcher));
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::try_refreshing_blockscan_checker_for_column_store(
    const int64_t start_offset,
    const int64_t end_offset)
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  if (OB_FAIL(co_prefetcher->refresh_blockscan_checker_for_column_store(co_prefetcher->cur_micro_data_fetch_idx_ + 1,
                                                                         co_prefetcher->get_border_rowkey()))) {
    LOG_WARN("Failed to refresh block scan checker", K(ret), K(co_prefetcher->get_border_rowkey()),
              KP(co_prefetcher));
  } else if (co_prefetcher->is_in_switch_range_scan_mode()
              || co_prefetcher->is_in_row_store_scan_mode()) {
    if (start_offset < end_offset && OB_FAIL(update_start_and_end_rowid_for_column_store(start_offset, end_offset))) {
      LOG_WARN("Failed to update start rowid and end rowid", K(ret), KP(co_prefetcher));
    }
  } else {
    const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
    co_prefetcher->adjust_start_row_id(end_offset - start_offset, is_reverse_scan);
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::update_start_and_end_rowid_for_column_store(
    const int64_t start_offset,
    const int64_t end_offset)
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  ObCSRowId start_row_id = co_prefetcher->cur_start_row_id();
  const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
  if (is_reverse_scan) {
    co_prefetcher->set_start_row_id(start_row_id + end_offset - 1);
    co_prefetcher->set_border_row_id(start_row_id + start_offset);
  } else {
    co_prefetcher->set_start_row_id(start_row_id + start_offset);
    co_prefetcher->set_border_row_id(start_row_id + end_offset - 1);
  }
  LOG_DEBUG("update_start_and_end_rowid_for_column_store",
             K(start_row_id), K(start_offset), K(end_offset));
  return ret;
}

// Explicit instantiations.
template class ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<32, 3>>;
template class ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<2, 2>>;
template class ObSSTableRowScanner<ObCOPrefetcher>;

}
}

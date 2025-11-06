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
#include "ob_aggregated_store.h"
#include "ob_aggregated_store_vec.h"
#include "storage/blocksstable/ob_micro_block_row_lock_checker.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
template<typename PrefetchType>
ObSSTableRowScanner<PrefetchType>::~ObSSTableRowScanner()
{
  storage::ObIndexSkipScanFactory::destroy_index_skip_scanner(skip_scanner_);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, micro_data_scanner_, ObMicroBlockRowScanner);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, mv_micro_data_scanner_, ObMultiVersionMicroBlockRowScanner);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, mv_di_micro_data_scanner_, ObMultiVersionDIMicroBlockRowScanner);
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reset()
{
  storage::ObIndexSkipScanFactory::destroy_index_skip_scanner(skip_scanner_);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, micro_data_scanner_, ObMicroBlockRowScanner);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, mv_micro_data_scanner_, ObMultiVersionMicroBlockRowScanner);
  FREE_ITER_FROM_ALLOCATOR(long_life_allocator_, mv_di_micro_data_scanner_, ObMultiVersionDIMicroBlockRowScanner);
  is_opened_ = false;
  range_idx_ = 0;
  is_di_base_iter_ = false;
  cur_range_idx_ = -1;
  sstable_ = nullptr;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  micro_scanner_ = nullptr;
  prefetcher_.reset();
  ObStoreRowIterator::reset();
  skip_state_.reset();
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reuse()
{
  if (nullptr != skip_scanner_) {
    skip_scanner_->reuse();
  }
  ObStoreRowIterator::reuse();
  range_idx_ = 0;
  is_opened_ = false;
  is_di_base_iter_ = false;
  cur_range_idx_ = -1;
  if (nullptr != micro_data_scanner_) {
    micro_data_scanner_->reuse();
  }
  if (nullptr != mv_micro_data_scanner_) {
    mv_micro_data_scanner_->reuse();
  }
  if (nullptr != mv_di_micro_data_scanner_) {
    mv_di_micro_data_scanner_->reuse();
  }
  if (nullptr != block_row_store_) {
    block_row_store_->reuse();
  }
  micro_scanner_ = nullptr;
  sstable_ = nullptr;
  prefetcher_.reuse();
  skip_state_.reset();
}

template<typename PrefetchType>
void ObSSTableRowScanner<PrefetchType>::reclaim()
{
  storage::ObIndexSkipScanFactory::destroy_index_skip_scanner(skip_scanner_);
  is_opened_ = false;
  range_idx_ = 0;
  is_di_base_iter_ = false;
  cur_range_idx_ = -1;
  prefetcher_.reclaim();
  if (nullptr != micro_data_scanner_) {
    micro_data_scanner_->reuse();
  }
  if (nullptr != mv_micro_data_scanner_) {
    mv_micro_data_scanner_->reuse();
  }
  if (nullptr != mv_di_micro_data_scanner_) {
    mv_di_micro_data_scanner_->reuse();
  }
  micro_scanner_ = nullptr;
  sstable_ = nullptr;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  ObStoreRowIterator::reset();
  is_reclaimed_ = true;
  skip_state_.reset();
}

template<typename PrefetchType>
bool ObSSTableRowScanner<PrefetchType>::can_blockscan() const
{
  return is_scan(type_) && prefetcher_.can_blockscan() &&
         nullptr != micro_scanner_ && micro_scanner_->can_blockscan();
}

template<typename PrefetchType>
bool ObSSTableRowScanner<PrefetchType>::can_batch_scan() const
{
  return can_blockscan() &&
      !block_row_store_->is_disabled() &&
      micro_scanner_->is_filter_applied() &&
      !access_ctx_->is_mview_query() &&
      // can batch scan when only enable_pd_aggregate in vec 1.0, as it uses own datum buffer and only return aggregated result
      (iter_param_->vectorized_enabled_ || (!iter_param_->plan_use_new_format() && iter_param_->enable_pd_aggregate()));
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
    is_di_base_iter_ = iter_param.is_delete_insert_ && sstable_->is_major_type_sstable();
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
        if (iter_param_->plan_use_new_format()) {
          prefetcher_.agg_store_ = static_cast<ObAggStoreBase *>(static_cast<ObAggregatedStoreVec *>(block_row_store_));
        } else {
          prefetcher_.agg_store_ = static_cast<ObAggStoreBase *>(static_cast<ObAggregatedStore *>(block_row_store_));
        }
      }
      if (nullptr != sample_executor
          && sstable_->is_major_sstable()
          && OB_FAIL(sample_executor->build_row_id_handle(
                          prefetcher_.get_index_tree_height(),
                          prefetcher_.get_index_prefetch_depth(),
                          prefetcher_.get_micro_data_pefetch_depth()))) {
        LOG_WARN("Failed to build row id handle", K(ret), KPC(sample_executor));
      } else if (OB_UNLIKELY(iter_param.is_skip_scan() &&
                 OB_FAIL(ObIndexSkipScanFactory::build_index_skip_scanner(iter_param,
                                                                          access_ctx,
                                                                          static_cast<const ObDatumRange *>(query_range),
                                                                          skip_scanner_)))) {
        LOG_WARN("failed to build index skip scanner", K(ret));
      } else if (FALSE_IT(prefetcher_.skip_scanner_ = skip_scanner_)) {
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
#define INIT_MICRO_DATA_SCANNER(ptr, type)                                                \
  do {                                                                                    \
    if (ptr == nullptr) {                                                                 \
      if (OB_ISNULL(ptr = OB_NEWx(type, long_life_allocator_, *long_life_allocator_))) {  \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                                  \
        LOG_WARN("Failed to alloc memory for scanner", K(ret));                           \
      } else if (OB_FAIL(ptr->init(*iter_param_, *access_ctx_, sstable_))) {              \
        LOG_WARN("Fail to init micro scanner", K(ret));                                   \
      }                                                                                   \
    } else if (OB_LIKELY(!ptr->is_valid())) {                                             \
      if (OB_FAIL(ptr->switch_context(*iter_param_, *access_ctx_, sstable_))) {           \
        LOG_WARN("Failed to switch micro scanner", K(ret), KPC(ptr), KPC_(iter_param));   \
      }                                                                                   \
    }                                                                                     \
    if (OB_SUCC(ret)) {                                                                   \
      micro_scanner_ = ptr;                                                               \
    }                                                                                     \
  } while(0)

  if (sstable_->is_multi_version_minor_sstable()) {
    if (iter_param_->is_delete_insert_) {
      INIT_MICRO_DATA_SCANNER(mv_di_micro_data_scanner_, ObMultiVersionDIMicroBlockRowScanner);
    } else {
      INIT_MICRO_DATA_SCANNER(mv_micro_data_scanner_, ObMultiVersionMicroBlockRowScanner);
    }
  } else {
    INIT_MICRO_DATA_SCANNER(micro_data_scanner_, ObMicroBlockRowScanner);
  }
#undef INIT_MICRO_DATA_SCANNER

  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::open_cur_data_block(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  int cmp_ret = -1;
  bool is_keep_order = access_ctx_->query_flag_.is_keep_order();
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
      if (OB_UNLIKELY(has_skip_scanner() && nullptr != micro_scanner_->get_reader())) {
        if (access_ctx_->query_flag_.is_reverse_scan()) {
          const ObIndexSkipState *next_state = prefetcher_.get_next_skip_state();
          if (nullptr != next_state) {
            if (OB_FAIL(skip_scanner_->skip(micro_info, *next_state, micro_info.skip_state_))) {
              LOG_WARN("fail to skip endkey", K(ret));
            }
          }
        } else if (OB_FAIL(skip_scanner_->skip(micro_info, skip_state_, micro_info.skip_state_))) {
          LOG_WARN("fail to skip endkey", K(ret));
        }
      }
      if (OB_FAIL(ret) || micro_info.skip_state_.is_skipped()) {
      } else if (OB_FAIL(micro_handle.get_micro_block_data(&macro_block_reader_, block_data))) {
        LOG_WARN("Fail to get block data", K(ret), K(micro_handle));
      } else if (OB_FAIL(micro_scanner_->open(
                  micro_handle.macro_block_id_,
                  block_data,
                  micro_info.is_left_border(),
                  micro_info.is_right_border()))) {
        LOG_WARN("Fail to open micro_scanner", K(ret), K(micro_info), K(micro_handle), KPC(this));
      } else if (OB_FAIL(prefetcher_.check_blockscan(can_blockscan))) {
        LOG_WARN("Fail to check_blockscan", K(ret));
      // } else if (!can_blockscan || !is_keep_order) {
      // } else if (prefetcher_.get_border_rowkey().is_valid() &&
      //            OB_NOT_NULL(iter_param_->rowkey_read_info_) &&
      //            iter_param_->rowkey_read_info_->get_datum_utils().is_valid() &&
      //            OB_FAIL(micro_info.endkey_.compare(
      //                prefetcher_.get_border_rowkey(),
      //                iter_param_->rowkey_read_info_->get_datum_utils(),
      //                is_keep_order, cmp_ret))) {
      //   LOG_WARN("Fail to compare", K(ret));
      // } else if (cmp_ret >= 0) {
      //   ret = OB_ERR_UNEXPECTED;
      //   LOG_ERROR("wrong can_blockscan", K(can_blockscan), K(micro_info.endkey_), K(prefetcher_.get_border_rowkey()), K(prefetcher_));
      }
      if (OB_SUCC(ret) && (iter_param_->is_delete_insert_ || (can_blockscan && nullptr != block_row_store_ && !block_row_store_->is_disabled()))) {
        // Apply pushdown filter and block scan
        sql::ObPushdownFilterExecutor *filter = block_row_store_->get_pd_filter();
        ObSampleFilterExecutor *sample_executor = static_cast<ObSampleFilterExecutor *>(access_ctx_->get_sample_executor());
        ObAggregatedStoreVec *agg_store_vec = iter_param_->enable_pd_aggregate() && iter_param_->plan_use_new_format()
          ? static_cast<ObAggregatedStoreVec *>(block_row_store_) : nullptr;
        if (nullptr != sample_executor && sstable_->is_major_sstable()) {
          sample_executor->set_block_row_range(prefetcher_.cur_micro_data_fetch_idx_,
                                               micro_scanner_->get_current_pos(),
                                               micro_scanner_->get_last_pos(),
                                               micro_info.get_row_count());
        }
        if (nullptr != filter) {
          micro_info.pre_process_filter(*filter);
        }
        if (nullptr != agg_store_vec && OB_FAIL(agg_store_vec->reset_agg_row_id())) {
          LOG_WARN("Fail to reset agg row id", K(ret));
        } else if (!has_skip_scanner() && OB_FAIL(micro_scanner_->apply_filter(can_blockscan))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to apply filter", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        if (nullptr != filter) {
          micro_info.post_process_filter(*filter);
        }
        if (nullptr != sample_executor && can_batch_scan()) {
          if (sstable_->is_major_sstable()) {
          } else if (OB_FAIL(sample_executor->increase_row_num(micro_scanner_->get_access_cnt()))) {
            LOG_WARN("Failed to increase row num in sample filter", KPC_(micro_scanner), KPC(sample_executor));
          }
        }
        LOG_TRACE("[PUSHDOWN] pushdown for block scan", K(prefetcher_.cur_micro_data_fetch_idx_), K(micro_info), KPC(block_row_store_));
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(has_skip_scanner() && !micro_info.skip_state_.is_skipped() &&
                        OB_FAIL(skip_scanner_->skip(*micro_scanner_, micro_info, true/*first*/)))) {
          LOG_WARN("Fail to skip rows", K(ret));
        } else {
          access_ctx_->inc_micro_access_cnt();
          REALTIME_MONITOR_ADD_SSSTORE_READ_BYTES(access_ctx_, micro_scanner_->get_data_length());
        }
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
   LOG_WARN("ObSSTableRowScanner has not been opened", K(ret), KPC(this));
  } else if (can_batch_scan()) {
    ret = OB_PUSHDOWN_STATUS_CHANGED;
  } else {
    while(OB_SUCC(ret)) {
      if (is_multi_get() && prefetcher_.range_count_ > common::OB_MULTI_GET_OPEN_ROWKEY_NUM && prefetcher_.cur_range_prefetch_idx_ >= range_idx_ + common::OB_MULTI_GET_OPEN_ROWKEY_NUM) {
        LOG_DEBUG("prefetch too many ranges", K(ret), K(range_idx_), K(prefetcher_.current_read_handle()), K(prefetcher_));
        ret = OB_ITER_END;
      } else if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), KPC(this));
      } else if (prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_) {
        if (OB_LIKELY(prefetcher_.is_prefetch_end_)) {
          ret = OB_ITER_END;
          if (nullptr != micro_scanner_) {
            micro_scanner_->reset_blockscan();
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), KPC(this));
        }
      } else if (prefetcher_.read_wait()) {
        continue;
      } else if (prefetcher_.current_read_handle().is_skip_prefetch_) {
        LOG_DEBUG("skip prefetch", K(ret), K(range_idx_), K(prefetcher_.current_read_handle()), K(prefetcher_));
        ++prefetcher_.cur_range_fetch_idx_;
      } else if (OB_FAIL(fetch_row(prefetcher_.current_read_handle(), store_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          if (prefetcher_.is_current_range_prefetch_finished()) {
            ++prefetcher_.cur_range_fetch_idx_;
          }
          ret = OB_SUCCESS;
        } else if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret)) {
          LOG_WARN("Fail to fetch row", K(ret), KPC(this));
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
      OB_FAIL(set_row_scn(access_ctx_->use_fuse_row_cache_, *iter_param_, store_row))) {
      LOG_WARN("failed to set row scn", K(ret), KPC(this));
    }
    if (OB_NOT_NULL(sstable_)) {
      if (sstable_->is_minor_sstable()) {
        ++access_ctx_->table_store_stat_.minor_sstable_read_row_cnt_;
      } else if (sstable_->is_major_type_sstable()) {
        ++access_ctx_->table_store_stat_.major_sstable_read_row_cnt_;
      }
    }
    LOG_DEBUG("[INDEX BLOCK] inner get next row", KPC(store_row), KPC(this));
  }
  LOG_DEBUG("chaser debug", K(ret), KPC(store_row), KPC(this));
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
        } else if (FALSE_IT(preprocess_skip_scanner(prefetcher_.current_micro_info()))) {
        } else if (OB_UNLIKELY(has_skip_scanner_and_not_skipped(prefetcher_.current_micro_info()) &&
                   OB_FAIL(skip_scanner_->skip(*micro_scanner_, prefetcher_.current_micro_info())))) {
          LOG_WARN("Fail to switch prefix", K(ret));
        } else if (OB_UNLIKELY(has_skip_scanner_and_not_skipped(prefetcher_.current_micro_info(), true))) {
          LOG_DEBUG("[INDEX SKIP SCAN] skip to next prefix and it has data, continue get row");
        } else if (prefetcher_.cur_micro_data_fetch_idx_ >= read_handle.micro_end_idx_) {
          ret = OB_ITER_END;
          if (ObStoreRowIterator::IteratorRowLockAndDuplicationCheck == type_ ||
              ObStoreRowIterator::IteratorRowLockCheck == type_) {
            ObMicroBlockRowLockChecker *checker = static_cast<ObMicroBlockRowLockChecker *>(micro_scanner_);
            checker->inc_empty_read(read_handle);
          }
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
  if (nullptr != block_row_store_) {
    if (OB_FAIL(prefetcher_.refresh_blockscan_checker(prefetcher_.cur_micro_data_fetch_idx_ + 1, rowkey))) {
      LOG_WARN("Failed to prepare blockscan check info", K(ret), K(rowkey), KPC(this));
    } else {
      LOG_DEBUG("refresh blockscan", K(ret), K(rowkey));
    }
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::get_next_rows()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_ || nullptr == block_row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The ObSSTableRowScanner has not been opened or init", K(ret), K_(is_opened), KP_(block_row_store), KPC(this));
  } else {
    while (OB_SUCC(ret) && !block_row_store_->is_end()) {
      if (OB_FAIL(prefetcher_.prefetch())) {
        LOG_WARN("Fail to do prefetch", K(ret), KPC(this));
      } else if (prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_) {
        if (OB_LIKELY(prefetcher_.is_prefetch_end_)) {
          ret = OB_ITER_END;
          if (nullptr != micro_scanner_) {
            micro_scanner_->reset_blockscan();
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), KPC(this));
        }
      } else if (prefetcher_.read_wait()) {
        continue;
      } else if (OB_FAIL(fetch_rows(prefetcher_.current_read_handle()))) {
        if (OB_ITER_END == ret) {
          if (prefetcher_.is_current_range_prefetch_finished()) {
            ++prefetcher_.cur_range_fetch_idx_;
          }
          ret = OB_SUCCESS;
        } else if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret)) {
          LOG_WARN("Fail to fetch row", K(ret), KPC(this));
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
    blocksstable::ObMicroIndexInfo &micro_info = prefetcher_.current_micro_info();
    while (OB_SUCC(ret) && !block_row_store_->is_end()) {
      if (OB_SUCCESS == micro_scanner_->end_of_block() && !can_batch_scan()) {
        if (!is_di_base_iter_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected scan status", K(ret), KPC(this));
        } else {
          ret = OB_PUSHDOWN_STATUS_CHANGED;
        }
      } else if (OB_FAIL(micro_scanner_->get_next_rows())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next row", K(ret));
        } else if (FALSE_IT(preprocess_skip_scanner(prefetcher_.current_micro_info()))) {
        } else if (OB_UNLIKELY(has_skip_scanner_and_not_skipped(prefetcher_.current_micro_info()) &&
                   OB_FAIL(skip_scanner_->skip(*micro_scanner_, prefetcher_.current_micro_info())))) {
          LOG_WARN("Fail to switch prefix", K(ret));
        } else if (OB_UNLIKELY(has_skip_scanner_and_not_skipped(prefetcher_.current_micro_info(), true))) {
          LOG_DEBUG("[INDEX SKIP SCAN] skip to next prefix and it has data, continue get rows");
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

    if (OB_PUSHDOWN_STATUS_CHANGED == ret && is_di_base_iter_) {
      // TODO: @dengzhi.ldz handle scenario with adding column
      if (OB_UNLIKELY(can_batch_scan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected block scan status", K(ret), KPC(this));
      } else {
        do {
          if (OB_FAIL(micro_scanner_->get_next_border_rows(prefetcher_.get_border_rowkey()))) {
            if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
            } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("Fail to get next border rows", K(ret));
            } else if (prefetcher_.cur_micro_data_fetch_idx_ < read_handle.micro_end_idx_) {
              ret = OB_SUCCESS;
              break;
            }
          }
        } while (OB_SUCC(ret) && iter_param_->enable_pd_aggregate() && !block_row_store_->is_end());
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
         prefetcher_.can_blockscan() &&
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
          range_idx = co_prefetcher->cur_range_fetch_idx_;
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
            if (co_prefetcher->micro_data_prefetch_idx_ > co_prefetcher->cur_micro_data_fetch_idx_) {
              const ObSSTableReadHandle& read_handle = co_prefetcher->current_read_handle();
              const blocksstable::ObMicroIndexInfo &micro_info = co_prefetcher->current_micro_info();
              int cmp_ret = micro_info.cs_row_range_.compare(end);
              if (access_ctx_->query_flag_.is_reverse_scan()) {
                cmp_ret = -cmp_ret;
              }
              if (micro_info.range_idx() == read_handle.range_idx_ && cmp_ret > 0) {
                co_prefetcher->set_block_scan_state(IN_END_OF_RANGE);
              }
            }
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

template<typename PrefetcheType>
int ObSSTableRowScanner<PrefetcheType>::try_skip_deleted_row(ObCSRowId &co_current)
{
  int ret = OB_SUCCESS;
  ObCOPrefetcher* co_prefetcher = reinterpret_cast<ObCOPrefetcher*>(&prefetcher_);
  const ObDatumRow *deleted_row = nullptr;
  if (OB_FAIL(inner_get_next_row_with_row_id(deleted_row, co_current))) {
    if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED == ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected pushdown status changed while skipping deleted row", K(ret), K(co_current));
    } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get deleted row and get end row id", K(ret), K(co_current));
    }
  } else if (OB_FAIL(micro_scanner_->check_and_revert_non_border_rowkey(co_prefetcher->get_border_rowkey(),
                                                                        *deleted_row,
                                                                        co_current))) {
    LOG_WARN("fail to check and revert non border rowkey", K(ret), K(co_prefetcher->get_border_rowkey()), KPC(deleted_row), K(co_current));
  } else {
    FLOG_INFO("co sstable try skip deleted row", K(ret), K(co_prefetcher->get_border_rowkey()), KPC(deleted_row), K(co_current));
  }
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
  ObSSTableReadHandle& read_handle = co_prefetcher->current_read_handle();
  const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
  // 1. Defensive check, must be same range idx.
  if (OB_UNLIKELY(cur_range_idx_ != read_handle.range_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected range idx", K(ret), K_(cur_range_idx), K(read_handle));
  }

  while(OB_SUCC(ret) && IN_END_OF_RANGE == co_prefetcher->get_block_scan_state()) {
    if (OB_UNLIKELY(!co_prefetcher->can_blockscan())) {
      break;
    } else if (OB_FAIL(co_prefetcher->prefetch())) {
      LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
    } else if (OB_UNLIKELY(prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_)) {
      // We will fetch the last data mirco block of current range, so never reach end.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected prefetch end", K(ret), K_(prefetcher));
    } else if (co_prefetcher->read_wait()) {
      continue;
    } else if (OB_UNLIKELY(co_prefetcher->cur_micro_data_fetch_idx_ > read_handle.micro_end_idx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected micro index", K(ret), K_(prefetcher_.is_prefetch_end), K(is_reverse_scan), K_(prefetcher_.cur_range_fetch_idx),
               K_(prefetcher_.cur_micro_data_fetch_idx), K_(prefetcher_.micro_data_prefetch_idx), K(read_handle));
    } else {
      int64_t start_offset = 0;
      int64_t end_offset = 0;
      ObMicroBlockData block_data;
      ObDatumRowkey border_rowkey;
      blocksstable::ObMicroIndexInfo &micro_info = co_prefetcher->current_micro_info();
      ObMicroBlockDataHandle &micro_handle = co_prefetcher->current_micro_handle();
      // 2. Open the data micro block.
      if (OB_FAIL(micro_handle.get_micro_block_data(&macro_block_reader_, block_data))) {
        LOG_WARN("Fail to get block data", K(ret), K_(prefetcher_.is_prefetch_end), K(is_reverse_scan), K_(prefetcher_.cur_micro_data_fetch_idx),
                 K_(prefetcher_.micro_data_prefetch_idx), K(read_handle), K(micro_handle));
      } else if (FALSE_IT(micro_scanner_->set_range(*read_handle.range_))) {
      } else if (OB_FAIL(micro_scanner_->open(
                  micro_handle.macro_block_id_,
                  block_data,
                  micro_info.is_left_border(),
                  micro_info.is_right_border()))) {
        LOG_WARN("Failed to open micro_scanner", K(ret), K(micro_info), K(micro_handle), KPC(this));
      } else if (OB_FAIL(co_prefetcher->get_border_rowkey_keep_order(border_rowkey))) {
        LOG_WARN("Failed to get border rowkey");
      // 3. Check the rows that satisfy border rowkey.
      } else if (OB_FAIL(micro_scanner_->advance_to_border(border_rowkey, start_offset, end_offset))) {
        LOG_WARN("Failed advance to border", K(ret), K(border_rowkey));
      } else if (start_offset < end_offset &&
                 OB_FAIL(co_prefetcher->update_end_rowid_for_column_store(start_offset, end_offset))) {
        LOG_WARN("Fail to update end row id for column store", K(ret), K(start_offset), K(end_offset));
      } else if (OB_ITER_END == micro_scanner_->end_of_block() && micro_info.is_end_border(is_reverse_scan)) {
        // For non-reverse scan, if current micro block is end, current micro info is also in border.
        // For reverse scan, if current micro block is end, current micro info is not determined.
        co_prefetcher->set_block_scan_state(PENDING_SWITCH);
        co_prefetcher->go_to_next_range();
      } else if (OB_ITER_END != micro_scanner_->end_of_block() || start_offset >= end_offset) {
        co_prefetcher->set_block_scan_state(ROW_STORE_SCAN);
      } else {
        co_prefetcher->block_scan_border_row_id_ = is_reverse_scan ?
            micro_info.cs_row_range_.begin() : micro_info.cs_row_range_.end();
        co_prefetcher->cur_micro_data_fetch_idx_++;
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
    if (OB_UNLIKELY(!co_prefetcher->can_blockscan())) {
      break;
    } else if (co_prefetcher->read_wait() &&
               OB_FAIL(co_prefetcher->prefetch())) {
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
        ObDatumRowkey border_rowkey;
        if (OB_FAIL(co_prefetcher->get_border_rowkey_keep_order(border_rowkey))) {
          LOG_WARN("Failed to get border rowkey");
        } else if (OB_FAIL(micro_scanner_->advance_to_border(border_rowkey, start_offset, end_offset))) {
          LOG_WARN("Failed to advance to border", K(ret), K(border_rowkey));
        } else if (OB_ITER_END != micro_scanner_->end_of_block()) {
          // Update start_rowid and border_rowid.
          if (start_offset < end_offset &&
              OB_FAIL(co_prefetcher->update_start_and_end_rowid_for_column_store(start_offset, end_offset))) {
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
    if (start_offset < end_offset &&
        OB_FAIL(co_prefetcher->update_start_and_end_rowid_for_column_store(start_offset, end_offset))) {
      LOG_WARN("Failed to update start rowid and end rowid", K(ret), KP(co_prefetcher));
    }
  } else {
    const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
    co_prefetcher->adjust_start_row_id(end_offset - start_offset, is_reverse_scan);
  }
  return ret;
}

template<typename PrefetchType>
int ObSSTableRowScanner<PrefetchType>::get_next_rowkey(int64_t &curr_scan_index,
                                                       blocksstable::ObDatumRowkey& rowkey,
                                                       common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  ObDatumRowkey tmp_rowkey;
  rowkey.reset();
  block_row_store_->disable();
  bool is_delete_insert = iter_param_->is_delete_insert_; // save is_delete_insert_
  const_cast<ObTableIterParam*>(iter_param_)->is_delete_insert_ = false;  // to avoid using the blockscan path

  // get next row
  if (OB_FAIL(get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to get next row from di base iterator", K(ret), KPC(this));
    } else {
      // range_idx_ maybe -1 for empty range
      curr_scan_index = MAX(cur_range_idx_, 0);
      if (access_ctx_->query_flag_.is_reverse_scan()) {
        rowkey.set_min_rowkey();
      } else {
        rowkey.set_max_rowkey();
      }
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(tmp_rowkey.assign(row->storage_datums_, iter_param_->get_schema_rowkey_count()))) {
    LOG_WARN("assign rowkey failed", K(ret), K(row), K(iter_param_->get_schema_rowkey_count()));
  } else if (OB_UNLIKELY(!tmp_rowkey.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp_rowkey is not valid", K(ret), K(tmp_rowkey));
  } else if (OB_FAIL(tmp_rowkey.deep_copy(rowkey, allocator))) {
    LOG_WARN("fail to deep copy rowkey", K(ret), K(tmp_rowkey));
  } else {
    curr_scan_index = cur_range_idx_;
  }

  const_cast<ObTableIterParam*>(iter_param_)->is_delete_insert_ = is_delete_insert; // restore is_delete_insert_
  return ret;
}

// Explicit instantiations.
template class ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<32, 3>>;
template class ObSSTableRowScanner<ObIndexTreeMultiPassPrefetcher<2, 2>>;
template class ObSSTableRowScanner<ObCOPrefetcher>;

}
}

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
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_index_tree_prefetcher.h"
#include "ob_aggregated_store.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

void ObIndexTreePrefetcher::reset()
{
  is_inited_ = false;
  is_rescan_ = false;
  rescan_cnt_ = 0;
  data_version_ = 0;
  sstable_ = nullptr;
  sstable_meta_handle_.reset();
  data_block_cache_ = nullptr;
  index_block_cache_ = nullptr;
  iter_param_ = nullptr;
  access_ctx_ = nullptr;
  datum_utils_ = nullptr;
  index_scanner_.reset();
  for (int64_t i = 0; i < DEFAULT_GET_MICRO_DATA_HANDLE_CNT; ++i) {
    micro_handles_[i].reset();
  }
  micro_block_handle_mgr_.reset();
}

void ObIndexTreePrefetcher::reuse()
{
  index_scanner_.reset();
}

int ObIndexTreePrefetcher::init(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  UNUSED(query_range);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIndexTreePrefetcher has been inited", K(ret));
  } else if (OB_UNLIKELY(ObStoreRowIterator::IteratorSingleGet != iter_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObStoreRowIterator", K(ret), K(iter_type));
  } else if (OB_FAIL(micro_block_handle_mgr_.init(false, true, *access_ctx.stmt_allocator_))) {
    LOG_WARN("failed to init block handle mgr", K(ret));
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else {
    sstable_ = &sstable;
    access_ctx_ = &access_ctx;
    iter_param_ = &iter_param;
    datum_utils_ = &(iter_param.get_read_info()->get_datum_utils());
    data_version_ = sstable_->get_data_version();
    data_block_cache_ = &(OB_STORE_CACHE.get_block_cache());
    index_block_cache_ = &(OB_STORE_CACHE.get_index_block_cache());
    is_inited_ = true;
  }
  return ret;
}

int ObIndexTreePrefetcher::switch_context(
    const int iter_type,
    ObSSTable &sstable,
    const ObStorageDatumUtils &datum_utils,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  UNUSED(query_range);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(ObStoreRowIterator::IteratorSingleGet != iter_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObStoreRowIterator", K(ret), K(iter_type));
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else {
    sstable_ = &sstable;
    datum_utils_ = &datum_utils;
    access_ctx_ = &access_ctx;
    data_version_ = sstable_->get_data_version();
    if (!is_rescan_) {
      is_rescan_ = true;
      for (int64_t i = 0; i < DEFAULT_GET_MICRO_DATA_HANDLE_CNT; ++i) {
        micro_handles_[i].reset();
      }
    } else {
      rescan_cnt_++;
      if (rescan_cnt_ >= MAX_RESCAN_HOLD_LIMIT) {
        rescan_cnt_ = 0;
        if (OB_FAIL(micro_block_handle_mgr_.reset_handle_cache())) {
          STORAGE_LOG(WARN, "failed to reset handle cache", K(ret));
        }
      }
    }
  }
  return ret;
}

// prefetch row status by rowkey
// 1. not exist; 2. in row cache; 3. in block
int ObIndexTreePrefetcher::single_prefetch(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexTreePrefetcher not init", K(ret));
  } else if (OB_UNLIKELY(!read_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_handle));
  } else if (OB_FAIL(lookup_in_cache(read_handle))) {
    LOG_WARN("Failed to lookup_in_cache", K(ret));
  } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
    if (OB_FAIL(lookup_in_index_tree(read_handle))) {
      LOG_WARN("Failed to lookup_in_block", K(ret));
    }
  }
  return ret;
}

int ObIndexTreePrefetcher::lookup_in_cache(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!read_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_handle));
  } else if (sstable_->is_empty()) {
    //empty sstable
    found = true;
    read_handle.row_state_ = ObSSTableRowState::NOT_EXIST;
  }

  if (OB_SUCC(ret) && !found && access_ctx_->enable_get_row_cache()) {
    ObRowCacheKey key(MTL_ID(), iter_param_->tablet_id_, *read_handle.rowkey_,
                      *datum_utils_, data_version_, sstable_->get_key().table_type_);
    if (OB_FAIL(ObStorageCacheSuite::get_instance().get_row_cache().get_row(key, read_handle.row_handle_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("Fail to get row from row cache", K(ret), K(key));
      } else {
        ++access_ctx_->table_store_stat_.row_cache_miss_cnt_;
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(read_handle.row_handle_.row_value_->get_start_log_ts() != sstable_->get_key().get_start_scn().get_val_for_tx())) {
      ++access_ctx_->table_store_stat_.row_cache_miss_cnt_;
      ret = OB_SUCCESS;
    } else {
      found = true;
      read_handle.row_state_ = ObSSTableRowState::IN_ROW_CACHE;
      ++access_ctx_->table_store_stat_.row_cache_hit_cnt_;
    }
  }

  if (OB_SUCC(ret) && !found) {
    read_handle.row_state_ = ObSSTableRowState::IN_BLOCK;
  }
  LOG_DEBUG("[INDEX BLOCK] prefetch in cache info", K(ret), KPC(read_handle.rowkey_), K(read_handle), KPC(this));
  return ret;
}

int ObIndexTreePrefetcher::lookup_in_index_tree(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!read_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_handle));
  } else if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
    LOG_WARN("Fail to get index block root", K(ret));
  } else if (OB_FAIL(init_index_scanner(index_scanner_))) {
    LOG_WARN("Fail to init index scanner", K(ret));
  } else if (OB_FAIL(index_scanner_.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      index_block_,
      *read_handle.rowkey_,
      read_handle.range_idx_))) {
    LOG_WARN("Fail to open index block scanner", K(ret));
  } else {
    EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
  }

  // 2. Loop into index tree to get MicroBlockData
  bool found = false;
  int64_t level = 0;
  ObMicroIndexInfo index_block_info;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(index_scanner_.get_next(index_block_info))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to get index block row", K(ret), K_(index_scanner));
      }
    } else if (index_block_info.is_macro_node() && OB_FAIL(check_bloom_filter(index_block_info, read_handle))) {
      LOG_WARN("Fail to check bloom filter", K(ret), K(index_block_info), K(read_handle));
    } else if (ObSSTableRowState::NOT_EXIST == read_handle.row_state_) {
      found = true;
    } else {
      // hold block cache of the parent temporaliy to avoid freed
      ObMicroBlockDataHandle &curr_handle = get_read_handle(level);
      ++level;
      if (OB_FAIL(prefetch_block_data(index_block_info, curr_handle, false))) {
        LOG_WARN("Fail to prefetch block data", K(ret));
      } else if (OB_FAIL(curr_handle.get_index_block_data(index_block_))) {
        LOG_WARN("Fail to get index block data", K(ret), K(curr_handle));
      } else if (OB_FAIL(index_scanner_.open(
                  index_block_info.get_macro_id(),
                  index_block_,
                  *read_handle.rowkey_,
                  read_handle.range_idx_))) {
        LOG_WARN("Fail to open index block scanner", K(ret));
      } else if (index_block_info.is_leaf_block()) {
        ObMicroBlockDataHandle &leaf_handle = get_read_handle(level);
        if (OB_FAIL(index_scanner_.get_next(index_block_info))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to get next row", K(ret), K(index_scanner_));
          }
        } else if (OB_FAIL(prefetch_block_data(index_block_info, leaf_handle))) {
          LOG_WARN("fail to prefetch_block_data", K(ret), K(read_handle), K_(index_scanner));
        } else {
          found = true;
          read_handle.micro_handle_ = &leaf_handle;
        }
      }
    }
  }
  LOG_DEBUG("[INDEX BLOCK] prefetch in block info", K(ret), K(found), K(read_handle), KPC(this));
  if (OB_ITER_END == ret) {
    read_handle.row_state_ = ObSSTableRowState::NOT_EXIST;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIndexTreePrefetcher::init_index_scanner(ObIndexBlockRowScanner &index_scanner)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_scanner.init(
      agg_projector_,
      agg_column_schema_,
      *datum_utils_,
      *access_ctx_->stmt_allocator_,
      access_ctx_->query_flag_,
      sstable_->get_macro_offset()))) {
    LOG_WARN("init index scanner fail", K(ret), KPC(sstable_));
  }
  return ret;
}

int ObIndexTreePrefetcher::check_bloom_filter(const ObMicroIndexInfo &index_info, ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;

  if (!read_handle.is_get_ || !index_info.is_valid() || !index_info.is_macro_node()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(index_info), K(read_handle));
  } else if (!access_ctx_->query_flag_.is_index_back() && access_ctx_->enable_bf_cache()) {
    int temp_ret = OB_SUCCESS;
    bool is_contain = true;
    if (OB_UNLIKELY(OB_SUCCESS != (temp_ret = OB_STORE_CACHE.get_bf_cache().may_contain(
        MTL_ID(),
        index_info.get_macro_id(),
        *read_handle.rowkey_,
        *datum_utils_,
        is_contain)))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != temp_ret)) {
        LOG_WARN("Fail to check bloomfilter", K(temp_ret));
      }
    } else {
      if (is_contain) {
        read_handle.is_bf_contain_ = true;
      } else {
        read_handle.row_state_ = ObSSTableRowState::NOT_EXIST;
        ++access_ctx_->table_store_stat_.bf_filter_cnt_;
      }
    }
    LOG_DEBUG("check bloomfilter", K(ret), K(read_handle), K(is_contain));
    access_ctx_->table_store_stat_.rowkey_prefix_ = read_handle.rowkey_->get_datum_cnt();
    ++access_ctx_->table_store_stat_.bf_access_cnt_;
  }

  return ret;
}

int ObIndexTreePrefetcher::prefetch_block_data(
    blocksstable::ObMicroIndexInfo &index_block_info,
    ObMicroBlockDataHandle &micro_handle,
    const bool is_data)
{
  int ret = OB_SUCCESS;
  bool need_submit_io = false;
  uint64_t tenant_id = MTL_ID();
  const MacroBlockId &macro_id = index_block_info.get_macro_id();
  const int64_t offset = index_block_info.get_block_offset();

  if (is_rescan_ &&
      tenant_id == micro_handle.tenant_id_ &&
      macro_id == micro_handle.macro_block_id_ &&
      offset == micro_handle.micro_info_.offset_ &&
      index_block_info.row_header_->get_block_size() == micro_handle.micro_info_.size_) {
    LOG_DEBUG("Cur micro handle is still valid", K(index_block_info), K(micro_handle));
    if (is_data) {
      EVENT_INC(ObStatEventIds::DATA_BLOCK_CACHE_HIT);
    } else {
      EVENT_INC(ObStatEventIds::INDEX_BLOCK_CACHE_HIT);
    }
  } else if (OB_FAIL(micro_block_handle_mgr_.get_micro_block_handle(
              tenant_id,
              index_block_info,
              is_data,
              micro_handle))) {
    //cache miss
    ++access_ctx_->table_store_stat_.block_cache_miss_cnt_;
    if (!is_data) {
      ++access_ctx_->table_store_stat_.index_block_cache_miss_cnt_;
    }
    need_submit_io = true;
    ret = OB_SUCCESS;
  } else {
    //cache hit
    ++access_ctx_->table_store_stat_.block_cache_hit_cnt_;
    if (!is_data) {
      ++access_ctx_->table_store_stat_.index_block_cache_hit_cnt_;
      EVENT_INC(ObStatEventIds::INDEX_BLOCK_CACHE_HIT);
    } else {
      EVENT_INC(ObStatEventIds::DATA_BLOCK_CACHE_HIT);
    }
  }
  if (OB_SUCC(ret)) {
    micro_handle.micro_info_.offset_ = offset;
    micro_handle.micro_info_.size_ = index_block_info.get_block_size();
    if (need_submit_io) {
      ObMacroBlockHandle macro_handle;
      if (is_data) {
        if (OB_FAIL(data_block_cache_->prefetch(
                    tenant_id,
                    macro_id,
                    index_block_info,
                    access_ctx_->query_flag_,
                    macro_handle))) {
          LOG_WARN("Fail to prefetch micro block", K(ret), K(index_block_info), K(macro_handle), K(micro_handle));
        }
      } else if (OB_FAIL(index_block_cache_->prefetch(
                  tenant_id,
                  macro_id,
                  index_block_info,
                  access_ctx_->query_flag_,
                  macro_handle))) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K(index_block_info), K(micro_handle));
      }
      if (OB_SUCC(ret) && ObSSTableMicroBlockState::UNKNOWN_STATE == micro_handle.block_state_) {
        micro_handle.tenant_id_ = tenant_id;
        micro_handle.macro_block_id_ = macro_id;
        micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
        micro_handle.io_handle_ = macro_handle;

        if (is_data && OB_FAIL(micro_block_handle_mgr_.put_micro_block_handle(
                    tenant_id,
                    macro_id,
                    *index_block_info.row_header_,
                    micro_handle))) {
          STORAGE_LOG(WARN, "failed to put handle cache", K(ret), K(tenant_id), K(macro_id), K(index_block_info));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_data) {
      EVENT_INC(ObStatEventIds::DATA_BLOCK_READ_CNT);
    } else {
      EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
    }
  }
  return ret;
}

////////////////////////////////// ObIndexTreeMultiPrefetcher /////////////////////////////////////////////

void ObIndexTreeMultiPrefetcher::reset()
{
  fetch_rowkey_idx_ = 0;
  prefetch_rowkey_idx_ = 0;
  prefetched_rowkey_cnt_ = 0;
  rowkeys_ = nullptr;
  ext_read_handles_.reset();
  ObIndexTreePrefetcher::reset();
}

void ObIndexTreeMultiPrefetcher::reuse()
{
  fetch_rowkey_idx_ = 0;
  prefetch_rowkey_idx_ = 0;
  prefetched_rowkey_cnt_ = 0;
  rowkeys_ = nullptr;
  ObIndexTreePrefetcher::reuse();
}

int ObIndexTreeMultiPrefetcher::init(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIndexTreeMultiPrefetcher has been inited", K(ret));
  } else if (OB_UNLIKELY(ObStoreRowIterator::IteratorMultiGet != iter_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(iter_type));
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else {
    sstable_ = &sstable;
    access_ctx_ = &access_ctx;
    iter_param_ = &iter_param;
    data_version_ = sstable_->is_major_sstable() ? sstable_->get_snapshot_version() : sstable_->get_key().get_end_scn().get_val_for_tx();
    data_block_cache_ = &(ObStorageCacheSuite::get_instance().get_block_cache());
    index_block_cache_ = &(ObStorageCacheSuite::get_instance().get_index_block_cache());
    ext_read_handles_.set_allocator(access_ctx.stmt_allocator_);
    rowkeys_ = static_cast<const common::ObIArray<blocksstable::ObDatumRowkey> *> (query_range);
    index_tree_height_ = sstable_meta_handle_.get_sstable_meta().get_index_tree_height();
    datum_utils_ = &(iter_param.get_read_info()->get_datum_utils());
    int32_t range_count = rowkeys_->count();
    max_handle_prefetching_cnt_ = min(range_count, MAX_MULTIGET_MICRO_DATA_HANDLE_CNT);
    if (0 == range_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range count should be greater than 0", K(ret), K(range_count));
    } else if (OB_FAIL(ext_read_handles_.prepare_reallocate(max_handle_prefetching_cnt_))) {
      LOG_WARN("Fail to init read_handles", K(ret), K(max_handle_prefetching_cnt_));
    } else if (OB_FAIL(micro_block_handle_mgr_.init(range_count > 1, access_ctx_->query_flag_.is_ordered_scan(), *access_ctx.stmt_allocator_))) {
      LOG_WARN("failed to init block handle mgr", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObIndexTreeMultiPrefetcher::switch_context(
    const int iter_type,
    ObSSTable &sstable,
    const ObStorageDatumUtils &datum_utils,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  bool is_multi_range = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(ObStoreRowIterator::IteratorMultiGet != iter_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(iter_type));
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else {
    sstable_ = &sstable;
    access_ctx_ = &access_ctx;
    datum_utils_ = &datum_utils;
    data_version_ = sstable_->is_major_sstable() ? sstable_->get_snapshot_version() : sstable_->get_key().get_end_scn().get_val_for_tx();
    rowkeys_ = static_cast<const common::ObIArray<blocksstable::ObDatumRowkey> *> (query_range);
    index_tree_height_ = sstable_meta_handle_.get_sstable_meta().get_index_tree_height();
    max_handle_prefetching_cnt_ = min(rowkeys_->count(), MAX_MULTIGET_MICRO_DATA_HANDLE_CNT);
    if (OB_FAIL(ext_read_handles_.prepare_reallocate(max_handle_prefetching_cnt_))) {
      LOG_WARN("Fail to init read_handles", K(ret), K(max_handle_prefetching_cnt_));
    } else if (!is_rescan_) {
      is_rescan_ = true;
      for (int64_t i = 0; i < ext_read_handles_.count(); ++i) {
        ext_read_handles_.at(i).reset();
      }
    } else {
      rescan_cnt_++;
      if (rescan_cnt_ >= MAX_RESCAN_HOLD_LIMIT) {
        rescan_cnt_ = 0;
        if (OB_FAIL(micro_block_handle_mgr_.reset_handle_cache())) {
          STORAGE_LOG(WARN, "failed to reset handle cache", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (index_scanner_.is_valid()) {
        index_scanner_.switch_context(sstable, datum_utils);
      }
    }
  }
  return ret;
}

int ObIndexTreeMultiPrefetcher::multi_prefetch()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexTreeMultiPrefetcher not init", K(ret));
  } else {
    const int64_t rowkey_cnt = rowkeys_->count();
    for (int64_t i = fetch_rowkey_idx_;
         OB_SUCC(ret) && prefetched_rowkey_cnt_ < rowkey_cnt && i < fetch_rowkey_idx_ + max_handle_prefetching_cnt_;
         ++i) {
      const bool is_rowkey_to_fetched = i == fetch_rowkey_idx_;
      const bool is_empty_handle = i >= prefetch_rowkey_idx_;
      ObSSTableReadHandleExt &read_handle = ext_read_handles_[i % max_handle_prefetching_cnt_];
      if (is_empty_handle && prefetch_rowkey_idx_ < rowkey_cnt) {
        read_handle.reuse();
        read_handle.rowkey_ = &rowkeys_->at(prefetch_rowkey_idx_);
        read_handle.range_idx_ = prefetch_rowkey_idx_;
        read_handle.is_get_ = true;
        prefetch_rowkey_idx_++;

        if (OB_FAIL(lookup_in_cache(read_handle))) {
          LOG_WARN("Failed to lookup_in_cache", K(ret));
        } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
          if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
            LOG_WARN("Fail to get index block root", K(ret));
          } else if (!index_scanner_.is_valid() && OB_FAIL(init_index_scanner(index_scanner_))) {
            LOG_WARN("Fail to init index scanner", K(ret));
          } else if (OB_FAIL(drill_down(ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID, read_handle, false, is_rowkey_to_fetched))) {
            LOG_WARN("Fail to prefetch next level", K(ret), K(index_block_), K(read_handle), KPC(this));
          } else {
            EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
          }
        } else {
          mark_cur_rowkey_prefetched(read_handle);
        }
      } else if (read_handle.cur_prefetch_end_) {
        continue;
      } else if (read_handle.cur_level_ >= index_tree_height_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to prefetch, unexpected cur level", K(ret), K(read_handle.cur_level_), K(index_tree_height_), K(read_handle), KPC(this));
      } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
        bool stop_prefetch = false;
        int64_t tenant_id = MTL_ID();
        ObMicroIndexInfo &cur_index_info = read_handle.index_block_info_;
        ObMicroBlockDataHandle &next_handle = read_handle.get_read_handle();
        if (OB_UNLIKELY(!cur_index_info.is_valid() ||
            nullptr == read_handle.micro_handle_ ||
            &next_handle == read_handle.micro_handle_ ||
            ObSSTableMicroBlockState::IN_BLOCK_IO != read_handle.micro_handle_->block_state_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Fail to prefetch, unexpected read handle", K(ret), K(read_handle), KPC(this));
        } else if (OB_FAIL(micro_block_handle_mgr_.get_micro_block_handle(
                    tenant_id,
                    cur_index_info,
                    cur_index_info.is_data_block(),
                    next_handle))) {
          //not in cache yet, stop this rowkey prefetching if it's not the rowkey to be feteched
          ret = OB_SUCCESS;
          if (is_rowkey_to_fetched) {
            if (OB_FAIL(read_handle.micro_handle_->get_index_block_data(index_block_))) {
              LOG_WARN("Fail to get index block data", K(ret), KPC(read_handle.micro_handle_));
            }
          } else {
            stop_prefetch = true;
          }
        } else if (FALSE_IT(read_handle.set_cur_micro_handle(next_handle))) {
        } else if (OB_FAIL(read_handle.micro_handle_->get_cached_index_block_data(index_block_))) {
          LOG_WARN("Fail to get cached index block data", K(ret), KPC(read_handle.micro_handle_));
        }
        if (OB_SUCC(ret) && !stop_prefetch) {
          if (OB_FAIL(drill_down(cur_index_info.get_macro_id(), read_handle, cur_index_info.is_leaf_block(), is_rowkey_to_fetched))) {
            LOG_WARN("Fail to prefetch next level", K(ret), K(index_block_), K(read_handle), KPC(this));
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexTreeMultiPrefetcher::drill_down(
    const MacroBlockId &macro_id,
    ObSSTableReadHandleExt &read_handle,
    const bool cur_level_is_leaf,
    const bool force_prefetch)
{
  int ret = OB_SUCCESS;
  ObMicroIndexInfo index_block_info;
  EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
  read_handle.cur_level_++;
  if (OB_FAIL(index_scanner_.open(macro_id, index_block_, *read_handle.rowkey_, read_handle.range_idx_))) {
    LOG_WARN("Fail to open index block scanner", K(ret), K(index_block_), K(read_handle));
  } else if (cur_level_is_leaf && read_handle.cur_level_ != index_tree_height_ - 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to prefetch, unexpected level", K(ret), K(cur_level_is_leaf),
             K(read_handle.cur_level_), K(index_tree_height_));
  } else if (OB_FAIL(index_scanner_.get_next(index_block_info))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Fail to get index block row", K(ret), K_(index_scanner));
    } else {
      mark_cur_rowkey_prefetched(read_handle);
      read_handle.row_state_ = ObSSTableRowState::NOT_EXIST;
      ret = OB_SUCCESS;
    }
  } else if (cur_level_is_leaf != index_block_info.is_data_block()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to prefetch, unexpected level", K(ret), K(cur_level_is_leaf), K(index_block_info));
  } else if (index_block_info.is_macro_node() && OB_FAIL(check_bloom_filter(index_block_info, read_handle))) {
    LOG_WARN("Fail to check bloom filter", K(ret), K(index_block_info), K(read_handle));
  } else if (ObSSTableRowState::NOT_EXIST == read_handle.row_state_) {
    mark_cur_rowkey_prefetched(read_handle);
  } else {
    // hold block cache of the parent temporaliy to avoid freed
    ObMicroBlockDataHandle &next_handle = read_handle.get_read_handle();
    if (OB_FAIL(prefetch_block_data(index_block_info, next_handle, cur_level_is_leaf))) {
      LOG_WARN("fail to prefetch_block_data", K(ret), K(read_handle), K(index_block_info), K(cur_level_is_leaf));
    } else if (FALSE_IT(read_handle.set_cur_micro_handle(next_handle))) {
    } else if (cur_level_is_leaf) {
      mark_cur_rowkey_prefetched(read_handle);
      read_handle.index_block_info_ = index_block_info;
    } else if (force_prefetch || ObSSTableMicroBlockState::IN_BLOCK_CACHE == next_handle.block_state_) {
      if (ObSSTableMicroBlockState::IN_BLOCK_CACHE == next_handle.block_state_) {
        LOG_DEBUG("cur handle is in cache", K(read_handle), K(index_block_info), K(next_handle));
        if (OB_FAIL(next_handle.get_cached_index_block_data(index_block_))) {
          LOG_WARN("Fail to get index block data", K(ret), K(next_handle));
        }
      } else {
        LOG_DEBUG("cur handle is not in cache, force prefetch", K(read_handle), K(index_block_info), K(next_handle));
        if (OB_FAIL(next_handle.get_index_block_data(index_block_))) {
          LOG_WARN("Fail to get index block data", K(ret), K(next_handle));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(drill_down(
                    index_block_info.get_macro_id(),
                    read_handle,
                    index_block_info.is_leaf_block(),
                    force_prefetch))) {
          LOG_WARN("Faile to prefetch data block", K(ret), K(read_handle));
        }
      }
    } else {
      LOG_DEBUG("cur handle is not in cache, has submit io", K(read_handle), K(index_block_info), K(next_handle));
      read_handle.index_block_info_ = index_block_info;
    }
  }
  return ret;
}

////////////////////////////////// MultiPassPrefetcher /////////////////////////////////////////////

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::reset()
{
  for (int64_t i = 0; i < DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT; i++) {
    micro_data_handles_[i].reset();
  }
  for (int16_t level = 0; level < tree_handles_.count(); level++) {
    tree_handles_.at(level).reset();
  }
  ObIndexTreePrefetcher::reset();
  can_blockscan_ = false;
  is_prefetch_end_ = false;
  is_row_lock_checked_ = false;
  need_check_prefetch_depth_ = false;
  cur_range_fetch_idx_ = 0;
  cur_range_prefetch_idx_ = 0;
  max_range_prefetching_cnt_ = 0;
  cur_micro_data_fetch_idx_ = -1;
  micro_data_prefetch_idx_ = 0;
  row_lock_check_version_ = transaction::ObTransVersion::INVALID_TRANS_VERSION;
  agg_row_store_ = nullptr;
  max_micro_handle_cnt_ = 0;
  iter_type_ = 0;
  cur_level_ = 0;
  index_tree_height_ = 0;
  prefetch_depth_ = 1;
  total_micro_data_cnt_ = 0;
  query_range_ = nullptr;
  border_rowkey_.reset();
  read_handles_.reset();
  tree_handles_.reset();
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::reuse()
{
  ObIndexTreePrefetcher::reuse();
  clean_blockscan_check_info();
  is_prefetch_end_ = false;
  is_row_lock_checked_ = false;
  need_check_prefetch_depth_ = false;
  cur_range_fetch_idx_ = 0;
  cur_range_prefetch_idx_ = 0;
  cur_micro_data_fetch_idx_ = -1;
  micro_data_prefetch_idx_ = 0;
  row_lock_check_version_ = transaction::ObTransVersion::INVALID_TRANS_VERSION;
  agg_row_store_ = nullptr;
  prefetch_depth_ = 1;
  total_micro_data_cnt_ = 0;
  for (int64_t i = 0; i < tree_handles_.count(); i++) {
    tree_handles_.at(i).reuse();
  }
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::init(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  static const int16_t MAX_INDEX_TREE_HEIGHT = 16;
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIndexTreeMultiPassPrefetcher has been inited", K(ret));
  } else if (sstable.is_empty()) {
    is_prefetch_end_ = true;
    is_inited_ = true;
  } else {
    iter_param_ = &iter_param;
    data_block_cache_ = &(ObStorageCacheSuite::get_instance().get_block_cache());
    index_block_cache_ = &(ObStorageCacheSuite::get_instance().get_index_block_cache());
    tree_handles_.set_allocator(access_ctx.stmt_allocator_);
    read_handles_.set_allocator(access_ctx.stmt_allocator_);
    max_micro_handle_cnt_ = DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT;
    datum_utils_ = &(iter_param.get_read_info()->get_datum_utils());
    bool is_multi_range = false;
    if (OB_FAIL(init_basic_info(iter_type, sstable, access_ctx, query_range, is_multi_range))) {
      LOG_WARN("Fail to init basic info", K(ret), K(access_ctx));
    } else if (OB_FAIL(micro_block_handle_mgr_.init(is_multi_range, access_ctx_->query_flag_.is_ordered_scan(), *access_ctx.stmt_allocator_))) {
      LOG_WARN("failed to init block handle mgr", K(ret));
    } else {
      for (int64_t level = 0; OB_SUCC(ret) && level < index_tree_height_; level++) {
        if (OB_FAIL(init_index_scanner(tree_handles_[level].index_scanner_))) {
          LOG_WARN("Fail to init index_scanner", K(ret), K(level));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::switch_context(
    const int iter_type,
    ObSSTable &sstable,
    const ObStorageDatumUtils &datum_utils,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  bool is_multi_range = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (sstable.is_empty()) {
    is_prefetch_end_ = true;
  } else if (OB_FAIL(init_basic_info(iter_type, sstable, access_ctx, query_range, is_multi_range))) {
    LOG_WARN("Fail to init basic info", K(ret), K(access_ctx));
  } else {
    if (!is_rescan_) {
      is_rescan_ = true;
      for (int64_t i = 0; i < DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT; i++) {
        micro_data_handles_[i].reset();
      }
      for (int16_t level = 0; level < tree_handles_.count(); level++) {
        tree_handles_.at(level).reset();
      }
    }
  }
  if (OB_SUCC(ret)) {
    datum_utils_ = &datum_utils;
    for (int64_t level = 0; OB_SUCC(ret) && level < index_tree_height_; level++) {
      if (tree_handles_[level].index_scanner_.is_valid()) {
        tree_handles_[level].index_scanner_.switch_context(sstable, datum_utils);
      } else if (OB_FAIL(init_index_scanner(tree_handles_[level].index_scanner_))) {
        LOG_WARN("Fail to init index_scanner", K(ret), K(level));
      }
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::init_basic_info(
    const int iter_type,
    ObSSTable &sstable,
    ObTableAccessContext &access_ctx,
    const void *query_range,
    bool &is_multi_range)
{
  static const int16_t MAX_INDEX_TREE_HEIGHT = 16;
  int ret = OB_SUCCESS;
  is_multi_range = false;
  int32_t range_count = 0;
  sstable_ = &sstable;
  access_ctx_ = &access_ctx;
  data_version_ = sstable_->get_data_version();
  cur_level_ = 0;
  iter_type_ = iter_type;
  need_check_prefetch_depth_ =
      (ObStoreRowIterator::IteratorScan == iter_type || ObStoreRowIterator::IteratorMultiScan == iter_type) &&
      iter_param_->limit_prefetch_ &&
      nullptr != access_ctx_->limit_param_ &&
      access_ctx_->limit_param_->limit_ >= 0 &&
      access_ctx_->limit_param_->limit_ < 4096;
  switch (iter_type) {
    case ObStoreRowIterator::IteratorMultiGet: {
      rowkeys_ = static_cast<const common::ObIArray<blocksstable::ObDatumRowkey> *> (query_range);
      range_count = rowkeys_->count();
      max_range_prefetching_cnt_ = min(range_count, DEFAULT_SCAN_RANGE_PREFETCH_CNT);
      if (0 == range_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range count should be greater than 0", K(ret), K(range_count));
      } else if (1 < range_count) {
        is_multi_range = true;
      }
      break;
    }
    case ObStoreRowIterator::IteratorScan:
    case ObStoreRowIterator::IteratorRowLockCheck: {
      range_ = static_cast<const blocksstable::ObDatumRange *>(query_range);
      max_range_prefetching_cnt_ = DEFAULT_SCAN_RANGE_PREFETCH_CNT;
      break;
    }
    case ObStoreRowIterator::IteratorMultiScan: {
      ranges_ = static_cast<const common::ObIArray<blocksstable::ObDatumRange> *>(query_range);
      range_count = ranges_->count();
      max_range_prefetching_cnt_ = DEFAULT_SCAN_RANGE_PREFETCH_CNT;
      if (0 == range_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range count should be greater than 0", K(ret), K(range_count));
      } else if (1 < range_count) {
        is_multi_range = true;
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid store iterator type", K(ret), K(iter_type));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else if (FALSE_IT(index_tree_height_ = sstable_meta_handle_.get_sstable_meta().get_index_tree_height())) {
  } else if (1 >= index_tree_height_ || MAX_INDEX_TREE_HEIGHT < index_tree_height_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index tree height", K(ret), K(index_tree_height_), K(MAX_INDEX_TREE_HEIGHT));
  } else if (OB_FAIL(tree_handles_.prepare_reallocate(index_tree_height_))) {
    LOG_WARN("Fail to init tree_handles", K(ret), K(index_tree_height_));
  } else if (OB_FAIL(read_handles_.prepare_reallocate(max_range_prefetching_cnt_))) {
    LOG_WARN("Fail to init read_handles", K(ret), K(max_range_prefetching_cnt_));
  }
  return ret;
}

/*
 * Prefetch() prefetch micro index block and micro data block
 * Prefetch index tree at each level before prefetch micro data block,
 * so that index block could be load when read the rows of data_block.
 * If the last row read is non-data-block, stop the prefetching. Others, micro data blocks prefetched.
 */
template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::prefetch()
{
  int ret = OB_SUCCESS;
  const int32_t prefetch_limit = MAX(2, max_micro_handle_cnt_ / 2);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexTreeMultiPassPrefetcher not init", K(ret));
  } else if (is_prefetch_end_) {
  } else if (micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_ >= prefetch_limit) {
    // continue current prefetch
  } else if (OB_FAIL(prefetch_index_tree())) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      is_prefetch_end_ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Fail to prefetch index tree", K(ret));
    }
  } else if (OB_FAIL(prefetch_micro_data())) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      is_prefetch_end_ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Fail to prefetch", K(ret));
    }
  }
  return ret;
}

// prefetch index block tree from up to down, cur_level_ is the level of index tree last read
template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::prefetch_index_tree()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_tree_height_ <= cur_level_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected tree level", K(ret), K(cur_level_), K(index_tree_height_));
  } else if (OB_FAIL(try_add_query_range(tree_handles_[0]))) {
    LOG_WARN("Fail to add query range", K(ret));
  } else {
    int16_t border_level = MIN(cur_level_ + 2, index_tree_height_);
    for (int16_t level = 1; OB_SUCC(ret) && level < border_level; level++) {
      if (tree_handles_[level].is_prefetch_end()) {
      } else if (OB_FAIL(tree_handles_[level].prefetch(
                  border_rowkey_,
                  level,
                  *this))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to prefetch index tree", K(ret), K(level),
                   K(tree_handles_[level - 1]), K(tree_handles_[level]));
        }
      }
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::try_add_query_range(
    ObIndexTreeLevelHandle &tree_handle)
{
  int ret = OB_SUCCESS;
  if (cur_range_prefetch_idx_ - cur_range_fetch_idx_ == max_range_prefetching_cnt_ ||
      tree_handle.is_prefetch_end()) {
    // read handles prefetched full
  } else if (0 == cur_range_prefetch_idx_ || tree_handle.reach_scanner_end()) {
    tree_handle.fetch_idx_ = tree_handle.prefetch_idx_ = 0;
    ObSSTableReadHandle &read_handle = read_handles_[cur_range_prefetch_idx_ % max_range_prefetching_cnt_];
    if (OB_FAIL(prepare_read_handle(tree_handle, read_handle))) {
      LOG_WARN("Fail to prepare read handle", K(ret));
    } else if (read_handle.is_get_) {
      // get
      if (OB_FAIL(lookup_in_cache(read_handle))) {
        LOG_WARN("Failed to lookup_in_cache", K(ret));
      } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
        if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
          LOG_WARN("Fail to get index block root", K(ret));
        } else if (OB_FAIL(tree_handle.index_scanner_.open(
            ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
            index_block_,
            *read_handle.rowkey_,
            read_handle.range_idx_))) {
          LOG_WARN("Fail to open index block scanner", K(ret), K(read_handle));
        } else {
          EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
        }
      }
    } else {
      // scan
      read_handle.row_state_ = ObSSTableRowState::IN_BLOCK;
      if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
        LOG_WARN("Fail to get index tree root", K(ret));
      } else if (OB_FAIL(tree_handle.index_scanner_.open(
          ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
          index_block_,
          *read_handle.range_,
          read_handle.range_idx_,
          true, /* is_left_border */
          true /* is_right_border */))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          LOG_WARN("Fail to open index scanner", K(ret), K(read_handle));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        EVENT_INC(ObStatEventIds::INDEX_BLOCK_READ_CNT);
      }
    }

    // TODO: @dengzhi.ldz, border of the first level is false in most cases, opt later
    if (OB_SUCC(ret) && OB_FAIL(tree_handle.check_blockscan(border_rowkey_))) {
      LOG_WARN("Fail to check_blockscan", K(ret), K(border_rowkey_), K(tree_handle));
    }
  }
  return ret;
}

static int32_t OB_SSTABLE_MICRO_AVG_COUNT = 100;
template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::prefetch_micro_data()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_tree_height_ <= cur_level_ ||
                  micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_ > max_micro_handle_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected prefetch status", K(ret), K_(cur_level), K_(index_tree_height),
             K_(micro_data_prefetch_idx), K_(cur_micro_data_fetch_idx), K_(max_micro_handle_cnt));
  } else if (micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_ == max_micro_handle_cnt_) {
    // DataBlock ring buf full
  } else {
    int64_t prefetched_cnt = 0;
    int64_t prefetch_micro_idx = 0;
    prefetch_depth_ = MIN(max_micro_handle_cnt_, 2 * prefetch_depth_);
    if (need_check_prefetch_depth_) {
      int64_t prefetch_micro_cnt = MAX(1,
          (access_ctx_->limit_param_->offset_ + access_ctx_->limit_param_->limit_ - access_ctx_->out_cnt_ + \
          OB_SSTABLE_MICRO_AVG_COUNT - 1) / OB_SSTABLE_MICRO_AVG_COUNT);
      prefetch_depth_ = MIN(prefetch_depth_, prefetch_micro_cnt);
    }
    int64_t prefetch_depth = min(static_cast<int64_t>(prefetch_depth_),
                                 max_micro_handle_cnt_ - (micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_));
    while (OB_SUCC(ret) && prefetched_cnt < prefetch_depth) {
      if (OB_FAIL(drill_down())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get index leaf", K(ret), K(cur_level_));
        } // else prefetch_end
      } else if (index_tree_height_ - 1 != cur_level_) {
        // not leaf level, prefetch index tree
        break;
      } else {
        // read index leaf and prefetch micro data
        while (OB_SUCC(ret) && prefetched_cnt < prefetch_depth) {
          prefetch_micro_idx = micro_data_prefetch_idx_ % max_micro_handle_cnt_;
          ObMicroIndexInfo &block_info = micro_data_infos_[prefetch_micro_idx];
          if (OB_FAIL(tree_handles_[cur_level_].get_next_data_row(block_info))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get next", K(ret), K(cur_level_), K(tree_handles_[cur_level_]));
            } else {
              // open next leaf by get_next_index_leaf in the next loop
              ret = OB_SUCCESS;
              break;
            }
          } else if (nullptr != agg_row_store_ && agg_row_store_->can_agg_index_info(block_info)) {
            if (OB_FAIL(agg_row_store_->fill_index_info(block_info))) {
              LOG_WARN("Fail to agg index info", K(ret), K(block_info), KPC(this));
            } else {
              LOG_DEBUG("Success to agg index info", K(ret), KPC(agg_row_store_));
              continue;
            }
          } else if (OB_FAIL(check_row_lock(block_info, is_row_lock_checked_))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("Fail to check row lock", K(ret), K(block_info), KPC(this));
            }
          } else if (OB_FAIL(prefetch_block_data(block_info, micro_data_handles_[prefetch_micro_idx]))) {
            LOG_WARN("fail to prefetch_block_data", K(ret), K(block_info));
          }

          if (OB_SUCC(ret)) {
            prefetched_cnt++;
            micro_data_prefetch_idx_++;
            tree_handles_[cur_level_].current_block_read_handle().end_prefetched_row_idx_++;
          }
        }
      }
      if (OB_SUCC(ret) && 0 < prefetched_cnt) {
        ObSSTableReadHandle &read_handle = read_handles_[prefetching_range_idx() % max_range_prefetching_cnt_];
        if (-1 == read_handle.micro_begin_idx_) {
          read_handle.micro_begin_idx_ = total_micro_data_cnt_;
          read_handle.micro_end_idx_ = total_micro_data_cnt_ + prefetched_cnt - 1;
        } else {
          read_handle.micro_end_idx_ = total_micro_data_cnt_ + prefetched_cnt - 1;
        }
        LOG_DEBUG("[INDEX BLOCK] read_handle prefetched info", K(ret), K(prefetching_range_idx()), K(read_handle),
                  K(total_micro_data_cnt_), K(prefetched_cnt), K(prefetch_depth));
        total_micro_data_cnt_ += prefetched_cnt;
        prefetch_depth -= prefetched_cnt;
        prefetched_cnt = 0;
      }
    }
  }
  LOG_DEBUG("[INDEX BLOCK] prefetched info", K(ret),  KPC(this));
  return ret;
}

// drill down to get next valid index micro block
template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::drill_down()
{
  int ret = OB_SUCCESS;
  if (index_tree_height_ <= cur_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected level of index tree leaf", K(ret), K(cur_level_), K(index_tree_height_));
  } else {
    while (OB_SUCC(ret)) {
      if (index_tree_height_ - 1 == cur_level_) {
      } else if (OB_FAIL(tree_handles_[cur_level_ + 1].forward(border_rowkey_,
                                                               iter_param_->has_lob_column_out()))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to consume tree handle", K(ret), K(cur_level_), K(tree_handles_[cur_level_ + 1]));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        cur_level_++;
      }

      if (OB_SUCC(ret)) {
        if (tree_handles_[cur_level_].reach_scanner_end()) {
          if (0 == cur_level_) {
            if (tree_handles_[cur_level_].is_prefetch_end()) {
              ret = OB_ITER_END;
            } else {
              // wait until prefetch range
              break;
            }
          } else {
            // not valid, try again
            cur_level_--;
          }
        } else {
          // found
          break;
        }
      }
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::prepare_read_handle(
    ObIndexTreeMultiPassPrefetcher::ObIndexTreeLevelHandle &tree_handle,
    ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null query_range", K(ret));
  } else {
    switch (iter_type_) {
      case ObStoreRowIterator::IteratorMultiGet: {
        read_handle.reuse();
        read_handle.rowkey_ = &rowkeys_->at(cur_range_prefetch_idx_);
        read_handle.range_idx_ = cur_range_prefetch_idx_;
        read_handle.is_get_ = true;
        if (++cur_range_prefetch_idx_ >= rowkeys_->count()) {
          tree_handle.set_prefetch_end();
        }
        break;
      }
      case ObStoreRowIterator::IteratorScan:
      case ObStoreRowIterator::IteratorRowLockCheck: {
        read_handle.reuse();
        read_handle.range_ = range_;
        read_handle.range_idx_ = cur_range_prefetch_idx_;
        read_handle.is_get_ = false;
        cur_range_prefetch_idx_++;
        tree_handle.set_prefetch_end();
        break;
      }
      case ObStoreRowIterator::IteratorMultiScan: {
        read_handle.reuse();
        const ObDatumRange *range = &ranges_->at(cur_range_prefetch_idx_);
        read_handle.range_ = range;
        read_handle.range_idx_ = cur_range_prefetch_idx_;
        read_handle.is_get_ = false;
        if (++cur_range_prefetch_idx_ >= ranges_->count()) {
          tree_handle.set_prefetch_end();
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected store iterator type", K(ret), K(iter_type_));
    }
  }
  return ret;
}

class ObMicroInfoComparator
{
public:
  ObMicroInfoComparator(const ObStorageDatumUtils &datum_utils, int &ret, bool is_reverse)
    : datum_utils_(datum_utils), ret_(ret), is_reverse_(is_reverse)
  {}
  ~ObMicroInfoComparator() {}
  inline bool operator() (const ObMicroIndexInfo &index_info, const ObDatumRowkey &rowkey)
  {
    int ret = compare(index_info, rowkey);
    return is_reverse_ ? (ret > 0) : (ret < 0);
  }
  inline int compare(const ObMicroIndexInfo &index_info, const ObDatumRowkey &rowkey)
  {
    int cmp_ret = 0;
    int &ret = ret_;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_UNLIKELY(!index_info.is_valid() || !rowkey.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid MicroIndexInfo", K(ret), K(index_info), K(rowkey));
    } else if (OB_FAIL(index_info.endkey_->compare(rowkey, datum_utils_, cmp_ret, false))) {
      LOG_WARN("fail to compare rowkey", K(ret), KPC(index_info.endkey_), K(rowkey));
    }
    return cmp_ret;
  }

private:
  const ObStorageDatumUtils &datum_utils_;
  int &ret_;
  bool is_reverse_;
};

// binary check border_rowkey in range array [start, end]
OB_INLINE static int binary_check_micro_infos(
    ObMicroIndexInfo *micro_infos,
    const ObStorageDatumUtils &datum_utils,
    const int64_t start,
    const int64_t end,
    const ObDatumRowkey &rowkey,
    bool is_reverse)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(end < start || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(start), K(end), K(rowkey));
  } else {
    ObMicroInfoComparator cmp(datum_utils, ret, is_reverse);
    const ObMicroIndexInfo *first = micro_infos + start;
    const ObMicroIndexInfo *last = micro_infos + end + 1;
    const ObMicroIndexInfo *found = std::lower_bound(first, last, rowkey, cmp);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get rowkey lower_bound", K(ret), K(rowkey));
    } else {
      int64_t check_border_idx = start + (found - first);
      if (is_reverse) {
        // [check_border_idx - 1] is the last micro whose endkey greater than border_rowkey in reverse scan, need move forward
        check_border_idx--;
        if (OB_UNLIKELY(check_border_idx == end && check_border_idx > 0)) {
          // move to left if the two rightmost endkeys is same(possible in multiple ranges)
          int cmp_ret = 0;
          for (; OB_SUCC(ret) && 0 == cmp_ret && check_border_idx > start; check_border_idx--) {
            const ObDatumRowkey *cur_endkey = (micro_infos + check_border_idx)->endkey_;
            const ObDatumRowkey *prev_endkey = (micro_infos + check_border_idx - 1)->endkey_;
            if (OB_FAIL(cur_endkey->compare(*prev_endkey, datum_utils, cmp_ret, false))) {
              LOG_WARN("fail to compare rowkey", K(ret), KPC(cur_endkey), KPC(prev_endkey));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t idx = start; idx < check_border_idx; idx++) {
          micro_infos[idx].set_blockscan();
        }
      }
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::check_data_infos_border(
    const int64_t start_pos,
    const int64_t end_pos,
    const ObDatumRowkey &border_rowkey,
    bool is_reverse)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(end_pos < start_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(start_pos), K(end_pos));
  } else if (!can_blockscan_) {
  } else {
    const ObStorageDatumUtils &datum_utils = *datum_utils_;
    int64_t start_idx = start_pos % max_micro_handle_cnt_;
    int64_t end_idx = end_pos % max_micro_handle_cnt_;
    if (!is_reverse) {
      if (OB_FAIL(micro_data_infos_[end_idx].endkey_->compare(border_rowkey, datum_utils, cmp_ret, false))) {
        LOG_WARN("Fail to compare endkey", K(ret), K(border_rowkey), K(micro_data_infos_[start_idx].endkey_));
      } else if (cmp_ret < 0) {
        for (int64_t pos = start_pos; pos < end_pos; pos++) {
          micro_data_infos_[pos % max_micro_handle_cnt_].set_blockscan();
        }
      } else {
        can_blockscan_ = false;
        if (start_pos < end_pos) {
          // check border in range [start_pos, end_pos - 1]
          if (end_idx  > start_idx) {
            // binary check in rang [start_idx, end_idx - 1]
            if (OB_FAIL(binary_check_micro_infos(micro_data_infos_, datum_utils, start_idx, end_idx - 1, border_rowkey, is_reverse))) {
              LOG_WARN("Fail to check_micro_infos", K(ret), K(start_idx), K(end_idx));
            }
          } else {
            cmp_ret = 0;
            // split to [start_idx, max_micro_handle_cnt_ - 1], [0, end_idx - 1]
            if (OB_FAIL(micro_data_infos_[max_micro_handle_cnt_ - 1].endkey_->compare(border_rowkey, datum_utils, cmp_ret, false))) {
              LOG_WARN("Fail to compare endkey", K(ret), K(border_rowkey), K(micro_data_infos_[max_micro_handle_cnt_ - 1]));
            } else if (cmp_ret < 0) {
              for (int64_t idx = start_idx; idx < max_micro_handle_cnt_; idx++) {
                micro_data_infos_[idx].set_blockscan();
              }
              if (0 < end_idx && OB_FAIL(binary_check_micro_infos(micro_data_infos_, datum_utils, 0, end_idx - 1, border_rowkey, is_reverse))) {
                LOG_WARN("Fail to check_micro_infos", K(ret), K(end_idx));
              }
            } else if (start_idx < max_micro_handle_cnt_ - 1) {
              if (OB_FAIL(binary_check_micro_infos(micro_data_infos_, datum_utils, start_idx, max_micro_handle_cnt_ - 2, border_rowkey, is_reverse))) {
                LOG_WARN("Fail to check_micro_infos", K(ret), K(start_idx));
              }
            }
          }
        }
      }
    } else {
      can_blockscan_ = false;
      if (start_pos < end_pos) {
        // check border in range [start_pos, end_pos]
        if (end_idx  > start_idx) {
          // binary check in rang [start_idx, end_idx]
          if (OB_FAIL(binary_check_micro_infos(micro_data_infos_, datum_utils, start_idx, end_idx, border_rowkey, is_reverse))) {
            LOG_WARN("Fail to check_micro_infos", K(ret), K(start_idx), K(end_idx));
          }
        } else {
          cmp_ret = 0;
          // split to [start_idx, max_micro_handle_cnt_ - 1], [0, end_idx]
          if (OB_FAIL(micro_data_infos_[0].endkey_->compare(border_rowkey, datum_utils, cmp_ret, false))) {
            LOG_WARN("Fail to compare endkey", K(ret), K(border_rowkey), K(micro_data_infos_[0]));
          } else if (cmp_ret > 0) {
            for (int64_t idx = start_idx; idx < max_micro_handle_cnt_; idx++) {
              micro_data_infos_[idx].set_blockscan();
            }
            if (OB_FAIL(binary_check_micro_infos(micro_data_infos_, datum_utils, 0, end_idx, border_rowkey, is_reverse))) {
              LOG_WARN("Fail to check_micro_infos", K(ret), K(end_idx));
            }
          } else if (OB_FAIL(binary_check_micro_infos(micro_data_infos_, datum_utils, start_idx, max_micro_handle_cnt_ - 1, border_rowkey, is_reverse))) {
            LOG_WARN("Fail to check_micro_infos", K(ret), K(start_idx));
          }
        }
      }
    }

  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::refresh_blockscan_checker(
    const int64_t start_micro_idx,
    const ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  // 1. check blockscan in the rest micro data prefetched
  if (OB_UNLIKELY(!border_rowkey.is_valid() || 0 >= start_micro_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to check range block scan", K(ret), K(border_rowkey), K(start_micro_idx));
  } else {
    ObSSTableReadHandle &read_handle = read_handles_[cur_range_fetch_idx_ % max_range_prefetching_cnt_];
    if (start_micro_idx > read_handle.micro_end_idx_) {
      can_blockscan_ = false;
    } else {
      can_blockscan_ = true;
      LOG_DEBUG("check blocksan info", K(start_micro_idx), K(read_handle), K(border_rowkey));
      if (OB_FAIL(check_data_infos_border(start_micro_idx, read_handle.micro_end_idx_, border_rowkey, access_ctx_->query_flag_.is_reverse_scan()))) {
        LOG_WARN("Fail to check_data_infos_border", K(ret), K(border_rowkey), K(start_micro_idx), K_(micro_data_prefetch_idx));
      } else if (!can_blockscan_) {
      } else {
        border_rowkey_ = border_rowkey;
        // 2. update blockscan status in the TreeLevelHandle
        bool need_check = true;
        for (int16_t level = 0; OB_SUCC(ret) && level < index_tree_height_; level++) {
          tree_handles_[level].can_blockscan_ = true;
          if (need_check && level <= cur_level_) {
            if (OB_FAIL(tree_handles_[level].check_blockscan(border_rowkey))) {
              LOG_WARN("Fail to refresh blockscan status", K(ret), K(level), K(tree_handles_[level]));
            } else if (tree_handles_[level].can_blockscan_){
              need_check = false;
            }
          }
        }
      }
    }
  }
  LOG_TRACE("[BLOCKSCAN] refresh_blockscan_checker", K(ret), K(border_rowkey), KPC(this));
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::check_blockscan(bool &can_blockscan)
{
  static int64 MICRO_DATA_CHECK_DEPTH = DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT / 2;
  int ret = OB_SUCCESS;
  can_blockscan = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (current_micro_info().can_blockscan(iter_param_->has_lob_column_out())) {
    can_blockscan = true;
  } else if (!can_blockscan_) {
  } else if (OB_FAIL(check_data_infos_border(
              cur_micro_data_fetch_idx_,
              MIN(cur_micro_data_fetch_idx_ + MICRO_DATA_CHECK_DEPTH, micro_data_prefetch_idx_ - 1),
              border_rowkey_,
              access_ctx_->query_flag_.is_reverse_scan()))) {
    LOG_WARN("Fail to check_data_infos_border", K(ret), K_(micro_data_prefetch_idx),
             K_(micro_data_prefetch_idx), K_(border_rowkey));
  } else {
    can_blockscan = current_micro_info().can_blockscan(iter_param_->has_lob_column_out());
    if (!can_blockscan_) {
      clean_blockscan_check_info();
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::check_row_lock(
    const blocksstable::ObMicroIndexInfo &index_info,
    bool &is_row_lock_checked)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexTreeMultiPassPrefetcher is not inited", K(ret));
  } else if (ObStoreRowIterator::IteratorRowLockCheck == iter_type_
               && !is_row_lock_checked) {
    const int64_t read_snapshot_version = access_ctx_->trans_version_range_.snapshot_version_;
    if (!index_info.contain_uncommitted_row()
          && index_info.get_max_merged_trans_version() <= read_snapshot_version) {
      ++cur_range_fetch_idx_;
      row_lock_check_version_ = index_info.get_max_merged_trans_version();
      ret = OB_ITER_END;
    }
    is_row_lock_checked = true;
  }
  return ret;
}

//////////////////////////////////////// ObIndexTreeLevelHandle //////////////////////////////////////////////
template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::ObIndexTreeLevelHandle::prefetch(
    const ObDatumRowkey &border_rowkey,
    const int64_t level,
    ObIndexTreeMultiPassPrefetcher &prefetcher)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= level || level >= prefetcher.index_tree_height_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid prefetch level", K(ret), K(level), K(prefetcher.index_tree_height_));
  } else if (is_prefetch_end_) {
    // nothing to do
  } else {
    // update read index by the fetch_idx of next level
    int32_t child_fetch_idx = (level == prefetcher.index_tree_height_ - 1) ?
        prefetcher.cur_micro_data_fetch_idx_ : prefetcher.tree_handles_[level + 1].fetch_idx_;
    for (; read_idx_ < fetch_idx_; read_idx_++) {
      if (index_block_read_handles_[read_idx_ % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_ > child_fetch_idx) {
        break;
      }
    }

    if (INDEX_TREE_PREFETCH_DEPTH == (prefetch_idx_ - read_idx_ + 1)) {
      // suspend current prefetch when no handle can be freed
    } else {
      ObIndexTreeLevelHandle &parent = prefetcher.tree_handles_[level - 1];
      int8_t prefetch_idx = (prefetch_idx_ + 1) % INDEX_TREE_PREFETCH_DEPTH;
      ObMicroIndexInfo &index_info = index_block_read_handles_[prefetch_idx].index_info_;
      if (OB_FAIL(parent.get_next_index_row(
                  border_rowkey,
                  index_info,
                  prefetcher.iter_param_->has_lob_column_out()))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next", K(ret), KPC(this));
        } else {
          is_prefetch_end_ = parent.is_prefetch_end();
          ret = OB_SUCCESS;
        }
      } else if (nullptr != prefetcher.agg_row_store_ && prefetcher.agg_row_store_->can_agg_index_info(index_info)) {
        if (OB_FAIL(prefetcher.agg_row_store_->fill_index_info(index_info))) {
          LOG_WARN("Fail to agg index info", K(ret), KPC(this));
        } else {
          LOG_DEBUG("Success to agg index info", K(ret), K(index_info));
        }
      } else if (OB_FAIL(prefetcher.check_row_lock(index_info, is_row_lock_checked_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to check row lock", K(ret), KPC(this));
        }
      } else {
        ObSSTableReadHandle &read_handle = prefetcher.read_handles_[index_info.range_idx() % prefetcher.max_range_prefetching_cnt_];
        if (index_info.is_get() && index_info.is_macro_node() && OB_FAIL(prefetcher.check_bloom_filter(index_info, read_handle))) {
          LOG_WARN("Fail to check bloom filter", K(ret), K(index_info), K(prefetcher.current_read_handle()));
        } else if (level == prefetcher.index_tree_height_ -1 && !index_info.is_leaf_block()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected unbalanced index tree", K(ret), K(level), K(index_info), K(parent));
        } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
          if (OB_FAIL(prefetcher.prefetch_block_data(index_info, index_block_read_handles_[prefetch_idx].data_handle_, false))) {
            LOG_WARN("Fail to prefetch block data", K(ret), KPC(this));
          } else {
            prefetch_idx_++;
            parent.current_block_read_handle().end_prefetched_row_idx_++;
          }
        }
      }
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::ObIndexTreeLevelHandle::forward(
    const ObDatumRowkey &border_rowkey,
    const bool has_lob_out)
{
  int ret = OB_SUCCESS;
  if (fetch_idx_ >= prefetch_idx_) {
    ret = OB_ITER_END;
  } else {
    fetch_idx_++;
    index_scanner_.reuse();
    int8_t fetch_idx = fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH;
    ObMicroIndexInfo &index_info = index_block_read_handles_[fetch_idx].index_info_;
    if (OB_FAIL(index_block_read_handles_[fetch_idx].data_handle_.get_index_block_data(index_block_))) {
      LOG_WARN("Fail to get index block data", K(ret), KPC(this));
    } else if (index_info.is_get()) {
      if (OB_FAIL(index_scanner_.open(
          index_info.get_macro_id(),
          index_block_,
          index_info.get_query_key(),
          index_info.range_idx()))) {
        LOG_WARN("Fail to open index scanner", K(ret), KPC(this));
      }
    } else if (OB_FAIL(index_scanner_.open(
        index_info.get_macro_id(),
        index_block_,
        index_info.get_query_range(),
        index_info.range_idx(),
        index_info.is_left_border(),
        index_info.is_right_border()))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to open index scanner", K(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      if (index_info.can_blockscan(has_lob_out)) {
      } else if (OB_FAIL(check_blockscan(border_rowkey))) {
        LOG_WARN("Fail to update_blockscan", K(ret), K(border_rowkey));
      }
    } else if (OB_LIKELY(OB_BEYOND_THE_RANGE == ret)) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && 0 < fetch_idx_) {
      index_block_read_handles_[fetch_idx].end_prefetched_row_idx_ =
          index_block_read_handles_[(fetch_idx_ - 1) % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_;
    }
  }
  return ret;
}

// Explicit instantiations.
template class ObIndexTreeMultiPassPrefetcher<32, 3>;
template class ObIndexTreeMultiPassPrefetcher<2, 2>;

}
}

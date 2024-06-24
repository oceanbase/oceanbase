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
#include "ob_sstable_index_filter.h"
#include "ob_aggregated_store.h"
#include "storage/access/ob_rows_info.h"
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
  access_ctx_ = nullptr;
  long_life_allocator_ = nullptr;
  inner_reset();
}

void ObIndexTreePrefetcher::reuse()
{
  index_scanner_.reuse();
}

void ObIndexTreePrefetcher::reclaim()
{
  inner_reset();
}

void ObIndexTreePrefetcher::inner_reset()
{
  iter_type_ = 0;
  cur_level_ = 0;
  index_tree_height_ = 0;
  max_rescan_height_ = 0;
  max_rescan_range_cnt_ = 0;
  data_version_ = 0;
  table_scan_cnt_ = 0;
  sstable_ = nullptr;
  sstable_meta_handle_.reset();
  iter_param_ = nullptr;
  datum_utils_ = nullptr;
  index_scanner_.reset();
  last_micro_block_handle_.reset();
  for (int64_t i = 0; i < DEFAULT_GET_MICRO_DATA_HANDLE_CNT; ++i) {
    micro_handles_[i].reset();
  }
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
  } else if (OB_FAIL(init_basic_info(iter_type, sstable, iter_param, access_ctx))) {
    LOG_WARN("Fail to init basic info", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIndexTreePrefetcher::switch_context(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  UNUSED(query_range);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(init_basic_info(iter_type, sstable, iter_param, access_ctx))) {
    LOG_WARN("Fail to init basic info", K(ret));
  }
  return ret;
}

int ObIndexTreePrefetcher::init_basic_info(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *index_read_info = nullptr;
  if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("failed to get sstable meta handle", K(ret), K(sstable));
  } else if (OB_FAIL(iter_param.get_index_read_info(sstable.is_normal_cg_sstable(), index_read_info))) {
    LOG_WARN("failed to get index read info", KR(ret), K(sstable), K(iter_param));
  } else {
    table_scan_cnt_++;
    const bool first_scan = is_first_scan();
    iter_type_ = iter_type;
    cur_level_ = 0;
    sstable_ = &sstable;
    access_ctx_ = &access_ctx;
    iter_param_ = &iter_param;
    datum_utils_ = &index_read_info->get_datum_utils();
    data_version_ = sstable_->get_data_version();
    bool is_normal_query = !access_ctx_->query_flag_.is_daily_merge() && !access_ctx_->query_flag_.is_multi_version_minor_merge();
    index_tree_height_ = sstable_meta_handle_.get_sstable_meta().get_index_tree_height(sstable.is_ddl_merge_sstable() && is_normal_query);
    max_rescan_height_ = index_tree_height_ > max_rescan_height_ ? index_tree_height_ : max_rescan_height_;

    if (OB_ISNULL(long_life_allocator_ = access_ctx.get_long_life_allocator())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Unexpected null long life allocator", K(ret));
    } else if (index_scanner_.is_valid()) {
      if (OB_UNLIKELY(first_scan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected state, index_scanner_ is valid at first scan", K(ret), KPC(this), K(index_scanner_), K(iter_param), K(lbt()));
      } else {
        const ObTablet *cur_tablet = OB_ISNULL(iter_param_->tablet_handle_) ? nullptr : iter_param_->tablet_handle_->get_obj();
        index_scanner_.switch_context(sstable, cur_tablet, *datum_utils_, *access_ctx_,
          ObRowkeyVectorHelper::can_use_non_datum_rowkey_vector(sstable.is_normal_cg_sstable(), iter_param_->tablet_id_)
            ? iter_param_->get_rowkey_col_descs() : nullptr);
      }
    } else if (OB_FAIL(init_index_scanner(index_scanner_))) {
      LOG_WARN("Fail to init index_scanner", K(ret));
    }
  }
  return ret;
}

// prefetch row status by rowkey
// 1. not exist; 2. in row cache; 3. in block
int ObIndexTreePrefetcher::single_prefetch(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  read_handle.row_state_ = ObSSTableRowState::IN_BLOCK;
  read_handle.index_block_info_.reset();
  read_handle.index_block_info_.is_root_ = true;
  read_handle.index_block_info_.cs_row_range_.start_row_id_ = 0;
  read_handle.index_block_info_.cs_row_range_.end_row_id_ =
      sstable_meta_handle_.get_sstable_meta().get_end_row_id(sstable_->is_ddl_merge_empty_sstable());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexTreePrefetcher not init", K(ret));
  } else if (sstable_->no_data_to_read()) {
    //empty sstable
    read_handle.row_state_ = ObSSTableRowState::NOT_EXIST;
  } else if (ObStoreRowIterator::IteratorSingleGet == iter_type_ &&
             OB_FAIL(lookup_in_cache(read_handle))) {
    LOG_WARN("Failed to lookup_in_cache", K(ret));
  } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
    if (OB_FAIL(lookup_in_index_tree(read_handle, false))) {
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
  } else if (access_ctx_->enable_get_row_cache()) {
    ObRowCacheKey key(MTL_ID(), iter_param_->tablet_id_, read_handle.get_rowkey(),
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
  LOG_DEBUG("[INDEX BLOCK] prefetch in cache info", K(ret), K(read_handle.get_rowkey()), K(read_handle), KPC(this));
  return ret;
}

int ObIndexTreePrefetcher::lookup_in_index_tree(ObSSTableReadHandle &read_handle, const bool force_prefetch)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!read_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_handle));
  } else if (ObSSTableRowState::NOT_EXIST == read_handle.row_state_) {
    found = true;
  }

  ObMicroIndexInfo &index_block_info = read_handle.index_block_info_;
  while (OB_SUCC(ret) && !found && cur_level_ < index_tree_height_) {
    if (0 == cur_level_) {
      if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
        LOG_WARN("Fail to get index block root", K(ret), KPC(sstable_), KP(sstable_));
      }
    } else {
      ObMicroBlockDataHandle &curr_handle = get_read_handle(cur_level_);
      if (OB_FAIL(curr_handle.get_micro_block_data(nullptr, index_block_, false))) {
        LOG_WARN("Fail to get index block data", K(ret), K(curr_handle));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_scanner_.open(
                0 == cur_level_ ?
                ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID : get_read_handle(cur_level_).macro_block_id_,
                index_block_,
                read_handle.get_rowkey(),
                read_handle.range_idx_,
                &index_block_info))) {
      LOG_WARN("Fail to open index block scanner", K(ret), K(cur_level_), K(index_block_), K(index_tree_height_), KPC(sstable_), KP(sstable_));
    } else if (OB_FAIL(index_scanner_.get_next(index_block_info))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to get index block row", K(ret), K_(index_scanner));
      }
    } else if (index_block_info.is_macro_node() &&
               !sstable_->is_normal_cg_sstable() &&
               OB_FAIL(check_bloom_filter(index_block_info, false, read_handle))) {
      LOG_WARN("Fail to check bloom filter", K(ret), K(index_block_info), K(read_handle));
    } else if (ObSSTableRowState::NOT_EXIST == read_handle.row_state_) {
      found = true;
    } else {
      ++cur_level_;
      ObMicroBlockDataHandle &curr_handle = get_read_handle(cur_level_);
      if (OB_FAIL(prefetch_block_data(index_block_info, curr_handle, index_block_info.is_data_block()))) {
        LOG_WARN("Fail to prefetch block data", K(ret));
      } else if (index_block_info.is_data_block()) {
        found = true;
        read_handle.micro_handle_ = &curr_handle;
      } else if (!force_prefetch && ObSSTableMicroBlockState::IN_BLOCK_CACHE != curr_handle.block_state_) {
        break;
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
  if (OB_ISNULL(iter_param_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter param", K(ret), KPC(iter_param_), K(lbt()));
  } else if (OB_FAIL(index_scanner.init(
      *datum_utils_,
      *access_ctx_->stmt_allocator_,
      access_ctx_->query_flag_,
      sstable_->get_macro_offset(),
      sstable_->is_normal_cg_sstable(),
      ObRowkeyVectorHelper::can_use_non_datum_rowkey_vector(sstable_->is_normal_cg_sstable(), iter_param_->tablet_id_)
        ? iter_param_->get_rowkey_col_descs() : nullptr))) {
    LOG_WARN("init index scanner fail", K(ret), KPC(sstable_), KP(sstable_));
  } else {
    const ObTablet *cur_tablet = OB_ISNULL(iter_param_->tablet_handle_) ? nullptr : iter_param_->tablet_handle_->get_obj();
    index_scanner.set_iter_param(sstable_, cur_tablet);
  }
  return ret;
}

bool ObIndexTreePrefetcher::last_handle_hit(const ObMicroIndexInfo &block_info,
                                            const bool is_data,
                                            ObMicroBlockDataHandle &micro_handle)
{
  bool bret = false;
  const MacroBlockId macro_id = block_info.get_macro_id();
  const int32_t offset = block_info.get_block_offset();
  const int32_t size = block_info.get_block_size();
  if (is_data) {
    if (last_micro_block_handle_.in_block_state() && last_micro_block_handle_.match(macro_id, offset, size)) {
      micro_handle = last_micro_block_handle_;
      EVENT_INC(ObStatEventIds::DATA_BLOCK_CACHE_HIT);
      bret = true;
    }
  } else if (micro_handle.match(macro_id, offset, size)) {
    EVENT_INC(ObStatEventIds::INDEX_BLOCK_CACHE_HIT);
    bret = true;
  }
  return bret;
}

int ObIndexTreePrefetcher::check_bloom_filter(
    const ObMicroIndexInfo &index_info,
    const bool is_multi_check,
    ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (!index_info.is_valid() || !index_info.is_macro_node()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(index_info), K(read_handle));
  } else if (!access_ctx_->query_flag_.is_index_back() && access_ctx_->enable_bf_cache()) {
    bool is_contain = true;
    if (is_multi_check) {
      if (OB_FAIL(OB_STORE_CACHE.get_bf_cache().may_contain(MTL_ID(),
                                                            index_info.get_macro_id(),
                                                            index_info.rows_info_,
                                                            index_info.rowkey_begin_idx_,
                                                            index_info.rowkey_end_idx_,
                                                            *datum_utils_,
                                                            is_contain))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("Fail to check bloomfilter", K(ret));
        }
      }
    } else if (read_handle.is_sorted_multi_get_) {
      if (OB_FAIL(OB_STORE_CACHE.get_bf_cache().may_contain(MTL_ID(),
                                                            index_info.get_macro_id(),
                                                            index_info.rowkeys_info_,
                                                            index_info.rowkey_begin_idx_,
                                                            index_info.rowkey_end_idx_,
                                                            *datum_utils_,
                                                            is_contain))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("Fail to check bloomfilter", K(ret));
        }
      }
    } else if (OB_FAIL(OB_STORE_CACHE.get_bf_cache().may_contain(MTL_ID(),
                                                                 index_info.get_macro_id(),
                                                                 read_handle.get_rowkey(),
                                                                 *datum_utils_,
                                                                 is_contain))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("Fail to check bloomfilter", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
    } else if (is_contain) {
      read_handle.is_bf_contain_ = true;
    } else {
      read_handle.row_state_ = ObSSTableRowState::NOT_EXIST;
      ++access_ctx_->table_store_stat_.bf_filter_cnt_;
    }
    LOG_DEBUG("check bloomfilter", K(ret), K(read_handle), K(is_contain));
    if (is_multi_check) {
      access_ctx_->table_store_stat_.rowkey_prefix_ = read_handle.rows_info_->get_datum_cnt();
    } else {
      access_ctx_->table_store_stat_.rowkey_prefix_ = read_handle.get_rowkey().get_datum_cnt();
    }
    ++access_ctx_->table_store_stat_.bf_access_cnt_;
  }
  return ret;
}

int ObIndexTreePrefetcher::prefetch_block_data(
    blocksstable::ObMicroIndexInfo &index_block_info,
    ObMicroBlockDataHandle &micro_handle,
    const bool is_data,
    const bool use_multi_block_prefetch,
    const bool need_submit_io)
{
  int ret = OB_SUCCESS;
  if (is_rescan() && last_handle_hit(index_block_info, is_data, micro_handle)) {
    ++access_ctx_->table_store_stat_.block_cache_hit_cnt_;
    LOG_DEBUG("last micro block handle hits", K(is_data), K(index_block_info),
                                              K(last_micro_block_handle_), K(micro_handle));
  } else if (OB_FAIL(access_ctx_->micro_block_handle_mgr_.get_micro_block_handle(
                         index_block_info,
                         is_data,
                         !is_data || need_submit_io, /* need submit io */
                         use_multi_block_prefetch,
                         micro_handle,
                         cur_level_))) {
    if (is_data && !need_submit_io && OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Fail to get micro block handle from handle mgr", K(ret));
    }
  } else if (is_rescan() && is_data && micro_handle.in_block_state()) {
    last_micro_block_handle_ = micro_handle;
  }
  return ret;
}

////////////////////////////////// ObIndexTreeMultiPrefetcher /////////////////////////////////////////////

void ObIndexTreeMultiPrefetcher::reset()
{
  ext_read_handles_.reset();
  level_handles_.reset();
  inner_reset();
  ObIndexTreePrefetcher::reset();
}

void ObIndexTreeMultiPrefetcher::reuse()
{
  for (int64_t i = 0; is_rowkey_sorted_ && i < index_tree_height_; ++i) {
    level_handles_.at(i).reset();
  }
  inner_reset();
  ObIndexTreePrefetcher::reuse();
}

void ObIndexTreeMultiPrefetcher::reclaim()
{
  for (int64_t i = 0; i < max_rescan_range_cnt_; ++i) {
    ext_read_handles_.at(i).reset();
  }
  ext_read_handles_.clear();
  for (int64_t i = 0; is_rowkey_sorted_ && i < max_rescan_height_; ++i) {
    level_handles_.at(i).reset();
  }
  level_handles_.clear();
  inner_reset();
  ObIndexTreePrefetcher::reclaim();
}

void ObIndexTreeMultiPrefetcher::inner_reset()
{
  is_rowkey_sorted_ = false;
  fetch_rowkey_idx_ = 0;
  prefetch_rowkey_idx_ = 0;
  prefetched_rowkey_cnt_ = 0;
  rowkeys_ = nullptr;
}

int ObIndexTreeMultiPrefetcher::init(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIndexTreeMultiPrefetcher has been inited", K(ret));
  } else if (OB_FAIL(init_basic_info(iter_type, sstable, iter_param, access_ctx))) {
    LOG_WARN("Fail to init basic info", K(ret));
  } else {
    is_rowkey_sorted_ = access_ctx.query_flag_.is_ordered_scan() &&
                        !access_ctx.query_flag_.is_reverse_scan() &&
                        ObStoreRowIterator::IteratorMultiGet == iter_type_ &&
                        !sstable.is_ddl_sstable();;
    ext_read_handles_.set_allocator(long_life_allocator_);
    rowkeys_ = static_cast<const common::ObIArray<blocksstable::ObDatumRowkey> *> (query_range);
    const int32_t range_count = rowkeys_->count();
    max_handle_prefetching_cnt_ = min(range_count, MAX_MULTIGET_MICRO_DATA_HANDLE_CNT);
    max_rescan_range_cnt_ = max_handle_prefetching_cnt_ > max_rescan_range_cnt_ ? max_handle_prefetching_cnt_ : max_rescan_range_cnt_;
    const int64_t handle_cnt = iter_param.is_use_global_iter_pool() ? MAX_MULTIGET_MICRO_DATA_HANDLE_CNT : max_handle_prefetching_cnt_;
    if (0 == range_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range count should be greater than 0", K(ret), K(range_count));
    } else if (OB_FAIL(ext_read_handles_.prepare_reallocate(handle_cnt))) {
      LOG_WARN("Fail to init read_handles", K(ret), K(handle_cnt));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    row_states_.set_allocator(long_life_allocator_);
    level_handles_.set_allocator(long_life_allocator_);
    if (is_rowkey_sorted_ && OB_FAIL(init_for_sorted_multi_get())) {
      LOG_WARN("Fail to init fro sorted multi get", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObIndexTreeMultiPrefetcher::switch_context(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(init_basic_info(iter_type, sstable, iter_param, access_ctx))) {
    LOG_WARN("Fail to init basic info", K(ret));
  } else {
    is_rowkey_sorted_ = access_ctx.query_flag_.is_ordered_scan() &&
                        !access_ctx.query_flag_.is_reverse_scan() &&
                        ObStoreRowIterator::IteratorMultiGet == iter_type_ &&
                        !sstable.is_ddl_sstable();
    rowkeys_ = static_cast<const common::ObIArray<blocksstable::ObDatumRowkey> *> (query_range);
    max_handle_prefetching_cnt_ = min(rowkeys_->count(), MAX_MULTIGET_MICRO_DATA_HANDLE_CNT);
    max_rescan_range_cnt_ = max_handle_prefetching_cnt_ > max_rescan_range_cnt_ ? max_handle_prefetching_cnt_ : max_rescan_range_cnt_;
    if (OB_FAIL(ext_read_handles_.prepare_reallocate(max_handle_prefetching_cnt_))) {
      LOG_WARN("Fail to init read_handles", K(ret), K(max_handle_prefetching_cnt_));
    }
  }
  if (OB_SUCC(ret)) {
    row_states_.set_allocator(long_life_allocator_);
    level_handles_.set_allocator(long_life_allocator_);
    if (is_rowkey_sorted_ && OB_FAIL(init_for_sorted_multi_get())) {
      LOG_WARN("Fail to init fro sorted multi get", K(ret));
    }
  }
  return ret;
}

int ObIndexTreeMultiPrefetcher::init_for_sorted_multi_get()
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_cnt = rowkeys_->count();
  int64_t max_height = index_tree_height_;
  if (1 >= index_tree_height_ || MAX_INDEX_TREE_HEIGHT < index_tree_height_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index tree height", K(ret), K(index_tree_height_));
  } else if (iter_param_->is_use_global_iter_pool()) {
    max_height = MAX_INDEX_TREE_HEIGHT;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(level_handles_.prepare_reallocate(max_height))) {
    LOG_WARN("Fail to init level handles", K(ret), K(max_height));
  } else if (OB_FAIL(row_states_.prepare_reallocate(rowkey_cnt))) {
    LOG_WARN("Fail to init row_states", K(ret), K(rowkey_cnt));
  } else {
    for (int64_t i = 0; i < rowkey_cnt; ++i) {
      row_states_.at(i) = ObSSTableRowState::IN_BLOCK;
    }
    rowkeys_info_.rowkeys_ = rowkeys_;
    rowkeys_info_.row_states_ = &row_states_;
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
    for (int64_t i = 0; is_rowkey_sorted_ && i < index_tree_height_; ++i) {
      ObCachedLevelMicroDataHandle &level_handle = level_handles_.at(i);
      if (level_handle.is_valid_ && fetch_rowkey_idx_ >= level_handle.rowkey_end_idx_) {
        level_handle.reset();
      }
    }
    for (int64_t i = fetch_rowkey_idx_;
         OB_SUCC(ret) && prefetched_rowkey_cnt_ < rowkey_cnt && i < fetch_rowkey_idx_ + max_handle_prefetching_cnt_;
         ++i) {
      const bool is_rowkey_to_fetched = i == fetch_rowkey_idx_;
      const bool is_empty_handle = i >= prefetch_rowkey_idx_;
      ObSSTableReadHandleExt &read_handle = ext_read_handles_[i % max_handle_prefetching_cnt_];
      if (is_empty_handle && prefetch_rowkey_idx_ < rowkey_cnt) {
        read_handle.reuse();
        read_handle.row_state_ = ObSSTableRowState::IN_BLOCK;
        read_handle.range_idx_ = prefetch_rowkey_idx_;
        read_handle.is_get_ = true;
        read_handle.index_block_info_.is_root_ = true;
        read_handle.index_block_info_.cs_row_range_.start_row_id_ = 0;
        read_handle.index_block_info_.cs_row_range_.end_row_id_ =
            sstable_meta_handle_.get_sstable_meta().get_end_row_id(sstable_->is_ddl_merge_empty_sstable());
        read_handle.is_sorted_multi_get_ = is_rowkey_sorted_;
        if (is_rowkey_sorted_) {
          read_handle.rowkeys_info_ = &rowkeys_info_;
          read_handle.index_block_info_.rowkey_begin_idx_ = prefetch_rowkey_idx_;
          read_handle.index_block_info_.rowkey_end_idx_ = rowkey_cnt;
        } else {
          read_handle.rowkey_ = &rowkeys_->at(prefetch_rowkey_idx_);
        }
        prefetch_rowkey_idx_++;

        if (is_rowkey_sorted_ && rowkeys_info_.is_rowkey_not_exist(read_handle.range_idx_)) {
          read_handle.row_state_ = ObSSTableRowState::NOT_EXIST;
           mark_cur_rowkey_prefetched(read_handle);
        } else if (OB_FAIL(ObStoreRowIterator::IteratorMultiGet == iter_type_ &&
                   lookup_in_cache(read_handle))) {
          LOG_WARN("Failed to lookup_in_cache", K(ret));
        } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
          if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
            LOG_WARN("Fail to get index block root", K(ret), KPC(sstable_), KP(sstable_));
          } else if (OB_FAIL(drill_down(ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID, read_handle, false, is_rowkey_to_fetched))) {
            LOG_WARN("Fail to prefetch next level", K(ret), K(index_block_), K(read_handle), KPC(this));
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
            (ObSSTableMicroBlockState::IN_BLOCK_IO != read_handle.micro_handle_->block_state_
             && ObSSTableMicroBlockState::NEED_SYNC_IO != read_handle.micro_handle_->block_state_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Fail to prefetch, unexpected read handle", K(ret), K(read_handle), KPC(this));
        } else if (OB_FAIL(access_ctx_->micro_block_handle_mgr_.get_micro_block_handle(
                    cur_index_info,
                    cur_index_info.is_data_block(),
                    false, /* need submit io */
                    false, /* use_multi_block_prefetch */
                    next_handle,
                    cur_level_))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            //not in cache yet, stop this rowkey prefetching if it's not the rowkey to be feteched
            ret = OB_SUCCESS;
            if (is_rowkey_to_fetched) {
              if (OB_FAIL(read_handle.micro_handle_->get_micro_block_data(nullptr, index_block_, false))) {
                LOG_WARN("Fail to get index block data", K(ret), KPC(read_handle.micro_handle_));
              }
            } else {
              stop_prefetch = true;
            }
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
  ObMicroIndexInfo &index_block_info = read_handle.index_block_info_;
  read_handle.cur_level_++;
  bool is_covered = false;
  ObCachedLevelMicroDataHandle *level_handle = is_rowkey_sorted_ ? &level_handles_.at(read_handle.cur_level_) : nullptr;
  const bool pre_locate = nullptr != level_handle && !level_handle->is_valid_;
  if (is_rowkey_sorted_) {
    if (level_handle->is_covered(read_handle.range_idx_)) {
      is_covered = true;
      if (FALSE_IT(read_handle.set_cur_micro_handle(level_handle->handle_))) {
      } else if (cur_level_is_leaf && read_handle.cur_level_ != index_tree_height_ - 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to prefetch, unexpected level", K(ret), K(cur_level_is_leaf),
                K(read_handle.cur_level_), K(index_tree_height_));
      } else if (cur_level_is_leaf) {
        mark_cur_rowkey_prefetched(read_handle);
      } else if (OB_FAIL(read_handle.micro_handle_->get_micro_block_data(nullptr, index_block_, false))) {
        LOG_WARN("Fail to get index block data", K(ret));
      } else if (OB_FAIL(drill_down(level_handle->macro_id_, read_handle, level_handle->is_leaf_block_, force_prefetch))) {
        LOG_WARN("Fail to prefetch data block", K(ret), K(read_handle));
      }
    } else if (!pre_locate) {
      index_block_info.rowkey_end_idx_ = index_block_info.rowkey_begin_idx_ + 1;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_covered) {
  } else if (is_rowkey_sorted_ && OB_FAIL(index_scanner_.open(macro_id,
                                                              index_block_,
                                                              read_handle.rowkeys_info_,
                                                              index_block_info.rowkey_begin_idx_,
                                                              index_block_info.rowkey_end_idx_))) {
    LOG_WARN("Fail to open index block scanner", K(ret), K(index_block_), K(read_handle));
  } else if (!is_rowkey_sorted_ && OB_FAIL(index_scanner_.open(macro_id,
                                                               index_block_,
                                                               *read_handle.rowkey_,
                                                               read_handle.range_idx_,
                                                               &index_block_info))) {
    LOG_WARN("Fail to open index block scanner", K(ret), K(index_block_), K(read_handle));
  } else if (cur_level_is_leaf && read_handle.cur_level_ != index_tree_height_ - 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to prefetch, unexpected level", K(ret), K(cur_level_is_leaf),
             K(read_handle.cur_level_), K(index_tree_height_));
  } else if (OB_FAIL(index_scanner_.get_next(index_block_info, false, pre_locate))) {
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
  } else if (index_block_info.is_macro_node() &&
             !sstable_->is_normal_cg_sstable() &&
             OB_FAIL(check_bloom_filter(index_block_info, false, read_handle))) {
    LOG_WARN("Fail to check bloom filter", K(ret), K(index_block_info), K(read_handle));
  } else if (ObSSTableRowState::NOT_EXIST == read_handle.row_state_) {
    mark_cur_rowkey_prefetched(read_handle);
  } else {
    // hold block cache of the parent temporaliy to avoid freed
    ObMicroBlockDataHandle &next_handle = read_handle.get_read_handle();
    if (OB_FAIL(prefetch_block_data(index_block_info, next_handle, cur_level_is_leaf))) {
      LOG_WARN("fail to prefetch_block_data", K(ret), K(read_handle), K(index_block_info), K(cur_level_is_leaf));
    } else if (FALSE_IT(read_handle.set_cur_micro_handle(next_handle))) {
    } else if (pre_locate &&
               index_block_info.rowkey_end_idx_ - index_block_info.rowkey_begin_idx_ > 1) {
      level_handles_.at(read_handle.cur_level_).set_handle(index_block_info.is_leaf_block(),
                                                           index_block_info.rowkey_begin_idx_,
                                                           index_block_info.rowkey_end_idx_,
                                                           index_block_info.get_macro_id(),
                                                           next_handle);
    }
    if (OB_FAIL(ret)) {
    } else if (cur_level_is_leaf) {
      mark_cur_rowkey_prefetched(read_handle);
    } else if (force_prefetch || ObSSTableMicroBlockState::IN_BLOCK_CACHE == next_handle.block_state_) {
      if (ObSSTableMicroBlockState::IN_BLOCK_CACHE == next_handle.block_state_) {
        LOG_DEBUG("cur handle is in cache", K(read_handle), K(index_block_info), K(next_handle));
        if (OB_FAIL(next_handle.get_cached_index_block_data(index_block_))) {
          LOG_WARN("Fail to get index block data", K(ret), K(next_handle));
        }
      } else {
        LOG_DEBUG("cur handle is not in cache, force prefetch", K(read_handle), K(index_block_info), K(next_handle));
        if (OB_FAIL(next_handle.get_micro_block_data(nullptr, index_block_, false))) {
          LOG_WARN("Fail to get index block data", K(ret), K(next_handle));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(drill_down(index_block_info.get_macro_id(), read_handle, index_block_info.is_leaf_block(), force_prefetch))) {
          LOG_WARN("Faile to prefetch data block", K(ret), K(read_handle));
        }
      }
    } else {
      LOG_DEBUG("cur handle is not in cache, has submit io", K(read_handle), K(index_block_info), K(next_handle));
    }
  }
  return ret;
}

////////////////////////////////// MultiPassPrefetcher /////////////////////////////////////////////
template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::~ObIndexTreeMultiPassPrefetcher()
{
  reset_tree_handles();
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::reset()
{
  for (int64_t i = 0; i < DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT; i++) {
    micro_data_handles_[i].reset();
  }
  reset_tree_handles();
  read_handles_.reset();
  inner_reset();
  multi_io_params_.reset();
  max_range_prefetching_cnt_ = 0;
  max_micro_handle_cnt_ = 0;
  ObIndexTreePrefetcher::reset();
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::reuse()
{
  for (int64_t i = 0; nullptr != tree_handles_ && i < tree_handle_cap_; i++) {
    tree_handles_[i].reuse();
  }
  clean_blockscan_check_info();
  inner_reset();
  multi_io_params_.reuse();
  ObIndexTreePrefetcher::reuse();
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::reclaim()
{
  for (int64_t i = 0; i < max_rescan_range_cnt_; ++i) {
    read_handles_.at(i).reset();
  }
  read_handles_.clear();
  for (int16_t i = 0; nullptr != tree_handles_ && i < max_rescan_height_; i++) {
    tree_handles_[i].reset();
  }
  for (int64_t i = 0; i < DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT; i++) {
    micro_data_handles_[i].reset();
  }
  inner_reset();
  multi_io_params_.reset();
  ObIndexTreePrefetcher::reclaim();
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::inner_reset()
{
  can_blockscan_ = false;
  is_prefetch_end_ = false;
  is_row_lock_checked_ = false;
  need_check_prefetch_depth_ = false;
  use_multi_block_prefetch_ = false;
  need_submit_io_ = true;
  cur_range_fetch_idx_ = 0;
  cur_range_prefetch_idx_ = 0;
  cur_micro_data_fetch_idx_ = -1;
  micro_data_prefetch_idx_ = 0;
  row_lock_check_version_ = transaction::ObTransVersion::INVALID_TRANS_VERSION;
  agg_row_store_ = nullptr;
  prefetch_depth_ = 1;
  total_micro_data_cnt_ = 0;
  query_range_ = nullptr;
  border_rowkey_.reset();
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::reset_tree_handles()
{
  if (nullptr != tree_handles_) {
    for (int16_t level = 0; level < tree_handle_cap_; level++) {
      tree_handles_[level].~ObIndexTreeLevelHandle();
    }
    if (nullptr != long_life_allocator_ && tree_handle_cap_ > 0) {
      long_life_allocator_->free(tree_handles_);
      tree_handles_ = nullptr;
    }
  }
  tree_handle_cap_ = 0;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::init_tree_handles(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (count > tree_handle_cap_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = long_life_allocator_->alloc(sizeof(ObIndexTreeLevelHandle) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory", K(ret), K(count));
    } else {
      reset_tree_handles();
      tree_handles_ = new (buf) ObIndexTreeLevelHandle [count];
      tree_handle_cap_ = count;
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::get_prefetch_depth(int64_t &depth)
{
  int ret = OB_SUCCESS;
  depth = 0;
  prefetch_depth_ = MIN(2 * prefetch_depth_, DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT);
  if (need_check_prefetch_depth_) {
    int64_t prefetch_micro_cnt = MAX(1,
                                     (access_ctx_->limit_param_->offset_ + access_ctx_->limit_param_->limit_ - access_ctx_->out_cnt_ + \
                                      SSTABLE_MICRO_AVG_COUNT - 1) / SSTABLE_MICRO_AVG_COUNT);
    prefetch_depth_ = MIN(prefetch_depth_, prefetch_micro_cnt);
  }
  depth = min(static_cast<int64_t>(prefetch_depth_),
              max_micro_handle_cnt_ - (micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_));
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::init(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIndexTreeMultiPassPrefetcher has been inited", K(ret));
  } else if (DATA_PREFETCH_DEPTH > MAX_DATA_PREFETCH_DEPTH || INDEX_PREFETCH_DEPTH > MAX_INDEX_PREFETCH_DEPTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Prefetch depth is too large", K(ret), K(DATA_PREFETCH_DEPTH), K(INDEX_PREFETCH_DEPTH));
  } else if (sstable.no_data_to_read()) {
    is_prefetch_end_ = true;
    is_inited_ = true;
  } else if (OB_ISNULL(long_life_allocator_ = access_ctx.get_long_life_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null long life allocator", K(ret));
  } else {
    read_handles_.set_allocator(long_life_allocator_);
    max_micro_handle_cnt_ = DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT;
    if (OB_FAIL(init_basic_info(iter_type, sstable, iter_param, access_ctx, query_range))) {
      LOG_WARN("Fail to init basic info", K(ret), K(access_ctx));
    } else {
      for (int64_t level = 0; OB_SUCC(ret) && level < index_tree_height_; level++) {
        if (OB_FAIL(init_index_scanner(tree_handles_[level].index_scanner_))) {
          LOG_WARN("Fail to init index_scanner", K(ret), K(level));
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_scan_cnt_++;
      is_inited_ = true;
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::switch_context(
    const int iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  const bool first_scan = is_first_scan();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (sstable.no_data_to_read()) {
    is_prefetch_end_ = true;
  } else if (OB_ISNULL(long_life_allocator_ = access_ctx.get_long_life_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null long life allocator", K(ret));
  } else if (OB_FAIL(init_basic_info(iter_type, sstable, iter_param, access_ctx, query_range))) {
    LOG_WARN("Fail to init basic info", K(ret), K(access_ctx));
  } else {
    table_scan_cnt_++;
    for (int64_t level = 0; OB_SUCC(ret) && level < index_tree_height_; level++) {
      if (tree_handles_[level].index_scanner_.is_valid()) {
        if (OB_UNLIKELY(first_scan)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected state, index_scanner_ is valid at first scan", K(ret), K(level), KPC(this),
              K(tree_handles_[level].index_scanner_), KPC(iter_param_), K(lbt()));
        } else if (OB_ISNULL(iter_param_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid iter param", K(ret), KPC(iter_param_), K(lbt()));
        } else {
          const ObTablet *cur_tablet = OB_ISNULL(iter_param_->tablet_handle_) ? nullptr : iter_param_->tablet_handle_->get_obj();
          tree_handles_[level].index_scanner_.switch_context(sstable, cur_tablet, *datum_utils_, *access_ctx_,
            ObRowkeyVectorHelper::can_use_non_datum_rowkey_vector(sstable.is_normal_cg_sstable(), iter_param_->tablet_id_)
              ? iter_param_->get_rowkey_col_descs() : nullptr);
        }
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
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  int32_t range_count = 0;
  int32_t max_handle_cnt = 0;
  int32_t max_height = 0;
  max_micro_handle_cnt_ = DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT;
  const ObITableReadInfo *index_read_info = nullptr;
  sstable_ = &sstable;
  iter_param_ = &iter_param;
  access_ctx_ = &access_ctx;
  data_version_ = sstable_->get_data_version();
  cur_level_ = 0;
  iter_type_ = iter_type;
  need_check_prefetch_depth_ =
      ObStoreRowIterator::is_scan(iter_type) &&
      iter_param_->limit_prefetch_ &&
      nullptr != access_ctx_->limit_param_ &&
      access_ctx_->limit_param_->limit_ >= 0 &&
      access_ctx_->limit_param_->limit_ < 4096 &&
      access_ctx_->limit_param_->offset_ < INT32_MAX;
  use_multi_block_prefetch_ = (iter_param.get_io_read_batch_size() > 0);
  switch (iter_type) {
    case ObStoreRowIterator::IteratorMultiGet:
    case ObStoreRowIterator::IteratorCOMultiGet: {
      rowkeys_ = static_cast<const common::ObIArray<blocksstable::ObDatumRowkey> *> (query_range);
      range_count = rowkeys_->count();
      max_range_prefetching_cnt_ = min(range_count, DEFAULT_SCAN_RANGE_PREFETCH_CNT);
      if (0 == range_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range count should be greater than 0", K(ret), K(range_count));
      }
      break;
    }
    case ObStoreRowIterator::IteratorScan:
    case ObStoreRowIterator::IteratorCOScan:
      range_ = static_cast<const blocksstable::ObDatumRange *>(query_range);
      max_range_prefetching_cnt_ = DEFAULT_SCAN_RANGE_PREFETCH_CNT;
      break;
    case ObStoreRowIterator::IteratorRowLockCheck:
    case ObStoreRowIterator::IteratorRowLockAndDuplicationCheck: {
      range_ = static_cast<const blocksstable::ObDatumRange *>(query_range);
      max_range_prefetching_cnt_ = 1;
      break;
    }
    case ObStoreRowIterator::IteratorMultiScan:
    case ObStoreRowIterator::IteratorCOMultiScan: {
      ranges_ = static_cast<const common::ObIArray<blocksstable::ObDatumRange> *>(query_range);
      range_count = ranges_->count();
      max_range_prefetching_cnt_ = DEFAULT_SCAN_RANGE_PREFETCH_CNT;
      if (0 == range_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range count should be greater than 0", K(ret), K(range_count));
      }
      break;
    }
    case ObStoreRowIterator::IteratorMultiRowLockCheck: {
      rows_info_ = static_cast<const ObRowsInfo *>(query_range);
      range_count = 1;
      max_range_prefetching_cnt_ = 1;
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid store iterator type", K(ret), K(iter_type));
  }
  max_rescan_range_cnt_ = max_range_prefetching_cnt_ > max_rescan_range_cnt_ ? max_range_prefetching_cnt_ : max_rescan_range_cnt_;
  bool is_normal_query = !access_ctx_->query_flag_.is_daily_merge() && !access_ctx_->query_flag_.is_multi_version_minor_merge();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(iter_param.get_index_read_info(sstable.is_normal_cg_sstable(), index_read_info))) {
    LOG_WARN("failed to get index read info", KR(ret), K(sstable), K(iter_param));
  } else if (FALSE_IT(datum_utils_ = &index_read_info->get_datum_utils())) {
  } else if (OB_FAIL(sstable.get_meta(sstable_meta_handle_))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else if (FALSE_IT(index_tree_height_ = sstable_meta_handle_.get_sstable_meta().get_index_tree_height(is_normal_query && sstable.is_ddl_merge_sstable()))) {
  } else if (FALSE_IT(max_rescan_height_ = index_tree_height_ > max_rescan_height_ ? index_tree_height_ : max_rescan_height_)) {
  } else if (1 >= index_tree_height_ || MAX_INDEX_TREE_HEIGHT < index_tree_height_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index tree height", K(ret), K(index_tree_height_));
  } else if (iter_param.is_use_global_iter_pool()) {
    max_handle_cnt = DEFAULT_SCAN_RANGE_PREFETCH_CNT;
    max_height = MAX_INDEX_TREE_HEIGHT;
  } else {
    max_handle_cnt = max_range_prefetching_cnt_;
    max_height = index_tree_height_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_handles_.prepare_reallocate(max_handle_cnt))) {
    LOG_WARN("Fail to init read_handles", K(ret), K(max_handle_cnt));
  } else if (OB_FAIL(init_tree_handles(max_height))) {
    LOG_WARN("Fail to init tree handles", K(ret), K(max_height));
  } else if (use_multi_block_prefetch_ &&
             OB_FAIL(multi_io_params_.init(
                     iter_param,
                     max_micro_handle_cnt_,
                     access_ctx_->query_flag_.is_reverse_scan(),
                     *access_ctx_->stmt_allocator_))) {
    LOG_WARN("Fail to init multi io params", K(ret));
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
void ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::inc_cur_micro_data_fetch_idx()
{
  current_micro_handle().reset();
  ++cur_micro_data_fetch_idx_;
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexTreeMultiPassPrefetcher not init", K(ret));
  } else if (is_prefetch_end_) {
  } else if (OB_FAIL(prefetch_index_tree())) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      is_prefetch_end_ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Fail to prefetch index tree", K(ret), K(index_tree_height_), K(cur_level_));
    }
  } else if (OB_FAIL(prefetch_micro_data())) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      is_prefetch_end_ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("Fail to prefetch", K(ret), K(index_tree_height_), K(cur_level_));
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
    } else if (!read_handle.is_valid()) {
    } else if (read_handle.is_get_) {
      // get
      if (OB_FAIL(lookup_in_cache(read_handle))) {
        LOG_WARN("Failed to lookup_in_cache", K(ret));
      } else if (ObSSTableRowState::IN_BLOCK == read_handle.row_state_) {
        if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
          LOG_WARN("Fail to get index block root", K(ret), KPC(sstable_), KP(sstable_));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(tree_handle.index_scanner_.open(
            ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
            index_block_,
            *read_handle.rowkey_,
            read_handle.range_idx_))) {
          LOG_WARN("Fail to open index block scanner", K(ret), K(read_handle));
        }
      }
    } else if (!is_multi_check()) {
      // scan
      read_handle.row_state_ = ObSSTableRowState::IN_BLOCK;
      if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
        LOG_WARN("Fail to get index tree root", K(ret), KPC(sstable_), KP(sstable_));
      }
      if (OB_FAIL(ret)) {
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
      }
    } else {
      read_handle.row_state_ = ObSSTableRowState::IN_BLOCK;
      if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
        LOG_WARN("Fail to get index tree root", K(ret), KPC(sstable_), KP(sstable_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tree_handle.index_scanner_.open(ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
                                                         index_block_,
                                                         read_handle.rows_info_,
                                                         0,
                                                         read_handle.rows_info_->get_rowkey_cnt()))) {
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
    if (OB_SUCC(ret) && read_handle.is_valid() && OB_FAIL(tree_handle.check_blockscan(border_rowkey_))) {
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
  int64_t prefetched_cnt = 0;
  int64_t prefetch_micro_idx = 0;
  int64_t prefetch_depth = 0;
  if (OB_UNLIKELY(index_tree_height_ <= cur_level_ ||
                  micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_ > max_micro_handle_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected prefetch status", K(ret), K_(cur_level), K_(index_tree_height),
             K_(micro_data_prefetch_idx), K_(cur_micro_data_fetch_idx), K_(max_micro_handle_cnt));
  } else if (micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_ == max_micro_handle_cnt_ ||
             (use_multi_block_prefetch_ && prefetch_depth_ > MIN_DATA_READ_BATCH_COUNT &&
              (max_micro_handle_cnt_ - (micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_)) < MIN_DATA_READ_BATCH_COUNT)) {
    // DataBlock ring buf full
  } else if (OB_FAIL(get_prefetch_depth(prefetch_depth))) {
    LOG_WARN("Fail to get prefetch depth", K(ret));
  } else {
    while (OB_SUCC(ret) && prefetched_cnt < prefetch_depth) {
      if (OB_FAIL(drill_down())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get index leaf", K(ret), K(cur_level_), K(prefetch_depth), K(prefetched_cnt));
        } // else prefetch_end
      } else if (index_tree_height_ - 1 != cur_level_) {
        // not leaf level, prefetch index tree
        break;
      } else {
        // read index leaf and prefetch micro data
        while (OB_SUCC(ret) && prefetched_cnt < prefetch_depth) {
          prefetch_micro_idx = micro_data_prefetch_idx_ % max_micro_handle_cnt_;
          ObMicroIndexInfo &block_info = micro_data_infos_[prefetch_micro_idx];
          ObSSTableIndexFilter *sstable_index_filter = iter_param_->sstable_index_filter_;
          ObSampleFilterExecutor *sample_executor = sstable_->is_major_sstable() ?
              static_cast<ObSampleFilterExecutor *>(access_ctx_->get_sample_executor()) : nullptr;
          if (access_ctx_->micro_block_handle_mgr_.reach_hold_limit()
              && micro_data_prefetch_idx_ > cur_micro_data_fetch_idx_ + 1) {
            LOG_DEBUG("micro block handle mgr has reach hold limit, stop prefetch", K(prefetch_depth),
                K(prefetched_cnt), K(micro_data_prefetch_idx_), K(cur_micro_data_fetch_idx_),
                K(access_ctx_->micro_block_handle_mgr_));
            prefetch_depth = prefetched_cnt;
            break;
          } else if (OB_FAIL(tree_handles_[cur_level_].get_next_data_row(is_multi_check(), block_info))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get next", K(ret), K(cur_level_), K(tree_handles_[cur_level_]));
            } else {
              // open next leaf by get_next_index_leaf in the next loop
              ret = OB_SUCCESS;
              break;
            }
          } else if (nullptr != sample_executor
                      && OB_FAIL(sample_executor->check_sample_block(block_info, cur_level_ + 1, tree_handles_[cur_level_].fetch_idx_,
                                                                   micro_data_prefetch_idx_, iter_param_->has_lob_column_out()))) {
            LOG_WARN("Failed to check if can skip micro block in sample", K(ret), K_(cur_level), K(block_info), KPC(sample_executor));
          } else if (nullptr != sstable_index_filter
                      && can_index_filter_skip(block_info, sample_executor)
                      && OB_FAIL(sstable_index_filter->check_range(iter_param_->read_info_,
                                  block_info, *(access_ctx_->allocator_), iter_param_->vectorized_enabled_))) {
            LOG_WARN("Fail to check if can skip prefetch", K(ret), K(block_info));
          } else if (block_info.is_filter_always_false()) {
            continue;
          } else if (nullptr != agg_row_store_ && agg_row_store_->can_agg_index_info(block_info)) {
            if (OB_FAIL(agg_row_store_->fill_index_info(block_info))) {
              LOG_WARN("Fail to agg index info", K(ret), K(block_info), KPC(this));
            } else {
              LOG_DEBUG("Success to agg index info", K(ret), K(block_info), KPC(agg_row_store_));
              continue;
            }
          } else if (OB_FAIL(check_row_lock(block_info, is_row_lock_checked_))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("Fail to check row lock", K(ret), K(block_info), KPC(this));
            }
          } else if (OB_FAIL(prefetch_data_block(micro_data_prefetch_idx_, block_info, micro_data_handles_[prefetch_micro_idx]))) {
            LOG_WARN("fail to prefetch_block_data", K(ret), K(block_info));
          }

          if OB_SUCC(ret) {
            prefetched_cnt++;
            micro_data_prefetch_idx_++;
            tree_handles_[cur_level_].current_block_read_handle().end_prefetched_row_idx_++;
          }
        }
        if (OB_SUCC(ret) && multi_io_params_.count() > 0 &&
            OB_FAIL(prefetch_multi_data_block(micro_data_prefetch_idx_))) {
          LOG_WARN("Fail to prefetch multi block", K(ret), K_(micro_data_prefetch_idx), K_(multi_io_params));
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
      } else if (OB_FAIL(tree_handles_[cur_level_ + 1].forward(*this, iter_param_->has_lob_column_out()))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to consume tree handle", K(ret), K(cur_level_), K(tree_handles_[cur_level_ + 1]), K(tree_handles_[cur_level_]));
        } else if (!tree_handles_[cur_level_ + 1].is_prefetch_end()) {
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
      case ObStoreRowIterator::IteratorMultiGet:
      case ObStoreRowIterator::IteratorCOMultiGet: {
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
      case ObStoreRowIterator::IteratorCOScan:
      case ObStoreRowIterator::IteratorRowLockCheck:
      case ObStoreRowIterator::IteratorRowLockAndDuplicationCheck: {
        read_handle.reuse();
        read_handle.range_ = range_;
        read_handle.range_idx_ = cur_range_prefetch_idx_;
        read_handle.is_get_ = false;
        cur_range_prefetch_idx_++;
        tree_handle.set_prefetch_end();
        break;
      }
      case ObStoreRowIterator::IteratorMultiScan:
      case ObStoreRowIterator::IteratorCOMultiScan: {
        read_handle.reuse();
        read_handle.range_ = &ranges_->at(cur_range_prefetch_idx_);
        read_handle.range_idx_ = cur_range_prefetch_idx_;
        read_handle.is_get_ = false;
        if (++cur_range_prefetch_idx_ >= ranges_->count()) {
          tree_handle.set_prefetch_end();
        }
        break;
      }
      case ObStoreRowIterator::IteratorMultiRowLockCheck: {
        read_handle.reuse();
        read_handle.rows_info_ = rows_info_;
        read_handle.range_idx_ = cur_range_prefetch_idx_;
        read_handle.is_get_ = false;
        cur_range_prefetch_idx_++;
        tree_handle.set_prefetch_end();
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
    } else if (OB_FAIL(index_info.endkey_.compare(rowkey, datum_utils_, cmp_ret, false))) {
      LOG_WARN("fail to compare rowkey", K(ret), K(index_info.endkey_), K(rowkey));
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
        if (OB_UNLIKELY(check_border_idx > 0)) {
          // move to left if the two rightmost endkeys is same(possible in multiple ranges)
          int cmp_ret = 0;
          for (; OB_SUCC(ret) && check_border_idx > start; check_border_idx--) {
            const ObCommonDatumRowkey &cur_endkey = (micro_infos + check_border_idx)->endkey_;
            const ObCommonDatumRowkey &prev_endkey = (micro_infos + check_border_idx - 1)->endkey_;
            if (OB_FAIL(cur_endkey.compare(prev_endkey, datum_utils, cmp_ret, false))) {
              LOG_WARN("fail to compare rowkey", K(ret), K(cur_endkey), K(prev_endkey));
            } else if (cmp_ret != 0) {
              break;
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
      if (OB_FAIL(micro_data_infos_[end_idx].endkey_.compare(border_rowkey, datum_utils, cmp_ret, false))) {
        LOG_WARN("Fail to compare endkey", K(ret), K(border_rowkey), K(micro_data_infos_[end_idx].endkey_));
      } else if (cmp_ret < 0) {
        for (int64_t pos = start_pos; pos <= end_pos; pos++) {
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
            if (OB_FAIL(micro_data_infos_[max_micro_handle_cnt_ - 1].endkey_.compare(border_rowkey, datum_utils, cmp_ret, false))) {
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
          if (OB_FAIL(micro_data_infos_[0].endkey_.compare(border_rowkey, datum_utils, cmp_ret, false))) {
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
  if (OB_UNLIKELY(!border_rowkey.is_valid() || 0 > start_micro_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to check range block scan", K(ret), K(border_rowkey), K(start_micro_idx));
  } else if (0 == start_micro_idx) {
      can_blockscan_ = true;
      LOG_DEBUG("check blocksan info", K(start_micro_idx), K(border_rowkey));
      border_rowkey_ = border_rowkey;
      for (int16_t level = 0; OB_SUCC(ret) && level < index_tree_height_; level++) {
        tree_handles_[level].can_blockscan_ = true;
        if (level <= cur_level_) {
          if (OB_FAIL(tree_handles_[level].check_blockscan(border_rowkey))) {
            LOG_WARN("Fail to refresh blockscan status", K(ret), K(level), K(tree_handles_[level]));
          }
        }
      }
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
        for (int16_t level = 0; OB_SUCC(ret) && level < index_tree_height_; level++) {
          tree_handles_[level].can_blockscan_ = true;
          if (level <= cur_level_) {
            if (OB_FAIL(tree_handles_[level].check_blockscan(border_rowkey))) {
              LOG_WARN("Fail to refresh blockscan status", K(ret), K(level), K(tree_handles_[level]));
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
  if (ObStoreRowIterator::IteratorRowLockCheck == iter_type_ && !is_row_lock_checked) {
    const int64_t read_snapshot_version = access_ctx_->store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
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

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::prefetch_data_block(
    const int64_t prefetch_idx,
    blocksstable::ObMicroIndexInfo &index_block_info,
    ObMicroBlockDataHandle &micro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prefetch_block_data(index_block_info,
                                  micro_handle,
                                  true, /* is_data */
                                  use_multi_block_prefetch_,
                                  need_submit_io_))) {
    LOG_WARN("Fail to prefetch data block data", K(ret));
  } else if (use_multi_block_prefetch_ && micro_handle.need_multi_io()) {
    bool need_split = true;
    while (OB_SUCC(ret) && need_split) {
      if (multi_io_params_.add_micro_data(index_block_info, prefetch_idx, micro_handle, need_split)) {
        if (OB_FAIL(prefetch_multi_data_block(prefetch_idx + 1))) {
          LOG_WARN("Fail to prefetch multi block", K(ret));
        }
      }
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::prefetch_multi_data_block(
    const int64_t max_prefetch_idx)
{
  int ret = OB_SUCCESS;
  if (multi_io_params_.count() > 0) {
    if (OB_FAIL(access_ctx_->micro_block_handle_mgr_.prefetch_multi_data_block(
                micro_data_infos_,
                micro_data_handles_,
                max_micro_handle_cnt_,
                max_prefetch_idx,
                multi_io_params_))) {
      LOG_WARN("Fail to prefetch multi block", K(ret));
    } else {
      multi_io_params_.reuse();
    }
  }
  return ret;
}

//////////////////////////////////////// ObIndexTreeLevelHandle //////////////////////////////////////////////
template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::ObIndexTreeLevelHandle::prefetch(
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
      ObSSTableIndexFilter *sstable_index_filter = prefetcher.iter_param_->sstable_index_filter_;
      ObSampleFilterExecutor *sample_executor = prefetcher.sstable_->is_major_sstable() ?
          static_cast<ObSampleFilterExecutor *>(prefetcher.access_ctx_->get_sample_executor()) : nullptr;
      if (OB_FAIL(parent.get_next_index_row(prefetcher.iter_param_->has_lob_column_out(),
                                            index_info,
                                            prefetcher))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next", K(ret), KPC(this));
        } else {
          is_prefetch_end_ = parent.is_prefetch_end();
          ret = OB_SUCCESS;
        }
      } else if (nullptr != sample_executor
                  && OB_FAIL(sample_executor->check_sample_block(index_info, level, parent.fetch_idx_,
                                                               prefetch_idx_ + 1, prefetcher.iter_param_->has_lob_column_out()))) {
        LOG_WARN("Failed to check if can skip perfetch micro block in sample", K(ret), K(level), K(index_info), KPC(sample_executor));
      } else if (nullptr != sstable_index_filter
                  && prefetcher.can_index_filter_skip(index_info, sample_executor)
                  && OB_FAIL(sstable_index_filter->check_range(prefetcher.iter_param_->read_info_, index_info,
                                                                *(prefetcher.access_ctx_->allocator_),
                                                                prefetcher.iter_param_->vectorized_enabled_))) {
        LOG_WARN("Fail to check if can skip prefetch", K(ret), K(index_info));
      } else if (index_info.is_filter_always_false()) {
      } else if (nullptr != prefetcher.agg_row_store_ && prefetcher.agg_row_store_->can_agg_index_info(index_info)) {
        if (OB_FAIL(prefetcher.agg_row_store_->fill_index_info(index_info))) {
          LOG_WARN("Fail to agg index info", K(ret), KPC(this));
        } else {
          LOG_DEBUG("Success to agg index info", K(ret), K(index_info), KPC(prefetcher.agg_row_store_));
        }
      } else if (OB_FAIL(prefetcher.check_row_lock(index_info, is_row_lock_checked_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to check row lock", K(ret), KPC(this));
        }
      } else {
        ObSSTableReadHandle &read_handle = prefetcher.read_handles_[index_info.range_idx() % prefetcher.max_range_prefetching_cnt_];
        if ((prefetcher.is_multi_check() || index_info.is_get())
             && index_info.is_macro_node()
             && OB_FAIL(prefetcher.check_bloom_filter(index_info, prefetcher.is_multi_check(), read_handle))) {
          LOG_WARN("Fail to check bloom filter", K(ret), K(index_info), K(prefetcher.current_read_handle()),
                    K(prefetcher.is_multi_check()));
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
        } else if (prefetcher.is_multi_check()) {
          read_handle.row_state_ = ObSSTableRowState::IN_BLOCK;
        }
      }
    }
  }
  return ret;
}

template <int32_t DATA_PREFETCH_DEPTH, int32_t INDEX_PREFETCH_DEPTH>
int ObIndexTreeMultiPassPrefetcher<DATA_PREFETCH_DEPTH, INDEX_PREFETCH_DEPTH>::ObIndexTreeLevelHandle::forward(
    ObIndexTreeMultiPassPrefetcher &prefetcher,
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
    if (OB_FAIL(index_block_read_handles_[fetch_idx].data_handle_.get_micro_block_data(nullptr, index_block_, false))) {
      LOG_WARN("Fail to get index block data", K(ret), KPC(this));
    } else if (prefetcher.is_multi_check()) {
      if (OB_FAIL(index_scanner_.open(index_info.get_macro_id(),
                                      index_block_,
                                      index_info.rows_info_,
                                      index_info.rowkey_begin_idx_,
                                      index_info.rowkey_end_idx_))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          LOG_WARN("Failed to open index scanner", K(ret), KPC(this));
        } else {
          ret = OB_SUCCESS;
        }
      }
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
      } else if (OB_FAIL(check_blockscan(prefetcher.border_rowkey_))) {
        LOG_WARN("Fail to update_blockscan", K(ret), K(prefetcher.border_rowkey_));
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

// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX STORAGE
#include "ob_cg_prefetcher.h"
#include "ob_i_cg_iterator.h"
#include "storage/access/ob_aggregated_store.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
namespace storage {

void ObCGPrefetcher::reset()
{
  ObSSTableIndexFilterFactory::destroy_sstable_index_filter(sstable_index_filter_);
  query_index_range_.reset();
  query_range_.reset();
  is_reverse_scan_ = false;
  is_project_without_filter_ = false;
  need_prewarm_ = false;
  cg_iter_type_ = -1;
  filter_bitmap_ = nullptr;
  micro_data_prewarm_idx_ = 0;
  cur_micro_data_read_idx_ = -1;
  cg_agg_cells_ = nullptr;
  ObIndexTreeMultiPassPrefetcher::reset();
}

void ObCGPrefetcher::reuse()
{
  ObSSTableIndexFilterFactory::destroy_sstable_index_filter(sstable_index_filter_);
  query_index_range_.reset();
  query_range_.reset();
  is_project_without_filter_ = false;
  need_prewarm_ = false;
  filter_bitmap_ = nullptr;
  micro_data_prewarm_idx_ = 0;
  cur_micro_data_read_idx_ = -1;
  ObIndexTreeMultiPassPrefetcher::reuse();
}

int ObCGPrefetcher::init_tree_handles(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (count > tree_handle_cap_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = access_ctx_->stmt_allocator_->alloc(sizeof(ObCSIndexTreeLevelHandle) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory", K(ret), K(count));
    } else {
      reset_tree_handles();
      tree_handles_ = new (buf) ObCSIndexTreeLevelHandle[count];
      tree_handle_cap_ = count;
    }
  }
  return ret;
}

int ObCGPrefetcher::init(
    const int cg_iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  query_range_.set_whole_range();
  cg_iter_type_ = cg_iter_type;
  if (OB_FAIL(ObIndexTreeMultiPassPrefetcher::init(
              ObStoreRowIterator::IteratorScan,
              sstable,
              iter_param,
              access_ctx,
              &query_range_))) {
    LOG_WARN("Fail to init column group prefetcher", K(ret));
  } else if (!is_prefetch_end_) {
    is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
    if (OB_FAIL(open_index_root())) {
      LOG_WARN("Fail to open index root", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    need_prewarm_ =
        (ObICGIterator::OB_CG_SCANNER == cg_iter_type_ ||
         ((ObICGIterator::OB_CG_ROW_SCANNER == cg_iter_type_ || ObICGIterator::OB_CG_GROUP_BY_SCANNER == cg_iter_type_) &&
          is_project_without_filter_)) &&
        nullptr == cg_agg_cells_ &&
        nullptr == access_ctx_->limit_param_ ;
  }
  return ret;
}

int ObCGPrefetcher::switch_context(
    const int cg_iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  query_range_.set_whole_range();
  cg_iter_type_ = cg_iter_type;
  if (OB_FAIL(ObIndexTreeMultiPassPrefetcher::switch_context(
              ObStoreRowIterator::IteratorScan,
              sstable,
              iter_param,
              access_ctx,
              &query_range_))) {
    LOG_WARN("Fail to init column group prefetcher", K(ret));
  } else if (!is_prefetch_end_) {
    is_reverse_scan_ = access_ctx.query_flag_.is_reverse_scan();
    if (OB_FAIL(open_index_root())) {
      LOG_WARN("Fail to open index root", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    need_prewarm_ =
        (ObICGIterator::OB_CG_SCANNER == cg_iter_type_ ||
         ((ObICGIterator::OB_CG_ROW_SCANNER == cg_iter_type_ || ObICGIterator::OB_CG_GROUP_BY_SCANNER == cg_iter_type_) &&
          is_project_without_filter_)) &&
        nullptr == cg_agg_cells_ &&
        nullptr == access_ctx_->limit_param_ ;
  }
  return ret;
}

int ObCGPrefetcher::open_index_root()
{
  int ret = OB_SUCCESS;
  ObMicroIndexInfo index_info;
  index_info.is_root_ = true;
  index_info.cs_row_range_.start_row_id_ = 0;
  index_info.cs_row_range_.end_row_id_ = sstable_meta_handle_.get_sstable_meta().get_end_row_id(sstable_->is_ddl_merge_empty_sstable());
  ObIndexTreeLevelHandle &tree_handle = tree_handles_[0];
  if (OB_FAIL(sstable_->get_index_tree_root(index_block_))) {
    LOG_WARN("Fail to get index block root", K(ret));
  } else if (OB_FAIL(tree_handle.index_scanner_.open(
              ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
              index_block_,
              query_range_,
              0,
              true,
              true,
              &index_info))) {
    if (OB_BEYOND_THE_RANGE != ret) {
      LOG_WARN("Fail to open index scanner", K(ret), K(query_range_));
    } else {
      // empty ddl_merge_sstable with empty ddl_kvs may return OB_BEYOND_THE_RANGE
      ret = OB_SUCCESS;
      is_prefetch_end_ = true;
    }
  } else {
    tree_handle.fetch_idx_ = tree_handle.prefetch_idx_ = 0;
    tree_handle.is_prefetch_end_ = true;
  }
  return ret;
}


void ObCGPrefetcher::update_query_range(const ObCSRange &range)
{
  if (is_reverse_scan_) {
    query_range_.start_key_.set_min_rowkey();
    datums_[0].reuse();
    datums_[0].set_uint(range.end_row_id_);
    query_range_.end_key_.assign(datums_, 1);
    query_range_.set_left_open();
    query_range_.set_right_closed();
  } else {
    datums_[0].reuse();
    datums_[0].set_uint(range.start_row_id_);
    query_range_.start_key_.assign(datums_, 1);
    query_range_.end_key_.set_max_rowkey();
    query_range_.set_left_closed();
    query_range_.set_right_open();
  }
  query_index_range_ = range;
}

void ObCGPrefetcher::update_leaf_query_range(const ObCSRowId leaf_start_row_id)
{
  OB_ASSERT(query_index_range_.is_valid());
  if (is_reverse_scan_) {
    leaf_query_range_.start_key_.set_min_rowkey();
    datums_[1].reuse();
    datums_[1].set_uint(query_index_range_.end_row_id_ - leaf_start_row_id);
    leaf_query_range_.end_key_.assign(datums_ + 1, 1);
    leaf_query_range_.set_left_open();
    leaf_query_range_.set_right_closed();
  } else {
    datums_[1].reuse();
    datums_[1].set_uint(query_index_range_.start_row_id_ - leaf_start_row_id);
    leaf_query_range_.start_key_.assign(datums_ + 1, 1);
    leaf_query_range_.end_key_.set_max_rowkey();
    leaf_query_range_.set_left_closed();
    leaf_query_range_.set_right_open();
  }
}

int ObCGPrefetcher::locate_in_prefetched_data(bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  const ObCSRowId start_row_idx = is_reverse_scan_ ? query_index_range_.end_row_id_ : query_index_range_.start_row_id_;
  int64_t max_data_prefetched_idx = MAX(micro_data_prewarm_idx_, micro_data_prefetch_idx_);
  if (max_data_prefetched_idx > 0 && max_data_prefetched_idx > cur_micro_data_read_idx_) {
    int cmp_ret = -1;
    for (int64_t micro_data_idx = MAX(0, cur_micro_data_read_idx_); OB_SUCC(ret) && cmp_ret < 0 && micro_data_idx < max_data_prefetched_idx; micro_data_idx++) {
      ObMicroIndexInfo &micro_info = micro_data_infos_[micro_data_idx % max_micro_handle_cnt_];
      const ObCSRange &micro_range = micro_info.get_row_range();
      cmp_ret = micro_range.compare(start_row_idx);
      if (is_reverse_scan_) {
        cmp_ret = -cmp_ret;
      }
      if (0 == cmp_ret) {
        cur_micro_data_fetch_idx_ = micro_data_idx - 1;
        cur_micro_data_read_idx_ = cur_micro_data_fetch_idx_;
        if (is_reverse_scan_) {
          micro_info.is_right_border_ = true;
        } else {
          micro_info.is_left_border_ = true;
        }
        found = true;
      }
    }

    if (OB_SUCC(ret) && found) {
      cmp_ret = 1;
      is_prefetch_end_ = false;
      micro_data_prefetch_idx_ = max_data_prefetched_idx;
      const ObCSRowId end_row_id = is_reverse_scan_ ? query_index_range_.start_row_id_ : query_index_range_.end_row_id_;
      for (int64_t micro_data_idx = max_data_prefetched_idx - 1; OB_SUCC(ret) && cmp_ret > 0 && micro_data_idx > cur_micro_data_fetch_idx_; micro_data_idx--) {
        const ObCSRange &micro_range = micro_data_infos_[micro_data_idx % max_micro_handle_cnt_].get_row_range();
        cmp_ret = micro_range.compare(end_row_id);
        if (is_reverse_scan_) {
          cmp_ret = -cmp_ret;
        }
        if (0 == cmp_ret) {
          is_prefetch_end_ = true;
          micro_data_prefetch_idx_ = micro_data_idx + 1;
        }
      }
    }
  }
  return ret;
}

int ObCGPrefetcher::refresh_index_tree()
{
  int ret = OB_SUCCESS;
  bool found_in_tree = false;
  const ObCSRowId row_idx = is_reverse_scan_ ? query_index_range_.end_row_id_ : query_index_range_.start_row_id_;
  for (int64_t level = cur_level_; OB_SUCC(ret) && level >= 0; level--) {
    if (OB_FAIL(static_cast<ObCSIndexTreeLevelHandle *>(tree_handles_ + level)->locate_row_index(
                *this, (0 == level), row_idx, found_in_tree))) {
      LOG_WARN("Fail to locate row index", K(ret), K(tree_handles_[level]));
    } else if (found_in_tree) {
      cur_level_ = level;
      break;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!found_in_tree) {
    // not found
    reuse();
    is_prefetch_end_ = true;
  } else {
    is_prefetch_end_ = false;
    cur_micro_data_fetch_idx_ = -1;
    cur_micro_data_read_idx_ = -1;
    micro_data_prefetch_idx_ = 0;
    micro_data_prewarm_idx_ = 0;
    total_micro_data_cnt_ = 0;
    for (int64_t level = 1; level < index_tree_height_; level++) {
      if (level <= cur_level_) {
        tree_handles_[level].is_prefetch_end_ =
            compare_range(tree_handles_[level].last_prefetched_index().get_row_range()) >= 0;
      } else {
        tree_handles_[level].reuse();
      }
    }
  }
  return ret;
}

bool ObCGPrefetcher::locate_back(const ObCSRange &locate_range)
{
  bool is_locate_back = false;
  if (0 < query_index_range_.end_row_id_) {
    int micro_index = MAX(0, cur_micro_data_read_idx_);
    if (is_reverse_scan_) {
      is_locate_back = locate_range.end_row_id_ > micro_data_infos_[micro_index % max_micro_handle_cnt_].get_row_range().end_row_id_;
    } else {
      is_locate_back = locate_range.start_row_id_ < micro_data_infos_[micro_index % max_micro_handle_cnt_].get_row_range().start_row_id_;
    }
  }
  return is_locate_back;
}

int ObCGPrefetcher::locate(const ObCSRange &range, const ObCGBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  is_prefetch_end_ = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCGPrefetcher not init", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid() || 0 > cur_level_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid locate argument", K(ret), K(range), K_(cur_level));
  } else if (FALSE_IT(filter_bitmap_ = bitmap)) {
  } else if (nullptr != bitmap && bitmap->is_all_false()) {
    is_prefetch_end_ = true;
  } else if (locate_back(range)) {
    ObIndexTreeMultiPassPrefetcher::reuse();
    micro_data_prewarm_idx_ = 0;
    cur_micro_data_read_idx_ = -1;
    update_query_range(range);
    read_handles_[0].row_state_ = ObSSTableRowState::IN_BLOCK;
    if (OB_FAIL(open_index_root())) {
      LOG_WARN("Fail to open index root", K(ret));
    }
  } else {
    bool found_in_data = false;
    update_query_range(range);
    read_handles_[0].row_state_ = ObSSTableRowState::IN_BLOCK;
    if (OB_FAIL(locate_in_prefetched_data(found_in_data))) {
      LOG_WARN("Fail to locate in prefetched data", K(ret), K_(query_index_range));
    } else if (found_in_data) {
      if (!is_prefetch_end_) {
        for (int64_t level = 1; level < index_tree_height_; level++) {
          tree_handles_[level].is_prefetch_end_ = level <= cur_level_ ?
              compare_range(tree_handles_[level].last_prefetched_index().get_row_range()) >= 0 : false;
        }
      }
    } else if (OB_FAIL(refresh_index_tree())) {
      LOG_WARN("Fail to refresh index tree", K(ret), K_(query_index_range));
    }
  }
  return ret;
}

// prefetch index block tree from up to down, cur_level_ is the level of index tree last read
int ObCGPrefetcher::prefetch_index_tree()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_tree_height_ <= cur_level_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected tree level", K(ret), K(cur_level_), K(index_tree_height_));
  } else {
    int16_t border_level = MIN(cur_level_ + 2, index_tree_height_);
    for (int16_t level = 1; OB_SUCC(ret) && level < border_level; level++) {
      if (tree_handles_[level].is_prefetch_end()) {
      } else if (OB_FAIL(static_cast<ObCSIndexTreeLevelHandle *>(tree_handles_ + level)->prefetch(level, *this))) {
        LOG_WARN("Fail to prefetch index tree", K(ret), K(level),
                 K(tree_handles_[level - 1]), K(tree_handles_[level]));
      }
    }
  }
  return ret;
}

int ObCGPrefetcher::prefetch_micro_data()
{
  int ret = OB_SUCCESS;
  int64_t prefetched_cnt = 0;
  int64_t prefetch_micro_idx = 0;
  int64_t prefetch_depth = 0;
  if (OB_UNLIKELY(index_tree_height_ <= cur_level_ ||
                  micro_data_prefetch_idx_ - cur_micro_data_read_idx_ > max_micro_handle_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected prefetch status", K(ret), K_(cur_level), K_(index_tree_height),
             K_(micro_data_prefetch_idx), K_(cur_micro_data_read_idx), K_(max_micro_handle_cnt));
  } else if (micro_data_prefetch_idx_ - cur_micro_data_read_idx_ == max_micro_handle_cnt_ ||
             (use_multi_block_prefetch_ && prefetch_depth_ > MIN_DATA_READ_BATCH_COUNT &&
              (max_micro_handle_cnt_ - (micro_data_prefetch_idx_ - cur_micro_data_fetch_idx_)) < MIN_DATA_READ_BATCH_COUNT)) {
    // DataBlock ring buf full
  } else if (OB_FAIL(get_prefetch_depth(prefetch_depth, micro_data_prefetch_idx_))) {
    LOG_WARN("Fail to get prefetch depth", K(ret));
  } else {
    while (OB_SUCC(ret) && !is_prefetch_end_ && prefetched_cnt < prefetch_depth) {
      if (OB_FAIL(drill_down())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get index leaf", K(ret), K(cur_level_));
        } // else prefetch_end
      } else if (index_tree_height_ - 1 != cur_level_) {
        // not leaf level, prefetch index tree
        break;
      } else {
        // read index leaf and prefetch micro data
        while (OB_SUCC(ret) && !is_prefetch_end_ && prefetched_cnt < prefetch_depth) {
          prefetch_micro_idx = micro_data_prefetch_idx_ % max_micro_handle_cnt_;
          ObMicroIndexInfo &block_info = micro_data_infos_[prefetch_micro_idx];
          bool can_agg = false;
          if (access_ctx_->micro_block_handle_mgr_.reach_hold_limit()
              && micro_data_prefetch_idx_ > cur_micro_data_fetch_idx_ + 1) {
            LOG_DEBUG("micro block handle mgr has reach hold limit, stop prefetch", K(prefetch_depth),
                K(prefetched_cnt), K(micro_data_prefetch_idx_), K(cur_micro_data_fetch_idx_),
                K(access_ctx_->micro_block_handle_mgr_));
            prefetch_depth = prefetched_cnt;
            break;
          } else if (OB_FAIL(tree_handles_[cur_level_].get_next_data_row(false, block_info))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get next", K(ret), K(cur_level_), K(tree_handles_[cur_level_]));
            } else {
              // open next leaf by get_next_index_leaf in the next loop
              ret = OB_SUCCESS;
              break;
            }
          } else if (compare_range(block_info.get_row_range()) > 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected prefetch index info", K(ret), K(block_info), KPC(this));
          } else if (!contain_rows(block_info.get_row_range())) {
            // continue if no rows contained
          } else if (nullptr != sstable_index_filter_
                     && can_index_filter_skip(block_info)
                     && OB_FAIL(sstable_index_filter_->check_range(
                             iter_param_->read_info_, block_info,
                             *(access_ctx_->allocator_), iter_param_->vectorized_enabled_))) {
            LOG_WARN("Fail to check if can skip prefetch", K(ret), K(block_info));
          } else if (nullptr != sstable_index_filter_
                     && (block_info.is_filter_always_false() || block_info.is_filter_always_true())) {
            // TODO: skip data block which is always_false/always_true and record the result in filter bitmap
            prefetched_cnt++;
            micro_data_prefetch_idx_++;
            tree_handles_[cur_level_].current_block_read_handle().end_prefetched_row_idx_++;
          } else if (OB_FAIL(can_agg_micro_index(block_info, can_agg))) {
            LOG_WARN("fail to check can agg index info", K(ret), K(block_info), KPC(cg_agg_cells_));
          } else if (can_agg) {
            if (OB_FAIL(cg_agg_cells_->process(block_info))) {
              LOG_WARN("Fail to agg index info", K(ret));
            } else {
              LOG_DEBUG("[COLUMNSTORE] success to agg index info", K(ret), K(block_info));
            }
          } else if (OB_FAIL(prefetch_data_block(
                      micro_data_prefetch_idx_,
                      block_info,
                      micro_data_handles_[prefetch_micro_idx]))) {
            LOG_WARN("fail to prefetch_block_data", K(ret), K(block_info));
          } else {
            prefetched_cnt++;
            micro_data_prefetch_idx_++;
            tree_handles_[cur_level_].current_block_read_handle().end_prefetched_row_idx_++;
          }

          if (OB_SUCC(ret) && (0 == compare_range(block_info.get_row_range()))) {
            is_prefetch_end_ = true;
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

int ObCGPrefetcher::compare_range(const ObCSRange &index_range)
{
  int cmp_ret = 0;
  if (is_reverse_scan_) {
    cmp_ret = -index_range.compare(query_index_range_.start_row_id_);
  } else {
    cmp_ret = index_range.compare(query_index_range_.end_row_id_);
  }
  LOG_DEBUG("[INDEX BLOCK] compare range", K(cmp_ret), K(is_reverse_scan_), K(index_range), K(query_index_range_));
  return cmp_ret;
}

bool ObCGPrefetcher::contain_rows(const ObCSRange &index_range)
{
  return (nullptr == filter_bitmap_ || !filter_bitmap_->is_all_false(index_range));
}

int ObCGPrefetcher::can_agg_micro_index(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg)
{
  int ret = OB_SUCCESS;
  const ObCSRange &index_range = index_info.get_row_range();
  can_agg = nullptr != cg_agg_cells_&&
                 index_range.start_row_id_ >= query_index_range_.start_row_id_ &&
                 index_range.end_row_id_ <= query_index_range_.end_row_id_ &&
                 (nullptr == filter_bitmap_ || filter_bitmap_->is_all_true(index_range));
  if (can_agg && OB_FAIL(cg_agg_cells_->can_use_index_info(index_info, can_agg))) {
    LOG_WARN("fail to check index info", K(ret),
                      K(index_info), KPC(cg_agg_cells_));
  }
  return ret;
}

int ObCGPrefetcher::ObCSIndexTreeLevelHandle::prefetch(
    const int64_t level,
    ObCGPrefetcher &prefetcher)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= level || level >= prefetcher.index_tree_height_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(level), K(prefetcher.index_tree_height_));
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
      bool can_agg = false;
      if (OB_FAIL(parent.get_next_index_row(prefetcher.iter_param_->has_lob_column_out(),
                                            index_info,
                                            prefetcher))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next", K(ret), KPC(this));
        } else {
          is_prefetch_end_ = parent.is_prefetch_end();
          ret = OB_SUCCESS;
        }
      } else if (prefetcher.compare_range(index_info.get_row_range()) > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected prefetch index info", K(ret), K(index_info), K(prefetcher));
      } else if (!prefetcher.contain_rows(index_info.get_row_range())) {
        // continue if no rows contained
      } else if (nullptr != prefetcher.sstable_index_filter_
                 && prefetcher.can_index_filter_skip(index_info)
                 && OB_FAIL(prefetcher.sstable_index_filter_->check_range(
                         prefetcher.iter_param_->read_info_,
                         index_info,
                         *(prefetcher.access_ctx_->allocator_),
                         prefetcher.iter_param_->vectorized_enabled_))) {
        LOG_WARN("Fail to check if can skip prefetch", K(ret), K(index_info));
        // TODO: skip data block which is always_false/always_true and record the result in filter bitmap
      } else if (OB_FAIL(prefetcher.can_agg_micro_index(index_info, can_agg))) {
        LOG_WARN("fail to check index info", K(ret), K(index_info), KPC(prefetcher.cg_agg_cells_));
      } else if (can_agg) {
        if (OB_FAIL(prefetcher.cg_agg_cells_->process(index_info))) {
          LOG_WARN("Fail to agg index info", K(ret));
        } else {
          LOG_DEBUG("[COLUMNSTORE] success to agg index info", K(ret), K(index_info));
        }
      } else {
        ObSSTableReadHandle &read_handle = prefetcher.read_handles_[index_info.range_idx() % prefetcher.max_range_prefetching_cnt_];
        if (level == prefetcher.index_tree_height_ -1 && !index_info.is_leaf_block()) {
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
      if (OB_SUCC(ret) && !is_prefetch_end_ && 0 == prefetcher.compare_range(index_info.get_row_range())) {
        is_prefetch_end_ = true;
      }
     }
  }
  return ret;
}

int ObCGPrefetcher::ObCSIndexTreeLevelHandle::forward(
    ObIndexTreeMultiPassPrefetcher &prefetcher,
    const bool has_lob_out)
{
  int ret = OB_SUCCESS;
  UNUSED(has_lob_out);
  if (fetch_idx_ >= prefetch_idx_) {
    ret = OB_ITER_END;
  } else {
    fetch_idx_++;
    index_scanner_.reuse();
    int8_t fetch_idx = fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH;
    ObMicroIndexInfo &index_info = index_block_read_handles_[fetch_idx].index_info_;
    ObCGPrefetcher &cg_prefetcher = static_cast<ObCGPrefetcher &>(prefetcher);
    ObDatumRange *query_range = &cg_prefetcher.query_range_;
    if (((cg_prefetcher.is_reverse_scan_ && index_info.is_right_border()) ||
         (!cg_prefetcher.is_reverse_scan_ && index_info.is_left_border())) &&
        index_info.is_leaf_block()) {
      cg_prefetcher.update_leaf_query_range(index_info.get_row_range().start_row_id_);
      query_range = &cg_prefetcher.leaf_query_range_;
    } // else border is false, locate [0, count -1] for the block in the middle
    if (OB_FAIL(index_block_read_handles_[fetch_idx].data_handle_.get_micro_block_data(nullptr, index_block_, false))) {
      LOG_WARN("Fail to get index block data", K(ret), K(index_block_), KPC(this));
    } else if (OB_FAIL(index_scanner_.open(
                index_info.get_macro_id(),
                index_block_,
                *query_range,
                index_info.range_idx(),
                index_info.is_left_border(),
                index_info.is_right_border(),
                &index_info))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to open index scanner", K(ret), KPC(this));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret) && 0 < fetch_idx_) {
      index_block_read_handles_[fetch_idx].end_prefetched_row_idx_ =
          index_block_read_handles_[(fetch_idx_ - 1) % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_;
    }
  }
  return ret;
}

int ObCGPrefetcher::ObCSIndexTreeLevelHandle::locate_row_index(
    ObCGPrefetcher &prefetcher,
    const bool is_root,
    const ObCSRowId row_idx,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (is_root) {
    if (OB_FAIL(index_scanner_.locate_range(prefetcher.query_range_, true, true))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to locate root range", K(ret), K(prefetcher.query_range_));
      }  else {
        LOG_DEBUG("Row index range beyond the sstable", K(prefetcher.query_index_range_), KPC(prefetcher.sstable_));
        ret = OB_SUCCESS;
      }
    } else {
      found = true;
    }
  } else {
    for (int32_t block_idx = read_idx_; block_idx <= prefetch_idx_; block_idx++) {
      ObIndexBlockReadHandle &index_read_handle = index_block_read_handles_[block_idx % INDEX_TREE_PREFETCH_DEPTH];
      index_read_handle.reuse();
      if (0 == index_read_handle.index_info_.get_row_range().compare(row_idx)) {
        read_idx_ = fetch_idx_ = block_idx;
        found = true;
        break;
      }
    }

    if (found) {
      index_scanner_.reuse();
      int8_t fetch_idx = fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH;
      ObMicroIndexInfo &index_info = index_block_read_handles_[fetch_idx].index_info_;
      ObDatumRange *query_range = &prefetcher.query_range_;
      if (index_info.is_leaf_block()) {
        prefetcher.update_leaf_query_range(index_info.get_row_range().start_row_id_);
        query_range = &prefetcher.leaf_query_range_;
      }
      if (OB_FAIL(index_block_read_handles_[fetch_idx].data_handle_.get_micro_block_data(nullptr, index_block_, false))) {
        LOG_WARN("Fail to get index block data", K(ret), KPC(this));
      } else if (OB_FAIL(index_scanner_.open(
                  index_info.get_macro_id(),
                  index_block_,
                  *query_range,
                  index_info.range_idx(),
                  true,
                  true,
                  &index_info))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          LOG_WARN("Fail to open index scanner", K(ret), KPC(this));
        } else {
          found = false;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

void ObCGPrefetcher::recycle_block_data()
{
  OB_ASSERT(cur_micro_data_read_idx_ <= cur_micro_data_fetch_idx_);
  for (int64_t i = MAX(cur_micro_data_read_idx_, 0); i < cur_micro_data_fetch_idx_; i++) {
    micro_data_handles_[i % max_micro_handle_cnt_].reset();
  }
  cur_micro_data_read_idx_ = cur_micro_data_fetch_idx_;
}

int ObCGPrefetcher::get_prefetch_depth(int64_t &depth, const int64_t prefetching_idx)
{
  int ret = OB_SUCCESS;
  depth = 0;
  prefetch_depth_ = MIN(2 * prefetch_depth_, DEFAULT_SCAN_MICRO_DATA_HANDLE_CNT);
  depth = min(static_cast<int64_t>(prefetch_depth_),
              max_micro_handle_cnt_ - (prefetching_idx - cur_micro_data_read_idx_));
  return ret;
}

int ObCGPrefetcher::prefetch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndexTreeMultiPassPrefetcher::prefetch())) {
    LOG_WARN("Fail to prefetch", K(ret));
  } else if (OB_FAIL(prewarm())) {
    LOG_WARN("Fail to prewarm", K(ret));
  }
  return ret;
}

int ObCGPrefetcher::prewarm()
{
  int ret = OB_SUCCESS;
  const int32_t prefetch_limit = MAX(2, max_micro_handle_cnt_ / 2);
  if (micro_data_prewarm_idx_ < micro_data_prefetch_idx_) {
    micro_data_prewarm_idx_ = micro_data_prefetch_idx_;
  }
  if (is_prefetch_end_ && need_submit_io_ && need_prewarm_ &&
      (index_tree_height_ - 1) == cur_level_ &&
      micro_data_prewarm_idx_ - cur_micro_data_fetch_idx_ < prefetch_limit) {

    int64_t prefetched_cnt = 0;
    int64_t prefetch_micro_idx = 0;
    int64_t prefetch_depth = 0;
    if (OB_FAIL(get_prefetch_depth(prefetch_depth, micro_data_prewarm_idx_))) {
      LOG_WARN("Fail to get prefetch depth", K(ret));
    } else {
      while (OB_SUCC(ret) && prefetched_cnt < prefetch_depth) {
        prefetch_micro_idx = micro_data_prewarm_idx_ % max_micro_handle_cnt_;
        ObMicroIndexInfo &block_info = micro_data_infos_[prefetch_micro_idx];
        if (OB_FAIL(tree_handles_[cur_level_].get_next_data_row(false, block_info))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next", K(ret), K_(cur_level), K(tree_handles_[cur_level_]));
          } else {
            // open next leaf by get_next_index_leaf in the next loop
            ret = OB_SUCCESS;
            break;
          }
        } else if (nullptr != sstable_index_filter_
                   && block_info.has_agg_data()
                   && block_info.is_filter_uncertain()
                   && OB_FAIL(sstable_index_filter_->check_range(
                           iter_param_->read_info_, block_info,
                           *(access_ctx_->allocator_), iter_param_->vectorized_enabled_))) {
          LOG_WARN("Fail to check if can skip prefetch", K(ret), K(block_info));
        } else if (nullptr != sstable_index_filter_
                   && (block_info.is_filter_always_false() || block_info.is_filter_always_true())) {
          // TODO: skip data block which is always_false/always_true and record the result in filter bitmap
          prefetched_cnt++;
          micro_data_prewarm_idx_++;
          tree_handles_[cur_level_].current_block_read_handle().end_prefetched_row_idx_++;
        } else if (OB_FAIL(prefetch_data_block(
                    micro_data_prewarm_idx_,
                    block_info,
                    micro_data_handles_[prefetch_micro_idx]))) {
          LOG_WARN("fail to prefetch_block_data", K(ret), K(block_info));
        } else {
          prefetched_cnt++;
          micro_data_prewarm_idx_++;
          tree_handles_[cur_level_].current_block_read_handle().end_prefetched_row_idx_++;
        }
      }
      if (OB_SUCC(ret) && multi_io_params_.count() > 0 &&
          OB_FAIL(prefetch_multi_data_block(micro_data_prewarm_idx_))) {
        LOG_WARN("Fail to prefetch multi block", K(ret), K_(micro_data_prewarm_idx), K_(multi_io_params));
      }
    }

    if (OB_SUCC(ret) && 0 < prefetched_cnt) {
      LOG_DEBUG("[INDEX BLOCK] prewarm prefetched info", K(ret), K_(cur_micro_data_fetch_idx), K_(micro_data_prefetch_idx),
                K_(micro_data_prewarm_idx), K_(total_micro_data_cnt), K(prefetched_cnt), K(prefetch_depth));
      total_micro_data_cnt_ += prefetched_cnt;
    }
  }
  return ret;
}

void ObCGIndexPrefetcher::reset()
{
  ObCGPrefetcher::reset();
  use_multi_block_prefetch_ = false;
  need_submit_io_ = false;
}

void ObCGIndexPrefetcher::reuse()
{
  ObCGPrefetcher::reuse();
  use_multi_block_prefetch_ = false;
  need_submit_io_ = false;
}

int ObCGIndexPrefetcher::init(
    const int cg_iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCGPrefetcher::init(cg_iter_type, sstable, iter_param, access_ctx))) {
    LOG_WARN("Fail to init cg prefetcher", K(ret));
  } else {
    use_multi_block_prefetch_ = false;
    need_submit_io_ = false;
  }
  return ret;
}

int ObCGIndexPrefetcher::switch_context(
    const int cg_iter_type,
    ObSSTable &sstable,
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCGPrefetcher::switch_context(cg_iter_type, sstable, iter_param, access_ctx))) {
    LOG_WARN("Fail to switch context for cg prefetcher", K(ret));
  } else {
    use_multi_block_prefetch_ = false;
    need_submit_io_ = false;
  }
  return ret;
}

int ObCGIndexPrefetcher::prefetch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndexTreeMultiPassPrefetcher::prefetch())) {
    LOG_WARN("Fail to prefetch", K(ret));
  }
  return ret;
}

}
}

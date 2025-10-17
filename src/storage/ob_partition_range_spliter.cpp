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

#include "ob_partition_range_spliter.h"
#include "compaction/ob_tablet_merge_ctx.h"
#include "access/ob_multiple_scan_merge.h"
#include "blocksstable/ob_micro_block_row_scanner.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace blocksstable;
using namespace compaction;
namespace storage
{

int ObIndexBlockTreeTraverser::init(ObSSTable &sstable, const ObITableReadInfo &index_read_info)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice for index block tree traverser", KR(ret));
  } else if (OB_UNLIKELY(!sstable.is_valid() || !index_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for index block tree traverser",
             KR(ret),
             K(sstable),
             K(index_read_info));
  } else {
    sstable_ = &sstable;
    read_info_ = &index_read_info;
    context_ = nullptr;
    visited_node_count_ = 0;
    visited_macro_node_count_ = 0;
    avg_range_visited_node_cnt_ = 0;
    remain_can_visited_node_cnt_ = 0;
    is_inited_ = true;
  }

  return ret;
}

void ObIndexBlockTreeTraverser::reuse()
{
  allocator_.reuse();
  visited_node_count_ = 0;
  visited_macro_node_count_ = 0;
  avg_range_visited_node_cnt_ = 0;
  remain_can_visited_node_cnt_ = 0;
}

int ObIndexBlockTreeTraverser::handle_overflow_ranges()
{
  int ret = OB_SUCCESS;

  // overflow ranges are out of index block tree, just call next range
  while (OB_SUCC(ret) && !context_->is_ended()) {
    if (OB_FAIL(context_->next_range())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Fail to go to next range", KR(ret), KPC(context_));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::traverse(ObIMultiRangeEstimateContext &context,
                                        const int64_t open_index_micro_block_limit)
{
  int ret = OB_SUCCESS;

  ObMicroBlockData root_index_block;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init traverser", KR(ret));
  } else if (OB_UNLIKELY(!context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid context", KR(ret), K(context));
  } else if (FALSE_IT(context_ = &context)) {
  } else if (FALSE_IT(avg_range_visited_node_cnt_ = 1.0 * open_index_micro_block_limit / context.get_ranges_count())) {
  } else if (FALSE_IT(remain_can_visited_node_cnt_ = 0)) {
  } else if (sstable_->is_empty()) {
    // skip
  } else if (OB_FAIL(sstable_->get_index_tree_root(root_index_block))) {
    LOG_WARN("Fail to get index tree root", KR(ret), KPC(context_));
  } else if (OB_FAIL(path_caches_.load_root(root_index_block, *this))) {
    LOG_WARN("Fail to load root node to path caches", KR(ret));
  } else if (OB_FAIL(inner_node_traverse(nullptr, PathInfo(), 0, avg_range_visited_node_cnt_))) {
    LOG_WARN("Fail to traverse index block tree", KR(ret), K(root_index_block), KPC(context_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(handle_overflow_ranges())) {
    LOG_WARN("Fail to handle overflow ranges", KR(ret), KPC(context_));
  }

  LOG_TRACE("Traverse index block tree, stats:", K(visited_node_count_), K(visited_macro_node_count_));

  return ret;
}

int ObIndexBlockTreeTraverser::is_range_cover_node_end_key(const ObDatumRange &range,
                                                           const ObMicroIndexInfo &micro_index_info,
                                                           bool &is_over)
{
  int ret = OB_SUCCESS;

  int endkey_cmp_ret = 0;

  // Don't compare multi version column
  // cmp_ret < 0         cmp_ret == 0 && range.is_right_closed()
  // node:  (.., 50]     (..., 100]
  // range: (0, 100)     (0  , 100]
  if (OB_FAIL(micro_index_info.endkey_.compare(range.get_end_key(),
                                               read_info_->get_datum_utils(),
                                               endkey_cmp_ret,
                                               /* compare_datum_cnt */ false))) {
    LOG_WARN("Fail to compare endkey", KR(ret), K(range), KPC(read_info_));
  } else {
    is_over = endkey_cmp_ret < 0 || (endkey_cmp_ret == 0 && range.is_right_closed());
  }

  return ret;
}

int ObIndexBlockTreeTraverser::goto_next_level_node(const ObMicroIndexInfo &micro_index_info,
                                                    const PathInfo &path_info,
                                                    const int64_t level,
                                                    double open_index_micro_block_limit)
{
  int ret = OB_SUCCESS;

  bool borrow_from_parent = false;
  bool should_estimate = false;
  bool is_in_cache = false;

  // in SS mode, get one index micro block maybe need 10ms, which is too slow
  // so we won't open the leaf node in index-block-tree, unless there is only one macro_block (in range)
  // TODO(menglan): must need prefetch for perf
  if (GCTX.is_shared_storage_mode() && micro_index_info.is_leaf_block() && path_info.path_count_ > 1) {
    should_estimate = true;
  } else if (micro_index_info.is_data_block() && micro_index_info.get_row_count() <= OPEN_DATA_MICRO_BLOCK_ROW_LIMIT) {
    should_estimate = true;
  } else {
    if (OB_FAIL(path_caches_.find(level, &micro_index_info, is_in_cache))) {
      if (ret != OB_NOT_SUPPORTED) {
        LOG_WARN("Fail to find node in path caches", KR(ret), K(micro_index_info));
      } else {
        ret = OB_SUCCESS;
        should_estimate = true;
      }
    } else if (!is_in_cache) {
      should_estimate = (remain_can_visited_node_cnt_ < 1);
      if (should_estimate && remain_can_visited_node_cnt_ + open_index_micro_block_limit >= 1) {
        borrow_from_parent = true;
        should_estimate = false;
        open_index_micro_block_limit = remain_can_visited_node_cnt_ + open_index_micro_block_limit - 1;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (should_estimate) {
    if (OB_FAIL(context_->on_node_estimate(micro_index_info, path_info.in_middle_))) {
      LOG_WARN("Fail to do context on leaf node estimate", KR(ret), K(micro_index_info));
    } else {
      remain_can_visited_node_cnt_ += open_index_micro_block_limit;
    }
  } else {
    if (!is_in_cache) {
      borrow_from_parent ? remain_can_visited_node_cnt_ = 0 : remain_can_visited_node_cnt_--;
    }

    if (micro_index_info.is_leaf_block()) {
      visited_macro_node_count_++;
    }
    visited_node_count_++;

    if (OB_FAIL(micro_index_info.is_data_block()
                    ? leaf_node_traverse(micro_index_info, path_info, level)
                    : inner_node_traverse(&micro_index_info, path_info, level, open_index_micro_block_limit))) {
      LOG_WARN("Fail to traverse next level node", KR(ret));
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::leaf_node_traverse(const ObMicroIndexInfo &micro_index_info,
                                                  const PathInfo &path_info,
                                                  const int64_t level)
{
  int ret = OB_SUCCESS;

  PathNodeCaches::CacheNode *cache_node = nullptr;
  bool is_in_cache = false;
  ObMicroBlockRowScanner scanner(allocator_);
  const ObDatumRange &range = context_->get_curr_range();
  ObPartitionEst est;

  if (OB_FAIL(path_caches_.get(level, &micro_index_info, *this, cache_node, is_in_cache))) {
    LOG_WARN("Fail to get node in path caches", KR(ret), K(micro_index_info));
  } else if (OB_FAIL(scanner.estimate_row_count(*read_info_, cache_node->micro_data_, range, false, est))) {
    LOG_WARN("Fail to estimate row count", KR(ret), K(cache_node->micro_data_), KPC(context_));
  } else if (OB_FAIL(context_->on_leaf_node(micro_index_info, est))) {
    LOG_WARN("Fail to do context on leaf node", KR(ret), K(micro_index_info), KPC(context_));
  }

  return ret;
}

int ObIndexBlockTreeTraverser::inner_node_traverse(const ObMicroIndexInfo *micro_index_info,
                                                   const PathInfo &path_info,
                                                   const int64_t level,
                                                   const double open_index_micro_block_limit)
{
  // open_index_micro_block_limit is the number of index micro blocks that can be opened in current subtree

  int ret = OB_SUCCESS;

  PathNodeCaches::CacheNode *cache_node = nullptr;
  TreeNodeContext *node = nullptr;
  ObMicroIndexInfo index_row;
  bool is_in_cache = false;

  if (OB_FAIL(path_caches_.get(level, micro_index_info, *this, cache_node, is_in_cache))) {
    LOG_WARN("Fail to get node in path caches", KR(ret), KPC(micro_index_info));
  } else {
    node = &cache_node->context_;
  }

  while (OB_SUCC(ret) && !context_->is_ended()) {
    const ObDatumRange &range = context_->get_curr_range();
    const int64_t curr_range_idx = context_->get_curr_range_idx();
    int64_t index_row_count = 0;

    // step 1. locate range in multi index row
    if (OB_FAIL(node->locate_range(range, index_row_count))) {
      if (ret != OB_BEYOND_THE_RANGE) {
        LOG_WARN("Fail to locate range", KR(ret));
      } else {
        // beyond the range, the remain ranges are all larger than node
        ret = OB_SUCCESS;
        break;
      }
    }

    // The path_info indicates which child of the parent node the current node is in (within the
    // interval being traverse). It could be the leftmost child, a middle child, or the rightmost
    // child. See the figure below.
    //                              start_key end_key
    //  C: coverd                       │       │
    //  N: partial coverage             ▼       ▼
    //                                  ─────────
    //                                 ┌─┬─┬─┬─┬─┐
    //                                 │N│C│C│C│N│
    //                                 │ │ │ │ │ │
    //                                 └┬┴─┴┬┴─┴┬┘
    //             ┌────────────────────┘   │   └──────────────────┐
    // left_most◄──┤                        ├──►in_middle          ├──►right_most
    //          ┌─┬┴┬─┐                  ┌─┬┴┬─┐                ┌─┬┴┬─┐
    //          │ │N│C│                  │C│C│C│                │C│C│N│
    //          │ │ │ ├──┐               │ │ │ │                │ │ │ │
    //          └─┴─┴─┘  │               └─┴─┴─┘                └─┴─┴─┘
    //             ───   │                                    i: 0 1 2
    //                   ▼
    //                still_estimating

    // step 2. iterate the index row to sum row count
    for (int64_t i = 0; OB_SUCC(ret) && i < index_row_count; i++) {
      if (OB_FAIL(node->get_next_index_row(index_row))) {
        LOG_WARN("Fail to get next index row", KR(ret), K(i), K(index_row_count));
      }

      // we can combine path_info information and compare endkey to check whether current range is
      // coverd the index row.
      bool is_coverd_by_range = false;
      PathInfo next_level_node_path_info(index_row_count, i, path_info);
      if (OB_FAIL(ret)) {
      } else if (next_level_node_path_info.in_middle_) {
        is_coverd_by_range = true;
      } else if (next_level_node_path_info.left_most_ && !next_level_node_path_info.right_most_) {
        is_coverd_by_range = range.get_start_key().is_min_rowkey();
      } else if (next_level_node_path_info.right_most_ && !next_level_node_path_info.left_most_) {
        if (range.get_end_key().is_max_rowkey()) {
          is_coverd_by_range = true;
        } else if (OB_FAIL(is_range_cover_node_end_key(range, index_row, is_coverd_by_range))) {
          LOG_WARN("Fail to compare range endkey and index row", KR(ret));
        }
      } else if (next_level_node_path_info.right_most_ && next_level_node_path_info.left_most_) {
        // locate range only hit node
        // is_coverd_by_range = false;
      }

      ObIMultiRangeEstimateContext::ObTraverserOperationType operation;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(context_->on_inner_node(index_row, is_coverd_by_range, operation))) {
        LOG_WARN("Fail to do on inner node", KR(ret), K(is_coverd_by_range), K(index_row), KPC(context_));
      } else {
        switch (operation) {
        case ObIMultiRangeEstimateContext::GOTO_NEXT_LEVEL:
          if (OB_FAIL(goto_next_level_node(index_row, PathInfo(index_row_count, i, path_info), level + 1, 1.0 * open_index_micro_block_limit / index_row_count))) {
            LOG_WARN("Fail to goto next level", KR(ret), K(index_row));
          }
          break;
        case ObIMultiRangeEstimateContext::NOTHING:
          // we averagly allocate open_index_micro_block_limit for current node's children
          remain_can_visited_node_cnt_ += 1.0 * open_index_micro_block_limit / index_row_count;
          break;
        };
      }
    }

    // step 3. continue to traverse next range
    if (OB_FAIL(ret)) {
    } else if (micro_index_info != nullptr) {
      // only root node can call next range
      break;
    } else if (OB_FAIL(context_->next_range())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Fail to advance context", KR(ret), KPC(context_));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      remain_can_visited_node_cnt_
          += avg_range_visited_node_cnt_ * (context_->get_curr_range_idx() - curr_range_idx);
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::load_root(const ObMicroBlockData &block_data,
                                                         ObIndexBlockTreeTraverser &traverser)
{
  int ret = OB_SUCCESS;

  if (caches_[0].is_inited_) {
    // is inited, skip
  } else if (FALSE_IT(caches_[0].micro_data_ = block_data)) {
  } else if (OB_FAIL(caches_[0].context_.init(block_data, nullptr, traverser))) {
    LOG_WARN("Fail to init root node context", KR(ret), K(block_data));
  } else {
    caches_[0].is_inited_ = true;
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::find(const int64_t level,
                                                    const ObMicroIndexInfo *micro_index_info,
                                                    bool &is_in_cache)
{
  int ret = OB_SUCCESS;

  is_in_cache = false;

  if (OB_UNLIKELY(level >= MAX_CACHE_LEVEL)) {
    ret = OB_NOT_SUPPORTED;
  } else if (!caches_[level].is_inited_) {
    is_in_cache = false;
  } else if (micro_index_info == nullptr) {
    // root node
    is_in_cache = caches_[0].is_inited_;
  } else {
    ObMicroBlockCacheKey key(MTL_ID(), *micro_index_info);
    is_in_cache = (key == caches_[level].key_);
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::get(const int64_t level,
                                                   const ObMicroIndexInfo *micro_index_info,
                                                   ObIndexBlockTreeTraverser &traverser,
                                                   CacheNode *&cache_node,
                                                   bool &is_in_cache)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(find(level, micro_index_info, is_in_cache))) {
    LOG_WARN("Fail to find node in cache", KR(ret), KPC(micro_index_info));
  } else if (is_in_cache) {
    cache_node = &caches_[level];
  } else {
    // not in cache, should load it
    cache_node = &caches_[level];

    if (micro_index_info == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Must load root node at first", KR(ret), K(level));
    } else if (cache_node->is_inited_) {
      cache_node->reuse();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prefetch(*micro_index_info, cache_node->micro_handle_))) {
      LOG_WARN("Fail to prefetch next level micro block data", KR(ret), K(micro_index_info));
    } else if (OB_FAIL(cache_node->micro_handle_.get_micro_block_data(
                  &cache_node->macro_block_reader_, cache_node->micro_data_, micro_index_info->is_data_block()))) {
      LOG_WARN("Fail to get index block data", KR(ret), K(cache_node->micro_data_));
    } else if (!micro_index_info->is_data_block()
               && OB_FAIL(
                   cache_node->is_inited_
                       ? cache_node->context_.open(cache_node->micro_data_, micro_index_info)
                       : cache_node->context_.init(cache_node->micro_data_, micro_index_info, traverser))) {
      LOG_WARN("Fail to init root node context", KR(ret), K(cache_node->micro_data_));
    } else if (OB_FAIL(
                   cache_node->key_.assign(ObMicroBlockCacheKey(MTL_ID(), *micro_index_info)))) {
      LOG_WARN("Fail to assign cache key", KR(ret), KPC(micro_index_info));
    } else {
      cache_node->is_inited_ = true;
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::PathNodeCaches::prefetch(const ObMicroIndexInfo &micro_index_info,
                                                        ObMicroBlockDataHandle &micro_handle)
{
  int ret = OB_SUCCESS;

  bool found = false;
  micro_handle.reset();
  micro_handle.allocator_ = &allocator_;

  ObMicroBlockCacheKey key(MTL_ID(), micro_index_info);
  ObIMicroBlockCache *cache
      = micro_index_info.is_data_block()
            ? &blocksstable::ObStorageCacheSuite::get_instance().get_block_cache()
            : &blocksstable::ObStorageCacheSuite::get_instance().get_index_block_cache();

  if (OB_ISNULL(cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get block cache because of null", KR(ret), KP(cache));
  } else if (OB_FAIL(cache->get_cache_block(key, micro_handle.cache_handle_))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("Fail to get cache block", KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    found = true;
    micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
  }

  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(micro_index_info.row_header_->fill_micro_des_meta(true /* deep_copy_key */,
                                                                  micro_handle.des_meta_))) {
      LOG_WARN("Fail to fill micro block deserialize meta", KR(ret));
    } else if (OB_FAIL(cache->prefetch(MTL_ID(),
                                       micro_index_info.get_macro_id(),
                                       micro_index_info,
                                       /* use_cache */ true,
                                       ObTabletID(ObTabletID::INVALID_TABLET_ID),
                                       micro_handle.io_handle_,
                                       &allocator_))) {
      LOG_WARN("Fail to prefetch data micro block", KR(ret), K(micro_index_info));
    } else if (ObSSTableMicroBlockState::UNKNOWN_STATE == micro_handle.block_state_) {
      micro_handle.tenant_id_ = MTL_ID();
      micro_handle.macro_block_id_ = micro_index_info.get_macro_id();
      micro_handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
      micro_handle.micro_info_.set(micro_index_info.get_block_offset(),
                                   micro_index_info.get_block_size(),
                                   micro_index_info.get_logic_micro_id(),
                                   micro_index_info.get_data_checksum());
    }
  }

  return ret;
}

int ObIndexBlockTreeTraverser::TreeNodeContext::init(const ObMicroBlockData &block_data,
                                                     const ObMicroIndexInfo *micro_index_info,
                                                     ObIndexBlockTreeTraverser &traverser)
{
  int ret = OB_SUCCESS;

  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         false,  /* is daily merge scan*/
                         false,  /* is read multiple macro block*/
                         false,  /* sys task scan, read one macro block in single io*/
                         false,  /* is full row scan*/
                         false,  /* index back */
                         false); /* query stat */

  if (OB_UNLIKELY(!traverser.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Invalid traverser", KR(ret));
  } else if (OB_FAIL(scanner_.init(traverser.get_read_info()->get_datum_utils(),
                                   allocator_,
                                   query_flag,
                                   traverser.get_sstable()->get_macro_offset()))) {
    LOG_WARN("Fail to init scanner", KR(ret), K(traverser));
  } else if (OB_FAIL(open(block_data, micro_index_info))) {
    LOG_WARN("Fail to open index block", KR(ret), K(scanner_));
  }

  return ret;
}

int ObIndexBlockTreeTraverser::TreeNodeContext::open(const ObMicroBlockData &block_data,
                                                     const ObMicroIndexInfo *micro_index_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(scanner_.open(micro_index_info == nullptr
                                ? ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID
                                : micro_index_info->get_macro_id(),
                            block_data))) {
    LOG_WARN("Fail to open index block", KR(ret), K(scanner_));
  }

  return ret;
}

void ObIndexBlockTreeTraverser::TreeNodeContext::reuse()
{
  scanner_.reuse();
}

int ObIndexBlockTreeTraverser::TreeNodeContext::locate_range(const ObDatumRange &range,
                                                             int64_t &index_row_count)
{
  int ret = OB_SUCCESS;

  index_row_count = 0;

  if (OB_FAIL(scanner_.locate_range(range, true, true))) {
    if (ret != OB_BEYOND_THE_RANGE) {
      LOG_WARN("Fail to locate range", KR(ret), K(range));
    }
  } else if (OB_FAIL(scanner_.get_index_row_count(index_row_count))) {
    LOG_WARN("Fail to get index row count", KR(ret), K(scanner_));
  }

  return ret;
}

int ObIndexBlockTreeTraverser::TreeNodeContext::get_next_index_row(ObMicroIndexInfo &idx_block_row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(scanner_.get_next(idx_block_row))) {
    LOG_WARN("Fail to get next index row", KR(ret), K(scanner_));
  }

  return ret;
}

template <typename Range>
int ObRangeCompartor::is_sorted(const ObIArray<Range> &ranges, ObRangeCompartor &compartor, bool &is_sorted)
{
  int ret = OB_SUCCESS;

  is_sorted = true;

  for (int64_t i = 1; OB_SUCC(ret) && is_sorted && i < ranges.count(); i++) {
    is_sorted = compartor.operator()(ranges.at(i - 1), ranges.at(i));
    if (OB_FAIL(compartor.sort_ret_)) {
      LOG_WARN("Fail to compare range", KR(ret), K(ranges), K(i));
    }
  }

  return ret;
}

template int ObRangeCompartor::is_sorted<ObRangeInfo>(const ObIArray<ObRangeInfo> &ranges, ObRangeCompartor &compartor, bool &is_sorted);
template int ObRangeCompartor::is_sorted<ObPairStoreAndDatumRange>(const ObIArray<ObPairStoreAndDatumRange> &ranges, ObRangeCompartor &compartor, bool &is_sorted);

int ObMultiRangeRowEstimateContext::init(const ObIArray<ObPairStoreAndDatumRange> &ranges,
                                         const ObITableReadInfo &read_info,
                                         const bool need_sort,
                                         const ObRangePrecision &range_precision,
                                         const bool can_goto_micro_level)
{
  int ret = OB_SUCCESS;

  // push all range's reference to range info array
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init estimate context twice", KR(ret));
  } else if (OB_UNLIKELY(ranges.empty() || !range_precision.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for context", KR(ret), K(ranges), K(range_precision));
  } else if (OB_FAIL(ranges_.reserve(ranges.count()))) {
    LOG_WARN("Fail to reserve range info array", KR(ret), K(ranges));
  } else if (FALSE_IT(range_precision_ = range_precision)) {
  } else if (OB_FAIL(range_precision_.recalc_range_precision(ranges.count()))) {
    LOG_WARN("Fail to recalc range precision", KR(ret));
  } else {
    curr_range_idx_ = 0;
    sample_step_ = range_precision_.get_sample_step();
    can_goto_micro_level_ = can_goto_micro_level;
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i++) {
      if (OB_FAIL(ranges_.push_back(ObRangeInfo(&ranges.at(i).datum_range_)))) {
        LOG_WARN("Fail to push back to range array", KR(ret), K(ranges), K(i));
      }
    }
  }

  // sort range info array by start key
  if (OB_FAIL(ret)) {
  } else if (need_sort) {
    ObRangeCompartor compartor(read_info);
    bool is_sorted = false;

    if (OB_FAIL(ObRangeCompartor::is_sorted(ranges_, compartor, is_sorted))) {
      LOG_WARN("Fail to check if ranges is sorted", KR(ret), K(ranges_));
    } else if (!is_sorted) {
      lib::ob_sort(ranges_.begin(), ranges_.end(), compartor);
      if (OB_FAIL(compartor.sort_ret_)) {
        LOG_WARN("Fail to sort range array", KR(ret), K(ranges_));
      }
    }
  }

  is_inited_ = OB_SUCC(ret);

  return ret;
}

int ObMultiRangeRowEstimateContext::on_node_estimate(const ObMicroIndexInfo &index_row,
                                                     const bool is_coverd_by_range)
{
  ranges_.at(curr_range_idx_).row_count_
      += is_coverd_by_range ? index_row.get_row_count() : index_row.get_row_count() >> 1;
  ranges_.at(curr_range_idx_).macro_block_count_
      += max(1ll,
             is_coverd_by_range ? index_row.get_macro_block_count()
                                : index_row.get_macro_block_count() >> 1);
  return OB_SUCCESS;
}

int ObMultiRangeRowEstimateContext::on_leaf_node(const ObMicroIndexInfo &index_row,
                                                 const ObPartitionEst &est)
{
  ranges_.at(curr_range_idx_).row_count_ += est.physical_row_count_;
  return OB_SUCCESS;
}

int ObMultiRangeRowEstimateContext::on_inner_node(const ObMicroIndexInfo &index_row,
                                                  const bool is_coverd_by_range,
                                                  ObTraverserOperationType &operation)
{
  int ret = OB_SUCCESS;

  if (is_coverd_by_range) {
    ranges_.at(curr_range_idx_).row_count_ += index_row.get_row_count();
    ranges_.at(curr_range_idx_).macro_block_count_ += index_row.get_macro_block_count();
    operation = NOTHING;
  } else if (index_row.get_row_count() < (ranges_.at(curr_range_idx_).row_count_ / SHOULD_ESTIMATE_RATIO)
             || (!can_goto_micro_level_ && index_row.is_leaf_block())) {
    // If the node's row count is significantly smaller than the current range's row count, it is suitable for estimating the current range.
    // if we can't goto micro level, we only estimate on leaf node
    ranges_.at(curr_range_idx_).row_count_ += index_row.get_row_count() >> 1;
    ranges_.at(curr_range_idx_).macro_block_count_ += max(1ll, index_row.get_macro_block_count() >> 1);
    operation = NOTHING;
  } else {
    operation = ObTraverserOperationType::GOTO_NEXT_LEVEL;
  }

  return ret;
}

int ObMultiRangeRowEstimateContext::next_range()
{
  int ret = OB_SUCCESS;

  const int64_t next_index = curr_range_idx_ + sample_step_;
  do {
    if (curr_range_idx_ + 1 >= ranges_.count()) {
      curr_range_idx_ = ranges_.count();
      ret = OB_ITER_END;
      break;
    } else {
      ++curr_range_idx_;
      if (curr_range_idx_ < next_index) {
        ranges_.at(curr_range_idx_).row_count_
            = curr_range_idx_ - 1 >= 0 ? ranges_.at(curr_range_idx_ - 1).row_count_ : 0;
        ranges_.at(curr_range_idx_).macro_block_count_
            = curr_range_idx_ - 1 >= 0 ? ranges_.at(curr_range_idx_ - 1).macro_block_count_ : 0;
      }
    }
  } while (OB_SUCC(ret) && curr_range_idx_ < next_index);

  return ret;
}

int64_t ObMultiRangeRowEstimateContext::get_row_count_sum() const
{
  int64_t sum = 0;

  for (int64_t i = 0; i < ranges_.count(); i++) {
    sum += ranges_.at(i).row_count_;
  }

  return sum;
}

int64_t ObMultiRangeRowEstimateContext::get_macro_block_count_sum() const
{
  int64_t sum = 0;

  for (int64_t i = 0; i < ranges_.count(); i++) {
    sum += ranges_.at(i).macro_block_count_;
  }

  return sum;
}

void ObMultiRangeRowEstimateContext::reuse()
{
  for (int64_t i = 0; i < ranges_.count(); i++) {
    ranges_.at(i).row_count_ = 0;
    ranges_.at(i).macro_block_count_ = 0;
  }
  curr_range_idx_ = 0;
}

int ObMultiRangeInfoEstimateContext::on_node_estimate(const ObMicroIndexInfo &index_row,
                                                      const bool is_coverd_by_range)
{
  int64_t estimate_factor = is_coverd_by_range ? 0 : 1;

  total_size_ += calc_estimate_size(index_row, is_coverd_by_range);
  total_row_count_ += index_row.get_row_count() >> estimate_factor;
  total_macro_block_count_ += (index_row.get_macro_id() != last_macro_block_id_
                               || index_row.get_block_offset() != last_index_row_in_block_offset_)
                                  ? max(1ll, index_row.get_macro_block_count() >> estimate_factor)
                                  : 0;
  last_macro_block_id_ = index_row.get_macro_id();
  last_index_row_in_block_offset_ = index_row.get_block_offset();
  return OB_SUCCESS;
}

int ObMultiRangeInfoEstimateContext::on_leaf_node(const ObMicroIndexInfo &index_row,
                                                  const ObPartitionEst &est)
{
  if (index_row.get_row_count() > 0) {
    total_size_ += est.physical_row_count_ * index_row.get_block_size() / index_row.get_row_count();
  }
  total_row_count_ += est.physical_row_count_;
  total_macro_block_count_ += (index_row.get_macro_id() != last_macro_block_id_
                               || index_row.get_block_offset() != last_index_row_in_block_offset_)
                                  ? 1
                                  : 0;
  last_macro_block_id_ = index_row.get_macro_id();
  last_index_row_in_block_offset_ = index_row.get_block_offset();
  return OB_SUCCESS;
}

int ObMultiRangeInfoEstimateContext::on_inner_node(const ObMicroIndexInfo &index_row,
                                                   const bool is_coverd_by_range,
                                                   ObTraverserOperationType &operation)
{
  int ret = OB_SUCCESS;

  if (is_coverd_by_range) {
    total_size_ += calc_estimate_size(index_row, is_coverd_by_range);
    total_row_count_ += index_row.get_row_count();
    total_macro_block_count_ += (index_row.get_macro_id() != last_macro_block_id_
                                 || index_row.get_block_offset() != last_index_row_in_block_offset_)
                                    ? index_row.get_macro_block_count()
                                    : 0;
    last_macro_block_id_ = index_row.get_macro_id();
    last_index_row_in_block_offset_ = index_row.get_block_offset();
    operation = NOTHING;
  } else {
    if (index_row.get_macro_block_count() >= 1) {
      avg_micro_block_size_ = index_row.get_macro_block_count() * OB_DEFAULT_MACRO_BLOCK_SIZE
                              / index_row.get_micro_block_count();
    }
    operation = ObTraverserOperationType::GOTO_NEXT_LEVEL;
  }

  // adapt now sql implement, macro level is enough
  if (operation == GOTO_NEXT_LEVEL && index_row.is_leaf_block()) {
    if (OB_FAIL(on_node_estimate(index_row, is_coverd_by_range))) {
      LOG_WARN("Fail to on node estimate", KR(ret), K(index_row));
    } else {
      operation = NOTHING;
    }
  }

  return ret;
}

int ObMultiRangeSplitContext::init(const ObIArray<ObPairStoreAndDatumRange> &ranges,
                                   const ObITableReadInfo &read_info,
                                   const int64_t split_row_limit,
                                   ObIAllocator &allocator,
                                   const ObIArray<ObRangeInfo> &range_infos,
                                   ObIArray<ObSplitRangeInfo> &split_ranges,
                                   const bool need_sort,
                                   const ObRangePrecision &range_precision,
                                   const bool can_goto_micro_level)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObMultiRangeRowEstimateContext::init(ranges, read_info, need_sort, range_precision, can_goto_micro_level))) {
    LOG_WARN("Fail to init base context", KR(ret));
  } else {
    split_row_limit_ = split_row_limit;
    split_row_upper_limit_ = split_row_limit * SPLIT_ROW_UPPER_RATIO;
    allocator_ = &allocator;
    range_infos_ = &range_infos;
    split_ranges_ = &split_ranges;
    read_info_ = &read_info;
  }

  return ret;
}

int ObMultiRangeSplitContext::try_to_add_new_split_range(int64_t &curr_row_count,
                                                         const int64_t add_row_count,
                                                         const ObCommonDatumRowkey &rowkey)
{
  //              start                       end
  //               ──────────────────────────────
  //                       ▲         ▲       ▲
  //                       │         │       │
  // Info 1: split_rowkey──┘         │       │
  //                                 │       │
  // Info 2: split_rowkey────────────┘       │
  //                                         │
  // Info 3: split_rowkey────────────────────┘
  // ......

  int ret = OB_SUCCESS;

  ObSplitRangeInfo *info = nullptr;
  curr_row_count += add_row_count;

  if (curr_row_count < split_row_limit_) {
  } else if (OB_ISNULL(info = split_ranges_->alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc place holder", KR(ret));
  } else {
    int cmp_ret = 0;
    info->row_count_ = curr_row_count;
    info->origin_range_ = ranges_.at(curr_range_idx_).range_;
    curr_row_count = 0;

    // split_rowkey = min(range.endkey, rowkey)
    if (OB_FAIL(rowkey.compare(info->origin_range_->get_end_key(),
                               read_info_->get_datum_utils(),
                               cmp_ret,
                               /* compare datum cnt */ false))) {
      LOG_WARN("Fail to compare end rowkey", KR(ret));
    } else if (cmp_ret >= 0) {
      if (OB_FAIL(info->origin_range_->get_end_key().deep_copy(info->split_rowkey_, *allocator_))) {
        LOG_WARN("Fail to deep copy rowkey", KR(ret), K(ranges_), K(curr_range_idx_));
      } else {
        info->is_range_end_split_ = true;
      }
    } else {
      if (OB_FAIL(rowkey.deep_copy(info->split_rowkey_, *allocator_))) {
        LOG_WARN("Fail to deep copy rowkey", KR(ret), K(rowkey));
      } else {
        info->is_range_end_split_ = false;
      }
    }
  }

  return ret;
}

int ObMultiRangeSplitContext::on_node_estimate(const ObMicroIndexInfo &index_row,
                                               const bool is_coverd_by_range)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(try_to_add_new_split_range(ranges_.at(curr_range_idx_).row_count_,
                                         is_coverd_by_range ? index_row.get_row_count()
                                                            : index_row.get_row_count() >> 1,
                                         index_row.endkey_))) {
    LOG_WARN("Fail to add new split range", KR(ret), K(index_row), K(ranges_), K(curr_range_idx_));
  }

  return ret;
}

int ObMultiRangeSplitContext::on_leaf_node(const ObMicroIndexInfo &index_row,
                                           const ObPartitionEst &est)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(try_to_add_new_split_range(ranges_.at(curr_range_idx_).row_count_,
                                         est.physical_row_count_,
                                         index_row.endkey_))) {
    LOG_WARN("Fail to add new split range",
             KR(ret),
             K(index_row),
             K(est),
             K(ranges_),
             K(curr_range_idx_));
  }

  return ret;
}

int ObMultiRangeSplitContext::on_inner_node(const ObMicroIndexInfo &index_row,
                                            const bool is_coverd_by_range,
                                            ObTraverserOperationType &operation)
{
  int ret = OB_SUCCESS;

  int64_t &curr_row_count = ranges_.at(curr_range_idx_).row_count_;
  int64_t inc_row_count = 0;

  if (is_coverd_by_range) {
    inc_row_count = index_row.get_row_count();
    operation = curr_row_count + inc_row_count > split_row_upper_limit_ ? GOTO_NEXT_LEVEL : NOTHING;
  } else if (index_row.get_row_count() < (curr_row_count / SHOULD_ESTIMATE_RATIO)) {
    inc_row_count = index_row.get_row_count() >> 1;
    operation = NOTHING;
  } else {
    operation = GOTO_NEXT_LEVEL;
  }

  if (operation == GOTO_NEXT_LEVEL && !can_goto_micro_level_ && index_row.is_leaf_block()) {
    inc_row_count = is_coverd_by_range ? index_row.get_row_count() : index_row.get_row_count() >> 1;
    operation = NOTHING;
  }

  if (operation == NOTHING) {
    if (OB_FAIL(try_to_add_new_split_range(curr_row_count, inc_row_count, index_row.endkey_))) {
      LOG_WARN("Fail to add new split range",
               KR(ret),
               K(index_row),
               K(curr_row_count),
               K(is_coverd_by_range),
               K(split_row_limit_));
    }
  }

  return ret;
}

int ObMultiRangeSplitContext::next_range()
{
  int ret = OB_SUCCESS;

  int64_t next_index = curr_range_idx_ + sample_step_;
  do {
    if (!is_ended()) {
      ObRangeInfo &curr_range = ranges_.at(curr_range_idx_);
      const ObSplitRangeInfo *last_split_info
          = split_ranges_->empty() ? nullptr : &split_ranges_->at(split_ranges_->count() - 1);

      if (last_split_info && last_split_info->origin_range_ == curr_range.range_
          && last_split_info->is_range_end_split_) {
        // if there are NOT remaining part, skip
      } else {
        ObSplitRangeInfo *info = nullptr;
        if (OB_ISNULL(info = split_ranges_->alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Fail to alloc place holder", KR(ret));
        } else if (OB_FAIL(curr_range.range_->get_end_key().deep_copy(info->split_rowkey_,
                                                                      *allocator_))) {
          LOG_WARN("Fail to deep copy rowkey", KR(ret), K(ranges_), K(curr_range_idx_));
        } else {
          info->row_count_ = curr_range.row_count_;
          info->origin_range_ = curr_range.range_;
          info->is_range_end_split_ = true;
          curr_range.row_count_ = 0;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (curr_range_idx_ + 1 >= ranges_.count()) {
        curr_range_idx_ = ranges_.count();
        ret = OB_ITER_END;
        break;
      } else {
        ++curr_range_idx_;
        if (curr_range_idx_ < next_index) {
          // when range precision is less than 100, some range will be skip estimate and split
          // just copy the row_count (estimated by row estimate context) and split it as a whole range
          ranges_.at(curr_range_idx_).row_count_ = range_infos_->at(curr_range_idx_).row_count_;
        } else if (curr_range_idx_ == next_index
                   && range_infos_->at(curr_range_idx_).row_count_ < split_row_limit_) {
          // if the range's row count is less than split row limit
          // just split it as a whole range and skip it
          ranges_.at(curr_range_idx_).row_count_ = range_infos_->at(curr_range_idx_).row_count_;
          next_index++;
        }
      }
    } else {
      ret = OB_ITER_END;
      break;
    }
  } while (OB_SUCC(ret) && curr_range_idx_ < next_index);

  return ret;
}

int ObPartitionMultiRangeSpliter::get_tables(ObTableStoreIterator &table_iter,
                                             ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;

  ObITable *last_major_sstable = nullptr;
  int64_t major_size = 0;
  int64_t major_row_count = 0;

  tables.reset();

  while (OB_SUCC(ret)) {
    ObITable *table = nullptr;

    if (OB_FAIL(table_iter.get_next(table))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to get next table", KR(ret), K(table_iter));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to get valid table", KR(ret), K(table_iter));
    } else if (table->is_major_sstable()) {
      if (table->is_co_sstable()) {
        ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table);
        if (co_sstable->is_rowkey_cg_base() && !co_sstable->is_cgs_empty_co_table()) {
          major_size = co_sstable->get_cs_meta().occupy_size_;
        } else {
          major_size = co_sstable->get_occupy_size();
        }
      } else {
        major_size = static_cast<ObSSTable *>(table)->get_occupy_size();
      }
      major_row_count = static_cast<ObSSTable *>(table)->get_row_count();
      last_major_sstable = table;
    } else if (table->is_minor_sstable()) {
      if (static_cast<ObSSTable *>(table)->get_occupy_size() <= MIN_SPLIT_TABLE_SIZE
          && static_cast<ObSSTable *>(table)->get_row_count() <= MIN_SPLIT_TABLE_ROW_COUNT) {
        // very small table, skip
      } else if (OB_FAIL(tables.push_back(table))) {
        LOG_WARN("Fail to add minor sstable", KR(ret), KPC(table));
      }
    } else if (table->is_data_memtable()) {
      int64_t mem_rows = 0;
      int64_t mem_size = 0;
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
      if (OB_FAIL(memtable->estimate_phy_size(nullptr, nullptr, mem_size, mem_rows))) {
        STORAGE_LOG(WARN, "Failed to get estimate size from memtable", K(ret));
      } else if (mem_size <= MIN_SPLIT_TABLE_SIZE
                 && mem_rows <= MIN_SPLIT_TABLE_ROW_COUNT) {
        // very small table, skip
      } else if (OB_FAIL(tables.push_back(table))) {
        LOG_WARN("Fail to add minor sstable", KR(ret), KPC(table));
      }
    } else if (table->is_direct_load_memtable()) {
      // TODO: @suzhi.yt 可能会导致划分range不均衡, 后续实现
    }
  }

  if (OB_SUCC(ret) && (major_size > MIN_SPLIT_TABLE_SIZE || major_row_count > MIN_SPLIT_TABLE_ROW_COUNT)) {
    if (OB_FAIL(tables.push_back(last_major_sstable))) {
      LOG_WARN("Fail to add last major sstable", KR(ret), KPC(last_major_sstable));
    }
  }

  return ret;
}

template <typename ObRange>
int ObPartitionMultiRangeSpliter::fast_build_range_array(
    const ObIArray<ObStoreRange> &ranges,
    const int64_t expected_task_cnt,
    ObIAllocator &allocator,
    ObArrayArray<ObRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(expected_task_cnt > ranges.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to build single range array", KR(ret), K(ranges));
  } else if (OB_FAIL(multi_range_split_array.reserve(expected_task_cnt))) {
    LOG_WARN("Fail to reserve array memory", KR(ret));
  } else {
    ObSEArray<ObRange, 4> one_task_ranges;
    ObRange range;
    int64_t avg_range_cnt = ranges.count() / expected_task_cnt,
            remain_range_cnt = ranges.count() % expected_task_cnt;

    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i++) {
      int64_t task_range_cnt
          = avg_range_cnt + (multi_range_split_array.count() < remain_range_cnt ? 1 : 0);

      if constexpr (std::is_same_v<ObRange, ObStoreRange>) {
        if (OB_FAIL(ranges.at(i).deep_copy(allocator, range))) {
          LOG_WARN("Fail to deep copy store range", KR(ret), K(i), K(ranges.at(i)));
        }
      } else if constexpr (std::is_same_v<ObRange, ObDatumRange>) {
        ObStoreRange tmp_range;
        if (OB_FAIL(ranges.at(i).deep_copy(allocator, tmp_range))) {
          LOG_WARN("Fail to deep copy store range", KR(ret), K(i), K(ranges.at(i)));
        } else if (OB_FAIL(range.from_range(tmp_range, allocator))) {
          LOG_WARN("Fail to deep copy store range", KR(ret), K(i), K(ranges.at(i)));
        }
      } else {
        static_assert(std::is_same_v<ObRange, ObStoreRange>
                          || std::is_same_v<ObRange, ObDatumRange>,
                      "only support ObDatumRange and ObStoreRange");
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(one_task_ranges.push_back(range))) {
        LOG_WARN("Fail to push back store range", KR(ret), K(range));
      } else if (one_task_ranges.count() >= task_range_cnt) {
        if (OB_FAIL(multi_range_split_array.push_back(one_task_ranges))) {
          LOG_WARN("Fail to push range split array", KR(ret), K(one_task_ranges));
        } else {
          one_task_ranges.reset();
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!one_task_ranges.empty() || multi_range_split_array.count() != expected_task_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to fast build range array",
               KR(ret),
               K(multi_range_split_array),
               K(expected_task_cnt),
               K(ranges),
               K(one_task_ranges));
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::transform_to_datum_range_and_sort(
    const ObIArray<ObStoreRange> &ranges,
    const ObITableReadInfo &read_info,
    ObIAllocator &allocator,
    ObIArray<ObPairStoreAndDatumRange> &sorted_ranges)
{
  int ret = OB_SUCCESS;

  sorted_ranges.reset();
  if (OB_FAIL(sorted_ranges.reserve(ranges.count()))) {
    LOG_WARN("Fail to reserve range array", KR(ret));
  }

  for (int i = 0; OB_SUCC(ret) && i < ranges.count(); i++) {
    ObPairStoreAndDatumRange *range = nullptr;
    if (OB_ISNULL(range = sorted_ranges.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc place holder for datum range", KR(ret));
    } else if (OB_FAIL(range->datum_range_.from_range(ranges.at(i), allocator))) {
      LOG_WARN("Fail to transform store range to datum range", KR(ret), K(ranges.at(i)));
    } else {
      range->origin_store_range_ = &ranges.at(i);
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObRangeCompartor cmp(read_info);
    bool is_sorted = false;

    if (OB_FAIL(ObRangeCompartor::is_sorted(sorted_ranges, cmp, is_sorted))) {
      LOG_WARN("Fail to check if ranges is sorted", KR(ret), K(sorted_ranges));
    } else if (!is_sorted) {
      lib::ob_sort(sorted_ranges.get_data(), sorted_ranges.get_data() + sorted_ranges.count(), cmp);
      if (OB_FAIL(cmp.sort_ret_)) {
        LOG_WARN("Fail to sort range array", KR(ret));
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliterHelper::init(ObIAllocator &allocator,
                                             const ObITableReadInfo &read_info,
                                             const ObIArray<ObStoreRange> &ranges)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Fail to init spliter helper", KR(ret));
  } else if (OB_UNLIKELY(!read_info.is_valid() || ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid read info or ranges", KR(ret), K(ranges), K(read_info));
  } else {
    allocator_ = &allocator;
    read_info_ = &read_info;
    table_id_ = ranges.at(0).get_table_id();
    is_inited_ = true;
  }

  return ret;
}

template <typename ObTemplateRowkey>
int ObPartitionMultiRangeSpliterHelper::deepcopy_rowkey(const ObDatumRowkey &rowkey,
                                                        const bool for_compaction,
                                                        ObTemplateRowkey &target)
{
  int ret = OB_SUCCESS;

  if constexpr (std::is_same_v<ObStoreRowkey, ObTemplateRowkey>) {
    if (for_compaction) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to deepcopy rowkey, type must be datum rowkey when for compaction", KR(ret));
    } else if (rowkey.is_min_rowkey()) {
      target.set_min();
    } else if (rowkey.is_max_rowkey()) {
      target.set_max();
    } else if (OB_FAIL(rowkey.deep_copy(target,
                                        *allocator_,
                                        read_info_->get_columns_desc(),
                                        read_info_->get_schema_rowkey_count(),
                                        0))) {
      LOG_WARN("Fail to deepcopy rowkey (datum -> store)", KR(ret), K(for_compaction), K(rowkey));
    }
  } else if constexpr (std::is_same_v<ObDatumRowkey, ObTemplateRowkey>) {
    ObDatumRowkey tmp_rowkey;
    if (!for_compaction) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Fail to deepcopy rowkey, type must be store rowkey when not for compaction", KR(ret));
    } else if (rowkey.is_min_rowkey()) {
      target.set_min_rowkey();
    } else if (rowkey.is_max_rowkey()) {
      target.set_max_rowkey();
    } else if (OB_FAIL(target.assign(const_cast<ObStorageDatum *>(rowkey.get_datum_ptr()),
                                     read_info_->get_schema_rowkey_count()))) {
      LOG_WARN("Fail to assign tmp rowkey", KR(ret), K(for_compaction), K(rowkey));
    } else if (OB_FAIL(target.deep_copy(tmp_rowkey, *allocator_))) {
      LOG_WARN("Fail to deepcopy rowkey (datum -> datum)", KR(ret), K(for_compaction), K(rowkey));
    } else if (OB_FAIL(tmp_rowkey.to_multi_version_rowkey(/* min_value */ false, *allocator_, target))) {
      LOG_WARN("Fail to transform to multi version rowkey");
    }
  } else {
    static_assert(std::is_same_v<ObTemplateRowkey, ObStoreRowkey>
                      || std::is_same_v<ObTemplateRowkey, ObDatumRowkey>,
                  "only support ObDatumRowkey and ObStoreRowkey");
  }

  return ret;
}

template <typename ObRange>
int ObPartitionMultiRangeSpliterHelper::construct_range(const ObDatumRowkey &start_key,
                                                        const ObDatumRowkey &end_key,
                                                        const ObBorderFlag &flag,
                                                        const bool for_compaction,
                                                        ObRange &range)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init spliter helper", KR(ret));
  } else if (OB_FAIL(deepcopy_rowkey(start_key,
                                     for_compaction,
                                     const_cast<ObRange &>(range).get_start_key()))) {
    LOG_WARN("Fail to transform start key to store range", KR(ret));
  } else if (OB_FAIL(deepcopy_rowkey(end_key,
                                     for_compaction,
                                     const_cast<ObRange &>(range).get_end_key()))) {
    LOG_WARN("Fail to transform end key to store range", KR(ret));
  } else {
    range.set_border_flag(flag);

    if constexpr (std::is_same_v<ObRange, ObStoreRange>) {
      range.set_table_id(table_id_);
    }
  }

  return ret;
}

template <typename ObRange>
int ObPartitionMultiRangeSpliterHelper::construct_and_push_range(
    const ObDatumRowkey *start_key,
    const ObDatumRowkey *end_key,
    const ObBorderFlag &flag,
    const bool for_compaction,
    ObIArray<ObRange> &ranges)
{
  int ret = OB_SUCCESS;

  ObRange range;
  bool is_equal = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init spliter helper", KR(ret));
  } else if (OB_ISNULL(start_key) || OB_ISNULL(end_key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid start key or end key", KR(ret), KPC(start_key), KPC(end_key));
  } else if (OB_FAIL(start_key->equal(*end_key, read_info_->get_datum_utils(), is_equal))) {
    LOG_WARN("Fail to compare start key and end key", KR(ret), KPC(start_key), KPC(end_key));
  } else if (is_equal && !(flag.inclusive_start() && flag.inclusive_end())) {
    // empty range
  } else if (OB_FAIL(construct_range(*start_key, *end_key, flag, for_compaction, range))) {
    LOG_WARN("Fail to construct store range", KR(ret));
  } else if (OB_FAIL(ranges.push_back(range))) {
    LOG_WARN("Fail to push back store range", KR(ret));
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::estimate_ranges_row_and_split(
    const ObIArray<ObPairStoreAndDatumRange> &sorted_ranges,
    const int64_t expected_task_count,
    const ObITableReadInfo &read_info,
    const ObIArray<ObITable *> &tables,
    ObIAllocator &allocator,
    ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
    int64_t &estimate_rows_sum,
    const int64_t max_time,
    const ObRangePrecision &range_precision)
{
  int ret = OB_SUCCESS;

  estimate_rows_sum = 0;

  if (OB_FAIL(heap_element_iters.reserve(tables.count()))) {
    LOG_WARN("Fail to reserve heap element iters", KR(ret));
  }

  int64_t total_macro_block_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
    ObITable *table = tables.at(i);
    if (table->is_sstable()) {
      ObSSTable *sstable = static_cast<ObSSTable *>(table);
      total_macro_block_cnt += sstable->get_data_macro_block_count();
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
    ObITable *table = tables.at(i);
    if (table->is_sstable()) {
      ObSSTable *sstable = static_cast<ObSSTable *>(table);
      int64_t open_index_block_limit
          = max(1,
                INDEX_BLOCK_PER_TIME * max_time * sstable->get_data_macro_block_count()
                    / max(1, total_macro_block_cnt));
      if (OB_FAIL(split_ranges_for_sstable(sorted_ranges,
                                           read_info,
                                           expected_task_count,
                                           open_index_block_limit,
                                           *sstable,
                                           allocator,
                                           heap_element_iters,
                                           estimate_rows_sum,
                                           range_precision))) {
        LOG_WARN("Fail to split ranges for sstable", KR(ret));
      }
    } else if (table->is_data_memtable()) {
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
      if (OB_FAIL(split_ranges_for_memtable(sorted_ranges,
                                            expected_task_count,
                                            *memtable,
                                            allocator,
                                            heap_element_iters,
                                            estimate_rows_sum,
                                            range_precision))) {
        LOG_WARN("Fail to split ranges for memtable", KR(ret));
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::split_ranges_for_sstable(
    const ObIArray<ObPairStoreAndDatumRange> &sorted_ranges,
    const ObITableReadInfo &read_info,
    const int64_t expected_task_count,
    const int64_t open_index_block_limit,
    ObSSTable &sstable,
    ObIAllocator &allocator,
    ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
    int64_t &estimate_rows_sum,
    const ObRangePrecision &range_precision)
{
  int ret = OB_SUCCESS;

  ObIndexBlockTreeTraverser traverser;
  ObMultiRangeRowEstimateContext row_estimate_context;
  ObMultiRangeSplitContext split_context;
  ObSplitRangeHeapElementIter *iter = nullptr;
  bool can_goto_micro_level = false;

  if (OB_ISNULL(iter = heap_element_iters.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc place holder", KR(ret));
  } else if (OB_FAIL(row_estimate_context.init(
                 sorted_ranges, read_info, /* need_sort */ false, range_precision, /* can_goto_micro_level */ false))) {
    LOG_WARN("Fail to init row estimate context", KR(ret));
  } else if (OB_FAIL(traverser.init(sstable, read_info))) {
    LOG_WARN("Fail to init row estimate traverser", KR(ret));
  } else if (OB_FAIL(traverser.traverse(row_estimate_context, open_index_block_limit))) {
    LOG_WARN("Fail to traverse sstable index block tree", KR(ret));
  } else if (row_estimate_context.get_macro_block_count_sum() < expected_task_count * SPLIT_RANGE_FACTOR) {
    can_goto_micro_level = true;
    row_estimate_context.reuse();
    row_estimate_context.set_can_goto_micro_level(true);
    traverser.reuse();

    if (OB_FAIL(traverser.traverse(row_estimate_context, open_index_block_limit))) {
      LOG_WARN("Fail to traverse sstable index block tree", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(estimate_rows_sum += row_estimate_context.get_row_count_sum())) {
  } else if (OB_FAIL(
                 split_context.init(sorted_ranges,
                                    read_info,
                                    calc_row_split_limit(row_estimate_context.get_row_count_sum(),
                                                         expected_task_count,
                                                         sorted_ranges.count()),
                                    allocator,
                                    row_estimate_context.get_ranges(),
                                    iter->get_split_ranges(),
                                    /* need_sort */ false,
                                    range_precision,
                                    can_goto_micro_level))) {
    LOG_WARN("Fail to init split context", KR(ret));
  } else if (FALSE_IT(traverser.reuse())) {
  } else if (OB_FAIL(traverser.traverse(split_context, open_index_block_limit))) {
    LOG_WARN("Fail to traverse sstable index block tree", KR(ret));
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::split_ranges_for_memtable(
    const ObIArray<ObPairStoreAndDatumRange> &ranges,
    const int64_t expected_task_count,
    memtable::ObMemtable &memtable,
    ObIAllocator &allocator,
    ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
    int64_t &estimate_rows_sum,
    const ObRangePrecision &range_precision)
{
  int ret = OB_SUCCESS;

  ObSplitRangeHeapElementIter *iter = nullptr;
  ObArray<int64_t> range_estimate_rows;
  int64_t rows_sum = 0;

  if (OB_ISNULL(iter = heap_element_iters.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc place holder", KR(ret));
  }

  // step 1. estimate rows
  int64_t sample_step = range_precision.get_sample_step();
  int64_t next_sample_point = 0;
  int64_t last_estimate_rows = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i++) {
    int64_t estimate_bytes = 0;
    int64_t estimate_rows = 0;
    if (i >= next_sample_point) {
      if (OB_FAIL(memtable.estimate_phy_size(&ranges.at(i).origin_store_range_->get_start_key(),
                                             &ranges.at(i).origin_store_range_->get_end_key(),
                                             estimate_bytes,
                                             estimate_rows))) {
        LOG_WARN("Fail to estimate rows for memtable", KR(ret), K(ranges.at(i)), K(memtable));
      } else {
        last_estimate_rows = estimate_rows;
        next_sample_point += sample_step;
      }
    } else {
      estimate_rows = last_estimate_rows;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(range_estimate_rows.push_back(estimate_rows))) {
      LOG_WARN("Fail to push back estimate rows", KR(ret), K(estimate_rows), K(memtable));
    } else {
      rows_sum += estimate_rows;
    }
  }

  // step 2. split range
  int64_t row_split_limit = calc_row_split_limit(rows_sum, expected_task_count, ranges.count());
  next_sample_point = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i++) {
    ObSEArray<ObStoreRange, 8> store_ranges;
    int64_t avg_split_range_rows = range_estimate_rows.at(i);

    if (i >= next_sample_point) {
      int64_t split_count = range_estimate_rows.at(i) / row_split_limit + 1;
      if (OB_FAIL(memtable.get_split_ranges(
              *ranges.at(i).origin_store_range_, split_count, store_ranges))) {
        LOG_WARN("Fail to split range for memtable", KR(ret), K(ranges.at(i)), K(memtable));
      } else {
        avg_split_range_rows = range_estimate_rows.at(i) / split_count;
        next_sample_point += sample_step;
      }
    } else {
      if (OB_FAIL(store_ranges.push_back(*ranges.at(i).origin_store_range_))) {
        LOG_WARN("Fail to push back to store ranges", KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      for (int j = 0; OB_SUCC(ret) && j < store_ranges.count(); j++) {
        ObSplitRangeInfo *info = nullptr;

        if (OB_ISNULL(info = iter->get_split_ranges().alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Fail to alloc place holder", KR(ret));
        } else if (OB_FAIL(info->split_rowkey_.from_rowkey(
                       store_ranges.at(j).get_end_key().get_rowkey(), allocator))) {
          LOG_WARN("Fail to transform store rowkey to datum rowkey", KR(ret), K(info));
        } else {
          info->row_count_ = avg_split_range_rows;
          info->origin_range_ = &ranges.at(i).datum_range_;
          info->is_range_end_split_ = (j == store_ranges.count() - 1);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    estimate_rows_sum += rows_sum;
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::build_heap(
    ObIArray<ObSplitRangeHeapElementIter> &heap_element_iters,
    ObBinaryHeap<ObSplitRangeHeapElement, ObSplitRangeHeapElementCompator> &heap)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < heap_element_iters.count(); i++) {
    ObSplitRangeHeapElementIter &iter = heap_element_iters.at(i);
    ObSplitRangeHeapElement element;
    if (OB_FAIL(iter.get_next(element))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Fail to get next element", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(heap.push(element))) {
      LOG_WARN("Fail to push element to heap", KR(ret));
    }
  }

  return ret;
}

template <typename ObRange>
int ObPartitionMultiRangeSpliter::do_task_split_algorithm(
    const ObIArray<ObStoreRange> &ranges,
    const ObITableReadInfo &read_info,
    const int64_t expected_task_count,
    const int64_t avg_task_row_count,
    ObIAllocator &allocator,
    ObBinaryHeap<ObSplitRangeHeapElement, ObSplitRangeHeapElementCompator> &heap,
    ObArrayArray<ObRange> &multi_range_split_array,
    const bool for_compaction)
{
  int ret = OB_SUCCESS;

  ObPartitionMultiRangeSpliterHelper helper;

  ObSEArray<ObRange, 8> one_task_ranges;
  const ObDatumRange *last_origin_range = nullptr;
  const ObDatumRowkey *start_key = nullptr, *end_key = nullptr;
  ObBorderFlag flag;
  int64_t curr_task_rows_sum = 0;
  int64_t pushed_task_count = 0;

  int64_t total_row_count = avg_task_row_count * expected_task_count;
  int64_t curr_avg_row_count = avg_task_row_count;
  int64_t high_task_row_count = max(1.0, (1.0 + (1.0 / SPLIT_RANGE_FACTOR / 2.0)) * curr_avg_row_count);
  int64_t low_task_row_count = max(1.0, (1.0 - (1.0 / SPLIT_RANGE_FACTOR / 2.0)) * curr_avg_row_count);

  if (OB_FAIL(helper.init(allocator, read_info, ranges))) {
    LOG_WARN("Fail to init spliter helper", KR(ret));
  }

  while (OB_SUCC(ret) && !heap.empty()) {
    ObSplitRangeHeapElement element = heap.top();

    if (pushed_task_count == expected_task_count - 1) {
      // all remain range should push into the last task
    } else if (curr_task_rows_sum >= curr_avg_row_count
               || (curr_task_rows_sum >= low_task_row_count
                   && curr_task_rows_sum + element.split_range_info_->row_count_ >= high_task_row_count)) {
      // split one task
      if (OB_FAIL(helper.construct_and_push_range(
              start_key, end_key, flag, for_compaction, one_task_ranges))) {
        LOG_WARN("Fail to construct store range", KR(ret));
      } else if (OB_FAIL(multi_range_split_array.push_back(one_task_ranges))) {
        LOG_WARN("Fail to push back one task ranges", KR(ret));
      } else {
        total_row_count -= curr_task_rows_sum;

        pushed_task_count++;
        one_task_ranges.reset();
        curr_task_rows_sum = 0;
        start_key = end_key;
        flag.unset_inclusive_start();
        flag.unset_inclusive_end();

        curr_avg_row_count = max(1, total_row_count / (expected_task_count - pushed_task_count));
        high_task_row_count = max(1.0, (1.0 + (1.0 / SPLIT_RANGE_FACTOR / 2.0)) * curr_avg_row_count);
        low_task_row_count = max(1.0, (1.0 - (1.0 / SPLIT_RANGE_FACTOR / 2.0)) * curr_avg_row_count);
      }
    }

    // the range is broken, must be separated
    if (OB_FAIL(ret)) {
    } else if (last_origin_range != nullptr
               && element.split_range_info_->origin_range_ != last_origin_range) {
      if (OB_FAIL(helper.construct_and_push_range(
              start_key, end_key, flag, for_compaction, one_task_ranges))) {
        LOG_WARN("Fail to construct store range", KR(ret));
      }
      last_origin_range = nullptr;
      start_key = end_key = nullptr;
      flag.unset_inclusive_start();
      flag.unset_inclusive_end();
    }

    // a separated new range, do init
    if (OB_FAIL(ret)) {
    } else if (nullptr == last_origin_range) {
      last_origin_range = element.split_range_info_->origin_range_;
      start_key = &element.split_range_info_->origin_range_->get_start_key();
      element.split_range_info_->origin_range_->is_left_closed() ? flag.set_inclusive_start()
                                                                 : flag.unset_inclusive_start();
    }

    // update curr endkey and curr_task_rows_sum
    if (OB_FAIL(ret)) {
    } else {
      end_key = &element.split_range_info_->split_rowkey_;
      curr_task_rows_sum += element.split_range_info_->row_count_;

      if (element.split_range_info_->is_range_end_split_) {
        element.split_range_info_->origin_range_->is_right_closed() ? flag.set_inclusive_end()
                                                                    : flag.unset_inclusive_end();
      } else {
        flag.set_inclusive_end();
      }
    }

    // push new element
    ObSplitRangeHeapElement new_element;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(heap.pop())) {
      LOG_WARN("Fail to pop element in heap", KR(ret));
    } else if (OB_FAIL(element.iter_->get_next(new_element))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("Fail to get next element in iter", KR(ret), K(element));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(heap.push(new_element))) {
      LOG_WARN("Fail to push element to heap", KR(ret));
    }
  }

  // make remain ranges into a task
  if (OB_SUCC(ret)) {
    if (OB_FAIL(helper.construct_and_push_range(
            start_key, end_key, flag, for_compaction, one_task_ranges))) {
      LOG_WARN("Fail to transform to store rowkey", KR(ret));
    } else if (!one_task_ranges.empty() && OB_FAIL(multi_range_split_array.push_back(one_task_ranges))) {
      LOG_WARN("Fail to push back one task ranges", KR(ret));
    }
  }

  return ret;
}

template <typename ObRange>
int ObPartitionMultiRangeSpliter::build_range_array(const ObIArray<ObStoreRange> &ranges,
                                                    const int64_t expected_task_count,
                                                    const ObITableReadInfo &index_read_info,
                                                    const ObIArray<ObITable *> &tables,
                                                    ObIAllocator &allocator,
                                                    ObArrayArray<ObRange> &multi_range_split_array,
                                                    const bool for_compaction,
                                                    const int64_t max_time,
                                                    const ObRangePrecision &range_precision)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator tmp_allocator;
  ObArray<ObPairStoreAndDatumRange> sorted_ranges;
  ObArray<ObSplitRangeHeapElementIter> heap_element_iters;
  ObSplitRangeHeapElementCompator cmp(index_read_info);
  ObBinaryHeap<ObSplitRangeHeapElement, ObSplitRangeHeapElementCompator> heap(cmp);

  int64_t estimate_rows_sum = 0;

  if (OB_FAIL(transform_to_datum_range_and_sort(ranges, index_read_info, allocator, sorted_ranges))) {
    LOG_WARN("Fail to transform store range array to datum range array", KR(ret));
  } else if (OB_FAIL(estimate_ranges_row_and_split(sorted_ranges,
                                                   expected_task_count,
                                                   index_read_info,
                                                   tables,
                                                   tmp_allocator,
                                                   heap_element_iters,
                                                   estimate_rows_sum,
                                                   max_time,
                                                   range_precision))) {
    LOG_WARN("Fail to estimate row and split", KR(ret));
  } else if (OB_FAIL(build_heap(heap_element_iters, heap))) {
    LOG_WARN("Fail to build heap for task split", KR(ret));
  } else if (OB_FAIL(do_task_split_algorithm(ranges,
                                             index_read_info,
                                             expected_task_count,
                                             max(1, estimate_rows_sum / expected_task_count),
                                             allocator,
                                             heap,
                                             multi_range_split_array,
                                             for_compaction))) {
    LOG_WARN("Fail to do task split algorithm", KR(ret));
  }

  return ret;
}

bool ObPartitionMultiRangeSpliter::all_range_is_single_rowkey(const ObIArray<ObStoreRange> &ranges)
{
  bool all_single_rowkey = true;

  for (int i = 0; i < ranges.count(); i++) {
    if (!ranges.at(i).is_single_rowkey()) {
      all_single_rowkey = false;
      break;
    }
  }

  return all_single_rowkey;
}

int ObPartitionMultiRangeSpliter::get_multi_range_size(const ObIArray<ObStoreRange> &ranges,
                                                       const ObITableReadInfo &index_read_info,
                                                       const ObIArray<ObITable *> &tables,
                                                       int64_t &total_size,
                                                       const int64_t max_time,
                                                       const int64_t range_precision)
{
  int ret = OB_SUCCESS;

  total_size = 0;
  int64_t unused_total_row_count = 0;
  int64_t unused_total_macro_block_count = 0;
  ObRangePrecision recalc_range_precision(range_precision);

  if (OB_UNLIKELY(!index_read_info.is_valid() || !recalc_range_precision.is_valid() || max_time <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get multi range size",
             KR(ret),
             K(index_read_info),
             K(range_precision),
             K(max_time));
  } else if (ranges.empty()) {
    // do nothing
  } else if (tables.empty()) {
    // only small tables
  } else if (OB_FAIL(recalc_range_precision.recalc_range_precision(ranges.count()))) {
    LOG_WARN("Fail to recalc range precision", KR(ret));
  } else if (OB_FAIL(estimate_ranges_info(ranges,
                                          index_read_info,
                                          tables,
                                          total_size,
                                          unused_total_row_count,
                                          unused_total_macro_block_count,
                                          max_time,
                                          recalc_range_precision))) {
    LOG_WARN("Fail to build range array", KR(ret));
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_multi_ranges_row_count(const ObIArray<ObStoreRange> &ranges,
                                                            const ObITableReadInfo &index_read_info,
                                                            ObTableStoreIterator &table_iter,
                                                            int64_t &row_count,
                                                            int64_t &macro_block_count,
                                                            const int64_t max_time,
                                                            const int64_t range_precision)
{
  int ret = OB_SUCCESS;

  ObRangePrecision recalc_range_precision(range_precision);
  int64_t unused_total_size = 0;
  row_count = 0;
  macro_block_count = 0;

  ObSEArray<ObITable *, 8> tables;

  if (OB_UNLIKELY(!index_read_info.is_valid() || !recalc_range_precision.is_valid() || max_time <= 0
                  || !table_iter.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get multi ranges row count",
             KR(ret),
             K(index_read_info),
             K(range_precision),
             K(max_time),
             K(table_iter));
  } else if (OB_FAIL(get_tables(table_iter, tables))) {
    LOG_WARN("Fail to get split tables", KR(ret), K(table_iter));
  } else if (ranges.empty()) {
    // do nothing
  } else if (tables.empty()) {
    // only small tables
    row_count = ranges.count();
    macro_block_count = 1;
  } else if (OB_FAIL(recalc_range_precision.recalc_range_precision(ranges.count()))) {
    LOG_WARN("Fail to recalc range precision", KR(ret));
  } else if (OB_FAIL(estimate_ranges_info(ranges,
                                          index_read_info,
                                          tables,
                                          unused_total_size,
                                          row_count,
                                          macro_block_count,
                                          max_time,
                                          recalc_range_precision))) {
    LOG_WARN("Fail to estimate ranges size", KR(ret));
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_multi_range_size(const ObIArray<ObStoreRange> &ranges,
                                                       const ObITableReadInfo &index_read_info,
                                                       ObTableStoreIterator &table_iter,
                                                       int64_t &total_size,
                                                       const int64_t max_time,
                                                       const int64_t range_precision)
{
  int ret = OB_SUCCESS;

  total_size = 0;

  ObSEArray<ObITable *, 8> tables;

  if (OB_UNLIKELY(!index_read_info.is_valid() || max_time <= 0 || !table_iter.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get multi ranges size",
             KR(ret),
             K(index_read_info),
             K(range_precision),
             K(max_time),
             K(table_iter));
  } else if (OB_FAIL(get_tables(table_iter, tables))) {
    LOG_WARN("Fail to get split tables", KR(ret), K(table_iter));
  } else if (ranges.empty()) {
    // do nothing
  } else if (tables.empty()) {
    // only small tables
  } else if (OB_FAIL(get_multi_range_size(
                 ranges, index_read_info, tables, total_size, max_time, range_precision))) {
    LOG_WARN("Fail to get multi range size", KR(ret));
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::estimate_ranges_info(const ObIArray<ObStoreRange> &ranges,
                                                       const ObITableReadInfo &read_info,
                                                       const ObIArray<ObITable *> &tables,
                                                       int64_t &total_size,
                                                       int64_t &total_row_count,
                                                       int64_t &total_macro_block_count,
                                                       const int64_t max_time,
                                                       const ObRangePrecision &range_precision)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator;
  ObArray<ObStoreRange> sampled_ranges;
  ObArray<ObPairStoreAndDatumRange> sorted_ranges;

  total_size = 0;
  total_row_count = 0;
  total_macro_block_count = 0;
  int64_t memtable_total_size = 0;
  int64_t sstable_total_macro_block_count = 0;

  if (OB_FAIL(sampled_ranges.reserve(ranges.count()))) {
    LOG_WARN("Fail to reserve sampled ranges", KR(ret));
  } else {
    int64_t sample_step = range_precision.get_sample_step();
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i += sample_step) {
      if (OB_FAIL(sampled_ranges.push_back(ranges.at(i)))) {
        LOG_WARN("Fail to push back sampled range", KR(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(transform_to_datum_range_and_sort(
                 sampled_ranges, read_info, allocator, sorted_ranges))) {
    LOG_WARN("Fail to transform store range array to datum range array", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      ObITable *table = tables.at(i);
      if (table->is_sstable()) {
        ObSSTable *sstable = static_cast<ObSSTable *>(table);
        sstable_total_macro_block_count += sstable->get_data_macro_block_count();
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      ObITable *table = tables.at(i);
      if (table->is_sstable()) {
        ObSSTable *sstable = static_cast<ObSSTable *>(table);
        ObIndexBlockTreeTraverser traverser;
        ObMultiRangeInfoEstimateContext info_estimate_context;

        if (sstable->is_empty() || sstable_total_macro_block_count == 0) {
          // empty sstable, skip
        } else if (OB_FAIL(info_estimate_context.init(sorted_ranges, read_info, false))) {
          LOG_WARN("Fail to init info estimate context", KR(ret));
        } else if (OB_FAIL(traverser.init(*sstable, read_info))) {
          LOG_WARN("Fail to init info estimate traverser", KR(ret));
        } else if (OB_FAIL(traverser.traverse(
                       info_estimate_context,
                       max(1,
                           INDEX_BLOCK_PER_TIME * max_time * sstable->get_data_macro_block_count()
                               / sstable_total_macro_block_count)))) {
          LOG_WARN("Fail to traverse sstable index block tree", KR(ret));
        } else {
          total_size += info_estimate_context.get_total_size();
          total_row_count += info_estimate_context.get_total_row_count();
          total_macro_block_count += info_estimate_context.get_total_macro_block_count();
        }
      } else if (table->is_data_memtable()) {
        memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);

        for (int64_t j = 0; OB_SUCC(ret) && j < sampled_ranges.count(); j++) {
          int64_t estimate_bytes = 0;
          int64_t estimate_rows = 0;

          if (OB_FAIL(memtable->estimate_phy_size(&sampled_ranges.at(j).get_start_key(),
                                                  &sampled_ranges.at(j).get_end_key(),
                                                  estimate_bytes,
                                                  estimate_rows))) {
            LOG_WARN("Fail to estimate rows for memtable",
                     KR(ret),
                     K(sampled_ranges.at(j)),
                     K(memtable));
          } else {
            memtable_total_size += estimate_bytes;
            total_size += estimate_bytes;
            total_row_count += estimate_rows;
            // total_macro_block_count += 0; // memtable don't have macro block
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    total_size = total_size * ranges.count() / sampled_ranges.count();
    total_row_count = total_row_count * ranges.count() / sampled_ranges.count();
    total_macro_block_count
        = min(total_macro_block_count >= sampled_ranges.count()
                  ? total_macro_block_count * ranges.count() / sampled_ranges.count()
                  : total_macro_block_count,
              sstable_total_macro_block_count);

    // TODO(menglan): adapt now sql implement
    constexpr int64_t ONE_TASK_ROW_COUNT = 32768;
    if (GCTX.is_shared_storage_mode()) {
      total_size = max(total_size,
                       total_macro_block_count * OB_DEFAULT_MACRO_BLOCK_SIZE + memtable_total_size);
    } else {
      total_size
          = max(total_size,
                max(total_macro_block_count * OB_DEFAULT_MACRO_BLOCK_SIZE + memtable_total_size,
                    (total_row_count / ONE_TASK_ROW_COUNT + 1) * OB_DEFAULT_MACRO_BLOCK_SIZE));
    }

    LOG_TRACE("Finish estimate ranges info",
              K(total_size),
              K(total_row_count),
              K(total_macro_block_count),
              K(memtable_total_size));
  }

  return ret;
}

template <typename ObRange>
int ObPartitionMultiRangeSpliter::get_split_multi_ranges(
    const ObIArray<ObStoreRange> &ranges,
    const int64_t expected_task_count,
    const ObITableReadInfo &index_read_info,
    const ObIArray<ObITable *> &tables,
    ObIAllocator &allocator,
    ObArrayArray<ObRange> &multi_range_split_array,
    const bool for_compaction,
    const int64_t max_time,
    const int64_t range_precision)
{
  int ret = OB_SUCCESS;
  int64_t start_time_ms = ObTimeUtility::current_time_ms();

  multi_range_split_array.reset();

  int64_t fast_split_range_task_count = 0;
  ObRangePrecision recalc_range_precision(range_precision);

  if (OB_UNLIKELY(!index_read_info.is_valid() || !recalc_range_precision.is_valid() || max_time <= 0 || expected_task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get split multi ranges",
             KR(ret),
             K(expected_task_count),
             K(range_precision),
             K(max_time));
  } else if (ranges.empty()) {
    // do nothing
  } else if (OB_FAIL(recalc_range_precision.recalc_range_precision(ranges.count()))) {
    LOG_WARN("Fail to recalc range precision", KR(ret));
  } else if (tables.empty()) {
    // only small tables
    fast_split_range_task_count = 1;
  } else if (OB_UNLIKELY(expected_task_count == 1)) {
    // only one task
    fast_split_range_task_count = 1;
  } else if (all_range_is_single_rowkey(ranges)) {
    // all single rowkey
    fast_split_range_task_count = min(ranges.count(), expected_task_count);
  }

  if (OB_FAIL(ret)) {
  } else if (fast_split_range_task_count > 0) {
    if (OB_FAIL(fast_build_range_array(
            ranges, fast_split_range_task_count, allocator, multi_range_split_array))) {
      LOG_WARN("Fail to fast build range array", KR(ret));
    }
  } else if (OB_FAIL(build_range_array(ranges,
                                       expected_task_count,
                                       index_read_info,
                                       tables,
                                       allocator,
                                       multi_range_split_array,
                                       for_compaction,
                                       max_time,
                                       recalc_range_precision))) {
    LOG_WARN("Fail to build range array", KR(ret));
  }

  int64_t end_time_ms = ObTimeUtility::current_time_ms();
  LOG_TRACE("Finish split multi ranges",
            KR(ret),
            K(end_time_ms - start_time_ms),
            K(expected_task_count),
            K(multi_range_split_array));
  return ret;
}

template <typename ObRange>
int ObPartitionMultiRangeSpliter::get_split_multi_ranges(
    const ObIArray<ObStoreRange> &ranges,
    const int64_t expected_task_count,
    const ObITableReadInfo &index_read_info,
    ObTableStoreIterator &table_iter,
    ObIAllocator &allocator,
    ObArrayArray<ObRange> &multi_range_split_array,
    const bool for_compaction,
    const int64_t max_time,
    const int64_t range_precision)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObITable *, 8> tables;

  multi_range_split_array.reset();

  if (OB_UNLIKELY(!table_iter.is_valid() || !index_read_info.is_valid() || max_time <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get split multi ranges", KR(ret), K(table_iter));
  } else if (ranges.empty()) {
    // do nothing
  } else if (OB_FAIL(get_tables(table_iter, tables))) {
    LOG_WARN("Fail to get split tables", KR(ret), K(table_iter));
  } else if (OB_FAIL(get_split_multi_ranges(ranges,
                                            expected_task_count,
                                            index_read_info,
                                            tables,
                                            allocator,
                                            multi_range_split_array,
                                            for_compaction,
                                            max_time,
                                            range_precision))) {
    LOG_WARN("Fail to split multi ranges", KR(ret), K(table_iter));
  }

  return ret;
}

template int ObPartitionMultiRangeSpliter::get_split_multi_ranges(const ObIArray<ObStoreRange> &,
                                                                  const int64_t,
                                                                  const ObITableReadInfo &,
                                                                  ObTableStoreIterator &,
                                                                  ObIAllocator &,
                                                                  ObArrayArray<ObStoreRange> &,
                                                                  const bool,
                                                                  const int64_t,
                                                                  const int64_t);

template int ObPartitionMultiRangeSpliter::get_split_multi_ranges(const ObIArray<ObStoreRange> &,
                                                                  const int64_t,
                                                                  const ObITableReadInfo &,
                                                                  ObTableStoreIterator &,
                                                                  ObIAllocator &,
                                                                  ObArrayArray<ObDatumRange> &,
                                                                  const bool,
                                                                  const int64_t,
                                                                  const int64_t);

////////////////////////////////////////////////////////////////////////////////////////////////////

ObPartitionMajorSSTableRangeSpliter::ObPartitionMajorSSTableRangeSpliter()
    : major_sstable_(nullptr),
      index_read_info_(nullptr),
      tablet_size_(-1),
      allocator_(nullptr),
      is_inited_(false)
{
}

ObPartitionMajorSSTableRangeSpliter::~ObPartitionMajorSSTableRangeSpliter()
{
}

int ObPartitionMajorSSTableRangeSpliter::init(const ObITableReadInfo &index_read_info,
                                              ObSSTable *major_sstable, int64_t tablet_size,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionMajorSSTableRangeSpliter init twice", KR(ret));
  } else if (OB_ISNULL(major_sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument null major sstable", KR(ret), K(major_sstable));
  } else if (OB_UNLIKELY(tablet_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument tablet size", KR(ret), K(tablet_size));
  } else {
    major_sstable_ = major_sstable;
    index_read_info_ = &index_read_info;
    tablet_size_ = tablet_size;
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionMajorSSTableRangeSpliter::scan_major_sstable_secondary_meta(
  const ObDatumRange &scan_range, ObSSTableSecMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(major_sstable_->scan_secondary_meta(*allocator_, scan_range, *index_read_info_,
                                                  DATA_BLOCK_META, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }
  return ret;
}

int ObPartitionMajorSSTableRangeSpliter::split_ranges(ObIArray<ObStoreRange> &result_ranges)
{
  int ret = OB_SUCCESS;
  int64_t parallel_degree = 0;
  result_ranges.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionMajorSSTableRangeSpliter not init", KR(ret));
  } else {
    // 计算parallel_degree
    if (major_sstable_->is_empty() || tablet_size_ == 0) {
      parallel_degree = 1;
    } else {
      const int64_t macro_block_count = major_sstable_->get_data_macro_block_count();
      const int64_t occupy_size = macro_block_count * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
      parallel_degree = (occupy_size + tablet_size_ - 1) / tablet_size_;
      if (parallel_degree > MAX_MERGE_THREAD) {
        int64_t macro_cnts = (macro_block_count + MAX_MERGE_THREAD - 1) / MAX_MERGE_THREAD;
        parallel_degree = (macro_block_count + macro_cnts - 1) / macro_cnts;
      }
    }
    // 根据parallel_degree生成ranges
    if (parallel_degree <= 1) {
      ObStoreRange whole_range;
      whole_range.set_whole_range();
      if (OB_FAIL(result_ranges.push_back(whole_range))) {
        STORAGE_LOG(WARN, "failed to push back merge range to array", KR(ret), K(whole_range));
      }
    } else {
      if (OB_FAIL(generate_ranges_by_macro_block(parallel_degree, result_ranges))) {
        STORAGE_LOG(WARN, "failed to generate ranges by macro block", KR(ret), K(parallel_degree));
      }
    }
  }

  return ret;
}

int ObPartitionMajorSSTableRangeSpliter::generate_ranges_by_macro_block(
  int64_t parallel_degree, ObIArray<ObStoreRange> &result_ranges)
{
  int ret = OB_SUCCESS;
  const ObIArray<share::schema::ObColDesc> &col_descs = index_read_info_->get_columns_desc();
  const int64_t macro_block_count = major_sstable_->get_data_macro_block_count();
  const int64_t macro_block_cnt_per_range =
    (macro_block_count + parallel_degree - 1) / parallel_degree;

  ObDataMacroBlockMeta blk_meta;
  ObSSTableSecMetaIterator *meta_iter = nullptr;
  ObDatumRange scan_range;
  scan_range.set_whole_range();
  if (OB_FAIL(scan_major_sstable_secondary_meta(scan_range, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }

  // generate ranges
  ObDatumRowkey endkey;
  ObStoreRange range;
  range.get_end_key().set_min();
  range.set_left_open();
  range.set_right_closed();
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_count;) {
    const int64_t last = i + macro_block_cnt_per_range - 1; // last macro block idx in current range
    range.get_start_key() = range.get_end_key();
    if (last < macro_block_count - 1) {
      // locate to the last macro-block meta in current range
      while (OB_SUCC(meta_iter->get_next(blk_meta)) && i++ < last);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(WARN, "Failed to get macro block meta", KR(ret), K(i - 1));
      } else if (OB_UNLIKELY(!blk_meta.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block meta", KR(ret), K(i - 1));
      } else if (OB_FAIL(blk_meta.get_rowkey(endkey))) {
        STORAGE_LOG(WARN, "Failed to get rowkey", KR(ret), K(blk_meta));
      } else if (OB_FAIL(endkey.to_store_rowkey(col_descs, *allocator_, range.get_end_key()))) {
        STORAGE_LOG(WARN, "Failed to transfer store rowkey", K(ret), K(endkey));
      }
    } else { // last range
      i = last + 1;
      range.get_end_key().set_max();
      range.set_right_open();
    }
    if (OB_SUCC(ret) && OB_FAIL(result_ranges.push_back(range))) {
      STORAGE_LOG(WARN, "Failed to push range", KR(ret), K(result_ranges), K(range));
    }
  }

  if (OB_NOT_NULL(meta_iter)) {
    meta_iter->~ObSSTableSecMetaIterator();
    meta_iter = nullptr;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::ObIncrementalIterator(compaction::ObTabletMergeCtx &merge_ctx, ObIAllocator &allocator)
    : merge_ctx_(merge_ctx),
      allocator_(allocator),
      tbl_read_info_(),
      iter_(nullptr),
      is_inited_(false)
{
}

ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::~ObIncrementalIterator()
{
  reset();
}

void ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::reset()
{
  rowkey_col_ids_.reset();
  out_cols_project_.reset();
  tbl_read_info_.reset();
  tbl_xs_param_.reset();
  store_ctx_.reset();
  tbl_xs_ctx_.reset();
  tbls_iter_.reset();
  get_tbl_param_.reset();
  range_to_scan_.reset();
  if (iter_ != nullptr) {
    iter_->~ObIStoreRowIterator();
    iter_ = nullptr;
  }
  is_inited_ = false;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIncrementalIterator init twice", KR(ret));
  } else {
    ObMultipleScanMerge *mpl_scan_mrg = nullptr;
    range_to_scan_.set_whole_range();
    if (OB_ISNULL(mpl_scan_mrg = OB_NEWx(ObMultipleScanMerge, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", KR(ret));
    } else if (OB_FAIL(prepare_table_access_param())) {
      STORAGE_LOG(WARN, "Failed to prepare table access param", KR(ret));
    } else if (OB_FAIL(prepare_store_ctx())) {
      STORAGE_LOG(WARN, "Failed to prepare store ctx", KR(ret));
    } else if (OB_FAIL(prepare_table_access_context())) {
      STORAGE_LOG(WARN, "Failed to prepare table access context", KR(ret));
    } else if (OB_FAIL(prepare_get_table_param())) {
      STORAGE_LOG(WARN, "Failed to prepare get table param", KR(ret));
    } else if (OB_FAIL(mpl_scan_mrg->init(tbl_xs_param_, tbl_xs_ctx_, get_tbl_param_))) {
      STORAGE_LOG(WARN, "Failed to init multiple scan merge", KR(ret));
    } else if (OB_FAIL(mpl_scan_mrg->open(range_to_scan_))) {
      STORAGE_LOG(WARN, "Failed to open multiple scan merge", KR(ret));
    } else {
      mpl_scan_mrg->set_iter_del_row(true);
      iter_ = mpl_scan_mrg;
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      if (mpl_scan_mrg != nullptr) {
        mpl_scan_mrg->~ObMultipleScanMerge();
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::get_next_row(
  const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIncrementalIterator not init", KR(ret));
  } else {
    ret = iter_->get_next_row(row);
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_table_access_param()
{
  int ret = OB_SUCCESS;
  const ObStorageSchema *storage_schema = merge_ctx_.get_schema();
  if (OB_FAIL(storage_schema->get_mulit_version_rowkey_column_ids(rowkey_col_ids_))) {
    STORAGE_LOG(WARN, "Failed to get rowkey column ids", KR(ret));
  } else if (OB_FAIL(tbl_read_info_.init(allocator_, storage_schema->get_column_count(),
                                         storage_schema->get_rowkey_column_num(),
                                         lib::is_oracle_mode(), rowkey_col_ids_))) {
    STORAGE_LOG(WARN, "Failed to init columns info", KR(ret));
  } else if (OB_FAIL(tbl_xs_param_.init_merge_param(merge_ctx_.get_tablet_id().id(),
                                                    merge_ctx_.get_tablet_id(),
                                                    tbl_read_info_,
                                                    false/*is_multi_version_minor_merge*/,
                                                    false/*is_delete_insert*/))) {
    STORAGE_LOG(WARN, "Failed to init table access param", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_col_ids_.count(); i++) {
    if (OB_FAIL(out_cols_project_.push_back(static_cast<int32_t>(i)))) {
      STORAGE_LOG(WARN, "Failed to push column project", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    tbl_xs_param_.iter_param_.out_cols_project_ = &out_cols_project_;
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_store_ctx()
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = merge_ctx_.get_ls_id();
  int64_t snapshot = merge_ctx_.get_snapshot();
  SCN scn;
  if (OB_FAIL(scn.convert_for_tx(snapshot))) {
    STORAGE_LOG(WARN, "convert for tx fail", K(ret), K(ls_id), K(snapshot));
  } else if (OB_FAIL(store_ctx_.init_for_read(ls_id,
                                              merge_ctx_.get_tablet_id(),
                                              INT64_MAX,
                                              -1,
                                              scn))) {
    STORAGE_LOG(WARN, "init store ctx fail", K(ret), K(ls_id), K(snapshot));
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_table_access_context()
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         true  /*daily_merge*/,
                         true  /*optimize*/,
                         true  /*whole_macro_scan*/,
                         false /*full_row*/,
                         false /*index_back*/,
                         false /*query_stat*/
                         );
  ObITable *major_sstable = merge_ctx_.get_tables_handle().get_table(0);
  ObVersionRange scan_version_range = merge_ctx_.static_param_.version_range_;
  scan_version_range.base_version_ = major_sstable->get_snapshot_version();
  if (OB_FAIL(tbl_xs_ctx_.init(query_flag, store_ctx_, allocator_, allocator_, scan_version_range))) {
    STORAGE_LOG(WARN, "Failed to init table access context", KR(ret));
  } else {
    tbl_xs_ctx_.merge_scn_ = merge_ctx_.static_param_.merge_scn_;
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_get_table_param()
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  for (int64_t i = 1; OB_SUCC(ret) && i < merge_ctx_.get_tables_handle().get_count(); i++) {
    table = merge_ctx_.get_tables_handle().get_table(i);
    if (OB_FAIL(tbls_iter_.add_table(table))) {
      STORAGE_LOG(WARN, "Failed to add table to inc handle", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_tbl_param_.tablet_iter_.table_iter()->assign(tbls_iter_))) {
      STORAGE_LOG(WARN, "Failed to assign tablet iterator", KR(ret));
    }
  }
  return ret;
}

ObPartitionIncrementalRangeSpliter::ObPartitionIncrementalRangeSpliter()
    : merge_ctx_(nullptr),
      allocator_(nullptr),
      major_sstable_(nullptr),
      tablet_size_(-1),
      iter_(nullptr),
      default_noisy_row_num_skipped_(DEFAULT_NOISY_ROW_NUM_SKIPPED),
      default_row_num_per_range_(DEFAULT_ROW_NUM_PER_RANGE),
      inc_ranges_(nullptr),
      base_ranges_(nullptr),
      combined_ranges_(nullptr),
      is_inited_(false)
{
}

ObPartitionIncrementalRangeSpliter::~ObPartitionIncrementalRangeSpliter()
{
  if (nullptr != iter_) {
    iter_->~ObIncrementalIterator();
    iter_ = nullptr;
  }
  if (nullptr != combined_ranges_) {
    combined_ranges_->~ObIArray<ObDatumRange>();
    combined_ranges_ = nullptr;
  }
  if (nullptr != base_ranges_) {
    base_ranges_->~ObIArray<ObDatumRange>();
    base_ranges_ = nullptr;
  }
  if (nullptr != inc_ranges_) {
    inc_ranges_->~ObIArray<ObDatumRange>();
    inc_ranges_ = nullptr;
  }
}

int ObPartitionIncrementalRangeSpliter::init(compaction::ObTabletMergeCtx &merge_ctx,
                                             ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObTablesHandleArray &tables_handle = merge_ctx.get_tables_handle();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionIncrementalRangeSpliter init twice", KR(ret));
  } else if (OB_UNLIKELY(tables_handle.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument tables handle (empty)", KR(ret), K(tables_handle));
  } else if (OB_ISNULL(tables_handle.get_table(0))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected null first table", KR(ret), K(tables_handle));
  } else if (OB_UNLIKELY(!tables_handle.get_table(0)->is_major_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "first table must be major sstable", KR(ret), K(tables_handle));
  } else {
    merge_ctx_ = &merge_ctx;
    allocator_ = &allocator;
    // TODO(@DanLing) use cg table in co dag_net
    // the first table is ObSSTable in unit test
    major_sstable_ = static_cast<ObSSTable *>(tables_handle.get_table(0));
    tablet_size_ = merge_ctx.get_schema()->get_tablet_size();
    if (OB_UNLIKELY(tablet_size_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument tablet size", KR(ret), K_(tablet_size));
    } else if (OB_FAIL(alloc_ranges())) {
      STORAGE_LOG(WARN, "failed to alloc ranges", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::alloc_ranges()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t ranges_size = sizeof(ObSEArray<ObDatumRange, 64>);
  if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(ranges_size * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to allocate memory", KR(ret), "size", ranges_size * 3);
  } else {
    inc_ranges_ = new (buf) ObSEArray<ObDatumRange, 64>();
    base_ranges_ = new (buf + ranges_size) ObSEArray<ObDatumRange, 64>();
    combined_ranges_ = new (buf + ranges_size * 2) ObSEArray<ObDatumRange, 64>();
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::init_incremental_iter()
{
  int ret = OB_SUCCESS;
  if (nullptr != iter_) {
    iter_->reset();
  } else {
    if (OB_ISNULL(iter_ = OB_NEWx(ObIncrementalIterator, allocator_, *merge_ctx_, *allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(iter_->init())) {
      STORAGE_LOG(WARN, "failed to init incremental iterator", KR(ret));
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::get_major_sstable_end_rowkey(ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(major_sstable_->get_last_rowkey(*allocator_, rowkey))) {
    STORAGE_LOG(WARN, "failed to get major sstable last rowkey", KR(ret), K(*major_sstable_));
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::scan_major_sstable_secondary_meta(
    const ObDatumRange &scan_range, ObSSTableSecMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo &rowkey_read_info =
    merge_ctx_->get_tablet()->get_rowkey_read_info();
  if (OB_FAIL(major_sstable_->scan_secondary_meta(*allocator_, scan_range, rowkey_read_info,
                                                  DATA_BLOCK_META, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::check_is_incremental(bool &is_incremental)
{
  int ret = OB_SUCCESS;
  const int64_t tables_handle_cnt = merge_ctx_->get_tables_handle().get_count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionIncrementalRangeSpliter not init", KR(ret));
  } else if (tables_handle_cnt <= 1) {
    // no incremental data
    is_incremental = false;
  } else if (major_sstable_->is_empty()) {
    // no base data
    is_incremental = true;
  } else {
    const ObDatumRow *row = nullptr;
    int64_t row_count = 0;
    is_incremental = false;
    if (OB_FAIL(init_incremental_iter())) {
      STORAGE_LOG(WARN, "failed to init incremental iterator", KR(ret));
    } else {
      // skip a few rows to avoid updated noise
      while (OB_SUCC(ret) && OB_SUCC(iter_->get_next_row(row)) && OB_NOT_NULL(row) &&
            ++row_count <= default_noisy_row_num_skipped_);
      if (OB_ITER_END == ret) {
        STORAGE_LOG(DEBUG, "incremental row num less than skipped num");
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret) && row_count > default_noisy_row_num_skipped_) {
        // compare with base sstable last rowkey
        int cmp_ret = 0;
        ObDatumRowkey row_rowkey;
        ObDatumRowkey end_rowkey;
        const int64_t rowkey_column_num = merge_ctx_->get_schema()->get_rowkey_column_num();
        const ObStorageDatumUtils &datum_utils =
          merge_ctx_->get_tablet()->get_rowkey_read_info().get_datum_utils();
        if (OB_FAIL(row_rowkey.assign(row->storage_datums_, rowkey_column_num))) {
          STORAGE_LOG(WARN, "failed to assign datum rowkey", KR(ret), K(*row),
                      K(rowkey_column_num));
        } else if (OB_FAIL(get_major_sstable_end_rowkey(end_rowkey))) {
          STORAGE_LOG(WARN, "failed to get base sstable last rowkey", KR(ret), K(*major_sstable_));
        } else if (OB_FAIL(row_rowkey.compare(end_rowkey, datum_utils, cmp_ret))) {
          STORAGE_LOG(WARN, "failed to compare rowkey", KR(ret), K(row_rowkey), K(end_rowkey));
        } else if (cmp_ret > 0) {
          is_incremental = true;
        }
        STORAGE_LOG(DEBUG, "cmp rowkey", KR(ret), K(cmp_ret), K(row_rowkey), K(end_rowkey));
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::split_ranges(ObDatumRangeArray &result_ranges)
{
  int ret = OB_SUCCESS;
  result_ranges.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionIncrementalRangeSpliter not init", KR(ret));
  } else if (tablet_size_ == 0) {
    ObDatumRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(result_ranges.push_back(whole_range))) {
      STORAGE_LOG(WARN, "failed to push back merge range to array", KR(ret), K(whole_range));
    }
  } else {
    ObDatumRangeArray *ranges = nullptr;
    if (OB_FAIL(get_ranges_by_inc_data(*inc_ranges_))) {
      STORAGE_LOG(WARN, "failed to get ranges by inc data", KR(ret));
    } else if (merge_ctx_->get_is_full_merge()) {
      if (major_sstable_->is_empty()) {
        ranges = inc_ranges_;
      } else if (OB_FAIL(get_ranges_by_base_sstable(*base_ranges_))) {
        STORAGE_LOG(WARN, "failed to get ranges by base sstable", KR(ret));
      } else if (OB_FAIL(combine_ranges(*base_ranges_, *inc_ranges_, *combined_ranges_))) {
        STORAGE_LOG(WARN, "failed to combine base and inc ranges", KR(ret));
      } else {
        ranges = combined_ranges_;
      }
    } else {
      ranges = inc_ranges_;
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ranges)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null ranges", K(ret), K(ranges));
      } else if (OB_FAIL(merge_ranges(*ranges, result_ranges))) {
        STORAGE_LOG(WARN, "failed to merge ranges", KR(ret));
      }
    }

    if (OB_SUCC(ret) && result_ranges.count() > 0) {
      const ObStorageDatumUtils &datum_utils =
        merge_ctx_->get_tablet()->get_rowkey_read_info().get_datum_utils();
      if (OB_FAIL(check_continuous(datum_utils, result_ranges))) {
        STORAGE_LOG(WARN, "failed to check continuous", KR(ret), K(result_ranges));
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::get_ranges_by_inc_data(ObDatumRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  const int64_t tables_handle_cnt = merge_ctx_->get_tables_handle().get_count();
  ranges.reset();
  if (OB_UNLIKELY(tablet_size_ <= 0 || tables_handle_cnt <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K_(tablet_size), K(tables_handle_cnt));
  } else {
    if (OB_FAIL(init_incremental_iter())) {
      STORAGE_LOG(WARN, "failed to init incremental iterator", KR(ret));
    } else {
      int64_t num_rows_per_range = default_row_num_per_range_;
      // calculate num_rows_per_range by macro block
      const int64_t macro_block_count = major_sstable_->get_data_macro_block_count();
      const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
      if (macro_block_count * macro_block_size > tablet_size_) {
        const int64_t row_count = major_sstable_->get_row_count();
        const int64_t num_rows_per_macro_block = row_count / macro_block_count;
        num_rows_per_range = num_rows_per_macro_block * tablet_size_ / macro_block_size;
      }

      if (OB_SUCC(ret)) {
        // to avoid block the macro-block, simply make num_rows_per_range >= DEFAULT_NOISY_ROW_NUM_SKIPPED
        if (num_rows_per_range < default_noisy_row_num_skipped_) {
          num_rows_per_range = default_noisy_row_num_skipped_;
        }
      }

      const int64_t rowkey_column_num = merge_ctx_->get_schema()->get_rowkey_column_num();
      int64_t count = 0;
      const ObDatumRow *row = nullptr;
      ObDatumRowkey rowkey;
      ObDatumRange range;
      ObDatumRange multi_version_range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();

      // generate ranges
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter_->get_next_row(row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            STORAGE_LOG(WARN, "failed to get nex row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null row", KR(ret));
        } else if (++count >= num_rows_per_range) {
          count = 0;
          range.start_key_ = range.end_key_;
          if (OB_FAIL(rowkey.assign(row->storage_datums_, rowkey_column_num))) {
            STORAGE_LOG(WARN, "failed to assign datum rowkey", KR(ret), K(*row),
                        K(rowkey_column_num));
          } else if (OB_FAIL(rowkey.deep_copy(range.end_key_, *allocator_))) {
            STORAGE_LOG(WARN, "failed to deep copy datum rowkey", KR(ret), K(rowkey));
          } else if (OB_FAIL(range.to_multi_version_range(*allocator_, multi_version_range))) {
            STORAGE_LOG(WARN, "failed to transfer multi version range", KR(ret), K(range));
          } else if (OB_FAIL(ranges.push_back(multi_version_range))) {
            STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
          }
        }
      }

      if (OB_SUCC(ret)) {
        // handle the last range
        if (count > 0) {
          range.start_key_ = range.end_key_;
          range.end_key_.set_max_rowkey();
          range.set_right_open();
          if (OB_FAIL(range.to_multi_version_range(*allocator_, multi_version_range))) {
            STORAGE_LOG(WARN, "failed to transfer multi version range", KR(ret), K(range));
          } else if (OB_FAIL(ranges.push_back(multi_version_range))) {
            STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
          }
        } else if (ranges.empty()) {
          range.set_whole_range();
          if (OB_FAIL(ranges.push_back(range))) {
            STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
          }
        } else {
          ObDatumRange &last_range = ranges.at(ranges.count() - 1);
          last_range.end_key_.set_max_rowkey();
          last_range.set_right_open();
        }
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::get_ranges_by_base_sstable(ObDatumRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  ranges.reset();
  if (OB_UNLIKELY(tablet_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get ranges by base sstable", KR(ret), K_(tablet_size));
  } else {
    ObSSTableSecMetaIterator *meta_iter = nullptr;
    ObDatumRange scan_range;
    scan_range.set_whole_range();
    if (OB_FAIL(scan_major_sstable_secondary_meta(scan_range, meta_iter))) {
      STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
    } else {
      const int64_t macro_block_cnt =
        major_sstable_->get_data_macro_block_count();
      const int64_t total_size = macro_block_cnt * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
      const int64_t range_cnt = (total_size + tablet_size_ - 1) / tablet_size_;
      const int64_t macro_block_cnt_per_range = (macro_block_cnt + range_cnt - 1) / range_cnt;

      ObDatumRowkey endkey;
      ObDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      ObDataMacroBlockMeta blk_meta;

      // generate ranges
      for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_cnt;) {
        const int64_t last = i + macro_block_cnt_per_range - 1;
        range.start_key_ = range.end_key_;
        if (last < macro_block_cnt - 1) {
          // locate to the last macro-block meta in current range
          while (OB_SUCC(meta_iter->get_next(blk_meta)) && i++ < last);
          if (OB_FAIL(ret)) {
            STORAGE_LOG(WARN, "Failed to get macro block meta", KR(ret), K(i - 1));
          } else if (OB_UNLIKELY(!blk_meta.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected invalid macro block meta", KR(ret), K(i - 1));
          } else if (OB_FAIL(blk_meta.get_rowkey(endkey))) {
            STORAGE_LOG(WARN, "Failed to get rowkey", KR(ret), K(blk_meta));
          } else if (OB_FAIL(endkey.deep_copy(range.end_key_, *allocator_))) {
            STORAGE_LOG(WARN, "Failed to transfer store rowkey", K(ret), K(endkey));
          }
        } else { // last range
          i = last + 1;
          range.end_key_.set_max_rowkey();
          range.set_right_open();
        }
        if (OB_SUCC(ret) && OB_FAIL(ranges.push_back(range))) {
          STORAGE_LOG(WARN, "Failed to push range", KR(ret), K(ranges), K(range));
        }
      }
    }
    if (OB_NOT_NULL(meta_iter)) {
      meta_iter->~ObSSTableSecMetaIterator();
      meta_iter = nullptr;
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::combine_ranges(const ObDatumRangeArray &base_ranges,
                                                       const ObDatumRangeArray &inc_ranges,
                                                       ObDatumRangeArray &result_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(base_ranges.empty() || inc_ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument to combine ranges", KR(ret),
                K(base_ranges), K(inc_ranges));
  } else {
    // incremental data is appended after base sstable
    // so just append the inc ranges to the base ranges
    result_ranges.reset();

    ObDatumRowkey end_rowkey;
    if (OB_FAIL(get_major_sstable_end_rowkey(end_rowkey))) {
      STORAGE_LOG(WARN, "failed to get base sstable last rowkey", KR(ret), K_(major_sstable));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < base_ranges.count(); i++) {
        const ObDatumRange &range = base_ranges.at(i);
        if (OB_FAIL(result_ranges.push_back(range))) {
          STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_ranges.count(); i++) {
        const ObDatumRange &range = inc_ranges.at(i);
        if (OB_FAIL(result_ranges.push_back(range))) {
          STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
        }
      }
      if (OB_SUCC(ret)) {
        // base_ranges和inc_ranges交接的地方 (k1, MAX) (MIN, k2] 改成 （k1, endkey] (endkey, k2]
        ObDatumRange &base_last_range = result_ranges.at(base_ranges.count() - 1);
        ObDatumRange &inc_first_range = result_ranges.at(base_ranges.count());
        if (OB_FAIL(end_rowkey.deep_copy(base_last_range.end_key_, *allocator_))) {
          STORAGE_LOG(WARN, "failed to deep copy rowkey", KR(ret), K(end_rowkey),
                      K(base_last_range.get_end_key()));
        } else {
          base_last_range.set_right_closed();
          inc_first_range.start_key_ = base_last_range.end_key_;
        }
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::merge_ranges(const ObDatumRangeArray &ranges,
                                                     ObDatumRangeArray &result_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(ranges));
  } else {
    const int64_t num_ranges_per_thread = (ranges.count() + MAX_MERGE_THREAD - 1) / MAX_MERGE_THREAD;
    ObDatumRange merged_range;
    const ObDatumRange *range;
    merged_range.end_key_.set_min_rowkey();
    merged_range.set_left_open();
    merged_range.set_right_closed();
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i += num_ranges_per_thread) {
      if (num_ranges_per_thread == 1) {
        range = &ranges.at(i);
      } else {
        int64_t last = i + num_ranges_per_thread - 1;
        last = (last < ranges.count() ? last : ranges.count() - 1);
        const ObDatumRange &first_range = ranges.at(i);
        const ObDatumRange &last_range = ranges.at(last);
        // merged_range.end_key_ should be the same as first_range.start_key_
        merged_range.set_start_key(first_range.get_start_key());
        merged_range.set_end_key(last_range.get_end_key());
        range = &merged_range;
      }

      if (OB_FAIL(result_ranges.push_back(*range))) {
        STORAGE_LOG(WARN, "failed to push range", KR(ret), K(*range));
      }
    }
    if (OB_SUCC(ret)) {
      (result_ranges.at(result_ranges.count() - 1)).set_right_open();
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::check_continuous(const ObStorageDatumUtils &datum_utils,
                                                         ObDatumRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranges.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bad ranges (empty)", K(ret), K(ranges));
  } else if (OB_UNLIKELY(!ranges.at(0).get_start_key().is_min_rowkey() ||
                         !ranges.at(0).is_left_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bad ranges (first start key not min)", K(ret), K(ranges));
  } else if (OB_UNLIKELY(!ranges.at(ranges.count() - 1).get_end_key().is_max_rowkey() ||
                         !ranges.at(ranges.count() - 1).is_right_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bad ranges (last end key not max)", K(ret), K(ranges));
  } else {
    bool is_equal = false;
    for (int64_t i = 1; OB_SUCC(ret) && i < ranges.count(); i++) {
      if (OB_UNLIKELY(!ranges.at(i - 1).is_right_closed())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected bad ranges (end key not included)", K(ret), K(i), K(ranges));
      } else if (OB_UNLIKELY(!ranges.at(i).is_left_open())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected bad ranges (start key not excluded)", K(ret), K(i), K(ranges));
      } else if (OB_FAIL(ranges.at(i).get_start_key().equal(ranges.at(i - 1).get_end_key(),
                                                            datum_utils, is_equal))) {
        STORAGE_LOG(WARN, "Failed to compare rowkeys", K(ret), K(i), K(ranges));
      } else if (OB_UNLIKELY(!is_equal)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected bad ranges (not contiguous)", K(ret), K(i), K(ranges));
      }
    }
  }
  return ret;
}

}
}
